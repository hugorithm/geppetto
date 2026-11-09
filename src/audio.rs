use std::{
    collections::VecDeque,
    io::{self, Read, Seek, SeekFrom},
    sync::{Arc, Condvar, Mutex},
};

use anyhow::{Result, ensure};
use rubato::{FftFixedIn, Resampler};
use symphonia_core::io::MediaSource;

pub const INPUT_FRAMES: usize = 960;
const MAX_PLAYBACK_BYTES: usize = 24_000 * 4 * 30;

pub struct DiscordResampler {
    resampler: FftFixedIn<f32>,
    pending: Vec<f32>,
}

impl DiscordResampler {
    pub fn new() -> Result<Self> {
        Ok(Self {
            resampler: FftFixedIn::new(48_000, 24_000, INPUT_FRAMES, 2, 1)?,
            pending: Vec::new(),
        })
    }

    pub fn convert(&mut self, stereo: &[i16]) -> Result<Vec<u8>> {
        ensure!(stereo.len().is_multiple_of(2), "Incomplete stereo frame");
        self.pending.extend(
            stereo
                .as_chunks::<2>()
                .0
                .iter()
                .map(|frame| (f32::from(frame[0]) + f32::from(frame[1])) / 65_536.0),
        );
        let mut pcm = Vec::new();
        while self.pending.len() >= INPUT_FRAMES {
            let mono: Vec<f32> = self.pending.drain(..INPUT_FRAMES).collect();
            let output = self.resampler.process(&[mono], None)?;
            for sample in &output[0] {
                let sample = (sample * 32_768.0).round().clamp(-32_768.0, 32_767.0) as i16;
                pcm.extend_from_slice(&sample.to_le_bytes());
            }
        }
        Ok(pcm)
    }
}

#[derive(Default)]
struct Buffer {
    bytes: VecDeque<u8>,
    finished: bool,
}

#[derive(Clone, Default)]
pub struct PlaybackBuffer(Arc<(Mutex<Buffer>, Condvar)>);

impl PlaybackBuffer {
    pub fn append_pcm16(&self, pcm: &[u8]) -> Result<()> {
        ensure!(pcm.len().is_multiple_of(2), "Incomplete PCM16 sample");
        let (mutex, ready) = &*self.0;
        let mut buffer = mutex.lock().unwrap();
        if buffer.finished {
            return Ok(());
        }
        ensure!(
            buffer.bytes.len() + pcm.len() * 2 <= MAX_PLAYBACK_BYTES,
            "Playback backlog exceeded 30 seconds"
        );
        for sample in pcm.as_chunks::<2>().0 {
            let sample = f32::from(i16::from_le_bytes([sample[0], sample[1]])) / 32_768.0;
            buffer.bytes.extend(sample.to_le_bytes());
        }
        ready.notify_all();
        Ok(())
    }

    pub fn finish(&self, discard: bool) {
        let (mutex, ready) = &*self.0;
        let mut buffer = mutex.lock().unwrap();
        buffer.finished = true;
        if discard {
            buffer.bytes.clear();
        }
        ready.notify_all();
    }
}

impl Read for PlaybackBuffer {
    fn read(&mut self, destination: &mut [u8]) -> io::Result<usize> {
        if destination.is_empty() {
            return Ok(0);
        }
        let (mutex, ready) = &*self.0;
        let mut buffer = mutex.lock().unwrap();
        while buffer.bytes.is_empty() && !buffer.finished {
            buffer = ready.wait(buffer).unwrap();
        }
        let count = destination.len().min(buffer.bytes.len());
        for target in &mut destination[..count] {
            *target = buffer.bytes.pop_front().unwrap();
        }
        Ok(count)
    }
}

impl Seek for PlaybackBuffer {
    fn seek(&mut self, _: SeekFrom) -> io::Result<u64> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Live audio cannot seek",
        ))
    }
}

impl MediaSource for PlaybackBuffer {
    fn is_seekable(&self) -> bool {
        false
    }
    fn byte_len(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;

    #[test]
    fn resampling_preserves_duration_across_partial_frames() -> Result<()> {
        let mut resampler = DiscordResampler::new()?;
        assert!(resampler.convert(&vec![0; 960])?.is_empty());
        let output = resampler.convert(&vec![0; 960])?;
        assert_eq!(output.len(), 480 * 2);
        assert!(output.iter().all(|byte| *byte == 0));
        assert!(resampler.convert(&[0]).is_err());
        Ok(())
    }

    #[test]
    fn playback_converts_pcm_and_drains_before_eof() -> Result<()> {
        let mut buffer = PlaybackBuffer::default();
        buffer.append_pcm16(&i16::MIN.to_le_bytes())?;
        buffer.append_pcm16(&i16::MAX.to_le_bytes())?;
        buffer.finish(false);
        let mut output = Vec::new();
        buffer.read_to_end(&mut output)?;
        assert_eq!(output.len(), 8);
        assert_eq!(f32::from_le_bytes(output[..4].try_into()?), -1.0);
        assert!(f32::from_le_bytes(output[4..].try_into()?) < 1.0);
        assert!(buffer.append_pcm16(&[0]).is_err());
        Ok(())
    }

    #[test]
    fn interruption_wakes_reader_and_discards_audio() {
        let buffer = PlaybackBuffer::default();
        let mut reader = buffer.clone();
        let task = std::thread::spawn(move || reader.read(&mut [0; 4]).unwrap());
        buffer.finish(true);
        assert_eq!(task.join().unwrap(), 0);
        buffer.append_pcm16(&[0, 0]).unwrap();
        assert_eq!(buffer.clone().read(&mut [0; 4]).unwrap(), 0);
    }

    #[test]
    fn songbird_decodes_before_the_live_stream_finishes() -> Result<()> {
        use songbird::input::{
            RawAdapter,
            codecs::{get_codec_registry, get_probe},
        };
        use symphonia_core::{
            formats::FormatOptions, io::MediaSourceStream, meta::MetadataOptions, probe::Hint,
        };
        let buffer = PlaybackBuffer::default();
        buffer.append_pcm16(&vec![0; 480 * 2])?;
        let source = buffer.clone();
        let (sender, receiver) = std::sync::mpsc::channel();
        let task = std::thread::spawn(move || {
            let stream = MediaSourceStream::new(
                Box::new(RawAdapter::new(source, 24_000, 1)),
                Default::default(),
            );
            let result = get_probe()
                .format(
                    &Hint::new(),
                    stream,
                    &FormatOptions::default(),
                    &MetadataOptions::default(),
                )
                .and_then(|mut probed| {
                    let mut decoder = get_codec_registry().make(
                        &probed.format.default_track().unwrap().codec_params,
                        &Default::default(),
                    )?;
                    let packet = probed.format.next_packet()?;
                    decoder.decode(&packet).map(|decoded| decoded.frames())
                });
            let _ = sender.send(result);
        });
        let result = receiver.recv_timeout(std::time::Duration::from_secs(2));
        buffer.finish(true);
        task.join().unwrap();
        assert_eq!(result.context("Live decoder waited for EOF")??, 480);
        Ok(())
    }
}
