//! Owns the two calls across the PulseAudio, Opus and Ogg ABI.
use listener_library::*;
use std::{ffi::CString, fs::File, os::fd::AsRawFd, os::raw::c_char};

use crate::{Capture, End, Refusal};

#[repr(C)]
struct NativeCapture {
    server: *const c_char,
    application_name: *const c_char,
    source: *const c_char,
    sample_rate: u32,
    channels: i32,
    fragment_bytes: u32,
    buffer_bytes: u32,
    pending_frames: u32,
    frame_samples: i32,
    opus_application: i32,
    bitrate: i32,
    bitrate_control: i32,
    complexity: i32,
    signal: i32,
    bandwidth: i32,
    discontinuous_transmission: i32,
    forward_error_correction: i32,
    packet_loss: i32,
    page_packets: u32,
    sync_each_page: i32,
    interrupt_on_gap: i32,
    serial: i32,
}
unsafe extern "C" {
    fn listener_record(settings: *const NativeCapture, control: i32, audio: i32) -> i32;
    fn listener_recover_audio(audio: i32) -> i32;
}

pub(crate) trait NativeRecording {
    fn record(self, control: &std::os::unix::net::UnixStream, file: &File) -> End;
}
impl NativeRecording for Capture {
    fn record(self, control: &std::os::unix::net::UnixStream, file: &File) -> End {
        let result = self.prepare(control, file);
        match result {
            Ok(0) => End::Stopped,
            Ok(1) | Err(Refusal::NoMicrophone) => End::MicrophoneLost,
            Ok(4) => End::Gap,
            Ok(_) | Err(Refusal::Unwritable) => End::Unwritable,
        }
    }
}

trait PreparingRecording {
    fn prepare(self, control: &std::os::unix::net::UnixStream, file: &File)
    -> Result<i32, Refusal>;
}
impl PreparingRecording for Capture {
    fn prepare(
        self,
        control: &std::os::unix::net::UnixStream,
        file: &File,
    ) -> Result<i32, Refusal> {
        let setting = self.recording_settings;
        let server = match setting.pulse_server {
            PulseServer::Default => None,
            PulseServer::Named(address) => {
                Some(CString::new(address.0).map_err(|_| Refusal::NoMicrophone)?)
            }
        };
        let name =
            CString::new(setting.pulse_application_name.0).map_err(|_| Refusal::NoMicrophone)?;
        let source = match self.source {
            Source::Default => None,
            Source::Named(name) => Some(CString::new(name.0).map_err(|_| Refusal::NoMicrophone)?),
        };
        let sample_rate = u32::try_from(setting.sample_rate.0).map_err(|_| Refusal::Unwritable)?;
        let frame_time = match setting.opus_frame_duration {
            OpusFrameDuration::TwoAndHalfMilliseconds => 2500,
            OpusFrameDuration::FiveMilliseconds => 5000,
            OpusFrameDuration::TenMilliseconds => 10000,
            OpusFrameDuration::TwentyMilliseconds => 20000,
            OpusFrameDuration::FortyMilliseconds => 40000,
            OpusFrameDuration::SixtyMilliseconds => 60000,
        };
        let frame_samples = i32::try_from(u64::from(sample_rate) * frame_time / 1_000_000)
            .map_err(|_| Refusal::Unwritable)?;
        let positive = |value| {
            u32::try_from(value)
                .ok()
                .filter(|value| *value > 0)
                .ok_or(Refusal::Unwritable)
        };
        let narrow = |value| i32::try_from(value).map_err(|_| Refusal::Unwritable);
        let native = NativeCapture {
            server: server
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            application_name: name.as_ptr(),
            source: source
                .as_ref()
                .map_or(std::ptr::null(), |value| value.as_ptr()),
            sample_rate,
            channels: match setting.channels {
                Channels::Mono => 1,
                Channels::Stereo => 2,
            },
            fragment_bytes: positive(setting.capture_fragment_bytes.0)?,
            buffer_bytes: positive(setting.capture_buffer_bytes.0)?,
            pending_frames: positive(setting.pending_pcm_frames.0)?,
            frame_samples,
            opus_application: match setting.opus_application {
                OpusApplication::Voice => 0,
                OpusApplication::Audio => 1,
                OpusApplication::RestrictedLowDelay => 2,
            },
            bitrate: narrow(
                self.kilobits
                    .0
                    .checked_mul(1000)
                    .ok_or(Refusal::Unwritable)?,
            )?,
            bitrate_control: match setting.bitrate_control {
                BitrateControl::Constant => 0,
                BitrateControl::Variable => 1,
                BitrateControl::ConstrainedVariable => 2,
            },
            complexity: narrow(setting.encoding_complexity.0)?,
            signal: match setting.audio_signal {
                AudioSignal::Automatic => 0,
                AudioSignal::Voice => 1,
                AudioSignal::Music => 2,
            },
            bandwidth: match setting.audio_bandwidth {
                AudioBandwidth::Automatic => 0,
                AudioBandwidth::Narrowband => 1,
                AudioBandwidth::Mediumband => 2,
                AudioBandwidth::Wideband => 3,
                AudioBandwidth::Superwideband => 4,
                AudioBandwidth::Fullband => 5,
            },
            discontinuous_transmission: i32::from(
                setting.discontinuous_transmission == DiscontinuousTransmission::Enabled,
            ),
            forward_error_correction: i32::from(
                setting.forward_error_correction == ForwardErrorCorrection::Enabled,
            ),
            packet_loss: narrow(setting.expected_packet_loss.0)?,
            page_packets: positive(setting.page_packets.0)?,
            sync_each_page: i32::from(setting.audio_durability == AudioDurability::SyncEachPage),
            interrupt_on_gap: i32::from(setting.gap_handling == GapHandling::Interrupt),
            serial: i32::from_ne_bytes(
                listener_place::random_bytes().map_err(|_| Refusal::Unwritable)?,
            ),
        };
        // The worker owns both descriptors and the C strings until the call returns.
        Ok(unsafe { listener_record(&native, control.as_raw_fd(), file.as_raw_fd()) })
    }
}

pub(crate) trait NativeRecovery {
    fn finish_sound_pages(&self) -> bool;
}
impl NativeRecovery for File {
    fn finish_sound_pages(&self) -> bool {
        // The File keeps this worker's private read/write descriptor live throughout.
        unsafe { listener_recover_audio(self.as_raw_fd()) == 0 }
    }
}
