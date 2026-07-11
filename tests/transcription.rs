use std::{
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::Path,
    sync::mpsc,
    thread,
    time::Duration,
};

use listener::{
    BatchTranscriptionInput, BatchTranscriptionRequest, Error, OpenAiCredentialSource,
    OpenAiRestTranscriber, OpenAiTranscriptionRequestConfiguration, RecordingAudioFormat,
    TranscriptionCustomization, TranscriptionCustomizationEnvironment,
    TranscriptionCustomizationTextSource,
};
use signal_listener::{AudioArtifactPath, DurableAudioArtifact, WirePath};
use tempfile::TempDir;

struct TranscriptionFixture;

impl TranscriptionFixture {
    fn maintained_terms() -> Vec<String> {
        Self::terms_from_fixture("transcription_customization_terms.txt")
    }

    fn removed_terms() -> Vec<String> {
        Self::terms_from_fixture("removed_transcription_terms.txt")
    }

    fn terms_from_fixture(file_name: &str) -> Vec<String> {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join(file_name);
        fs::read_to_string(path)
            .expect("read transcription fixture")
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect()
    }

    fn customization() -> TranscriptionCustomization {
        TranscriptionCustomization::new(Self::maintained_terms())
    }
}

#[test]
fn transcription_customization_round_trips_through_rkyv_archive() {
    let customization = TranscriptionFixture::customization();

    let bytes = customization.to_rkyv_bytes().expect("encode customization");
    let recovered =
        TranscriptionCustomization::from_rkyv_bytes(&bytes).expect("decode customization");

    assert_eq!(recovered, customization);
}

#[test]
fn transcription_customization_archive_rejects_magic_mismatch() {
    let mut bytes = TranscriptionFixture::customization()
        .to_rkyv_bytes()
        .expect("encode customization");
    bytes[0] ^= 0xff;

    let error = TranscriptionCustomization::from_rkyv_bytes(&bytes)
        .expect_err("magic mismatch should reject archive");

    assert!(
        matches!(error, Error::TranscriptionCustomizationArchiveMagic),
        "expected customization archive magic error, got {error:?}"
    );
}

#[test]
fn transcription_customization_archive_rejects_unsupported_version() {
    let mut bytes = TranscriptionFixture::customization()
        .to_rkyv_bytes()
        .expect("encode customization");
    bytes[8..12].copy_from_slice(&2_u32.to_le_bytes());

    let error = TranscriptionCustomization::from_rkyv_bytes(&bytes)
        .expect_err("unsupported version should reject archive");

    assert!(
        matches!(
            error,
            Error::TranscriptionCustomizationArchiveVersion {
                version: 2,
                expected: 1
            }
        ),
        "expected customization archive version error, got {error:?}"
    );
}

#[test]
fn transcription_prompt_uses_configured_terms_without_removed_terms() {
    let customization = TranscriptionFixture::customization();
    let prompt = customization.prompt();

    for term in TranscriptionFixture::maintained_terms() {
        assert!(
            prompt.as_str().contains(&term),
            "prompt should include maintained vocabulary term {term}"
        );
    }
    for term in TranscriptionFixture::removed_terms() {
        assert!(
            !prompt.as_str().contains(&term),
            "prompt should exclude removed vocabulary term {term}"
        );
    }
}

#[test]
fn text_source_compiles_terms_into_runtime_archive_shape() {
    let source = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures")
            .join("transcription_customization_terms.txt"),
    )
    .expect("read terms source");
    let customization = TranscriptionCustomizationTextSource::new(source).into_customization();
    let bytes = customization.to_rkyv_bytes().expect("encode customization");
    let recovered =
        TranscriptionCustomization::from_rkyv_bytes(&bytes).expect("decode customization");

    assert_eq!(
        recovered.vocabulary_terms(),
        TranscriptionFixture::maintained_terms().as_slice()
    );
}

#[test]
fn configured_customization_archive_decode_failure_is_visible() {
    let directory = TempDir::new().expect("create customization tempdir");
    let archive_path = directory.path().join("transcription-customization.rkyv");
    fs::write(&archive_path, b"not a transcription customization archive")
        .expect("write invalid customization archive");
    let environment = TranscriptionCustomizationEnvironment::new(Some(archive_path));

    let error = environment
        .prompt()
        .expect_err("invalid customization archive should fail");

    assert!(
        matches!(error, Error::TranscriptionCustomizationArchiveMagic),
        "expected customization archive magic error, got {error:?}"
    );
}

#[test]
fn openai_transcriber_customization_archive_failure_is_returned_without_panic() {
    let directory = TempDir::new().expect("create customization tempdir");
    let archive_path = directory.path().join("transcription-customization.rkyv");
    fs::write(&archive_path, b"not a transcription customization archive")
        .expect("write invalid customization archive");

    let error = OpenAiRestTranscriber::from_customization_archive_path(archive_path)
        .expect_err("invalid customization archive should return an error");

    assert!(
        matches!(error, Error::TranscriptionCustomizationArchiveMagic),
        "expected customization archive magic error, got {error:?}"
    );
}

#[test]
fn openai_form_sends_configured_prompt_with_every_transcription_request() {
    let server = OpenAiCaptureServer::spawn();
    let directory = TempDir::new().expect("create transcription tempdir");
    let input_path = directory.path().join("input.s16le");
    fs::write(&input_path, [0_u8, 0_u8]).expect("write pcm input");
    let prompt = TranscriptionFixture::customization().prompt();
    let transcriber = OpenAiRestTranscriber::new(
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .expect("build http client"),
        OpenAiCredentialSource::literal("test-openai-key"),
        OpenAiTranscriptionRequestConfiguration::new_with_endpoint(
            server.endpoint(),
            "test-model",
            "en",
            prompt.as_str(),
        ),
    );
    let artifact = DurableAudioArtifact::new(AudioArtifactPath::new(WirePath::new(
        input_path.to_string_lossy().into_owned(),
    )));
    let input = BatchTranscriptionInput::signed_sixteen_bit_little_endian_pcm(
        input_path,
        RecordingAudioFormat::signed_sixteen_bit_little_endian_mono_16khz(),
    );

    let transcript = transcriber
        .transcribe(BatchTranscriptionRequest::new_with_input(artifact, input))
        .expect("transcribe through local OpenAI endpoint");
    let request = server.request();

    assert_eq!(transcript.as_str(), "captured transcript");
    assert!(
        request.contains("name=\"prompt\""),
        "multipart request should carry a prompt part"
    );
    for term in TranscriptionFixture::maintained_terms() {
        assert!(
            request.contains(&term),
            "OpenAI request should include configured term {term} in prompt"
        );
    }
}

struct OpenAiCaptureServer {
    endpoint: String,
    request_receiver: mpsc::Receiver<String>,
}

impl OpenAiCaptureServer {
    fn spawn() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind OpenAI test server");
        let endpoint = format!(
            "http://{}",
            listener.local_addr().expect("read OpenAI test address")
        );
        let (request_sender, request_receiver) = mpsc::channel();
        thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept OpenAI request");
            let request = RecordedHttpRequest::read_from(&mut stream);
            RecordedHttpRequest::write_success_response(&mut stream);
            request_sender.send(request).expect("record OpenAI request");
        });
        Self {
            endpoint,
            request_receiver,
        }
    }

    fn endpoint(&self) -> &str {
        &self.endpoint
    }

    fn request(self) -> String {
        self.request_receiver
            .recv_timeout(Duration::from_secs(5))
            .expect("receive OpenAI request")
    }
}

struct RecordedHttpRequest;

enum HttpRequestBodyFraming {
    ContentLength(usize),
    Chunked,
}

impl RecordedHttpRequest {
    fn read_from(stream: &mut TcpStream) -> String {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .expect("set OpenAI request read timeout");
        let mut bytes = Vec::new();
        let mut buffer = [0_u8; 4096];
        let header_end = loop {
            Self::read_more(stream, &mut bytes, &mut buffer, "headers arrived");
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        match Self::body_framing(&headers) {
            HttpRequestBodyFraming::ContentLength(content_length) => {
                Self::read_content_length_body(
                    stream,
                    &mut bytes,
                    &mut buffer,
                    header_end,
                    content_length,
                );
            }
            HttpRequestBodyFraming::Chunked => {
                Self::read_chunked_body(stream, &mut bytes, &mut buffer, header_end);
            }
        }
        String::from_utf8_lossy(&bytes).into_owned()
    }

    fn read_content_length_body(
        stream: &mut TcpStream,
        bytes: &mut Vec<u8>,
        buffer: &mut [u8],
        header_end: usize,
        content_length: usize,
    ) {
        while bytes.len() - header_end < content_length {
            Self::read_more(stream, bytes, buffer, "body completed");
        }
    }

    fn read_chunked_body(
        stream: &mut TcpStream,
        bytes: &mut Vec<u8>,
        buffer: &mut [u8],
        header_end: usize,
    ) {
        let mut chunk_start = header_end;
        loop {
            let chunk_size_line_end = loop {
                if let Some(index) = bytes[chunk_start..]
                    .windows(2)
                    .position(|window| window == b"\r\n")
                {
                    break chunk_start + index;
                }
                Self::read_more(stream, bytes, buffer, "chunk size arrived");
            };
            let chunk_size_line = std::str::from_utf8(&bytes[chunk_start..chunk_size_line_end])
                .expect("OpenAI chunk size is UTF-8");
            let chunk_size_hex = chunk_size_line
                .split(';')
                .next()
                .expect("OpenAI chunk size line")
                .trim();
            let chunk_size =
                usize::from_str_radix(chunk_size_hex, 16).expect("parse OpenAI chunk size");
            let chunk_payload_start = chunk_size_line_end + 2;
            let chunk_end = chunk_payload_start + chunk_size + 2;
            while bytes.len() < chunk_end {
                Self::read_more(stream, bytes, buffer, "chunk body completed");
            }
            assert_eq!(
                &bytes[chunk_payload_start + chunk_size..chunk_end],
                b"\r\n",
                "OpenAI chunk must end with CRLF"
            );
            chunk_start = chunk_end;
            if chunk_size == 0 {
                break;
            }
        }
    }

    fn read_more(
        stream: &mut TcpStream,
        bytes: &mut Vec<u8>,
        buffer: &mut [u8],
        expectation: &str,
    ) {
        let count = stream.read(buffer).expect("read OpenAI request");
        assert!(count > 0, "OpenAI client closed before {expectation}");
        bytes.extend_from_slice(&buffer[..count]);
    }

    fn body_framing(headers: &str) -> HttpRequestBodyFraming {
        for line in headers.lines() {
            if let Some((name, value)) = line.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    return HttpRequestBodyFraming::ContentLength(
                        value
                            .trim()
                            .parse()
                            .expect("parse OpenAI request content length"),
                    );
                }
                if name.eq_ignore_ascii_case("transfer-encoding")
                    && value.trim().eq_ignore_ascii_case("chunked")
                {
                    return HttpRequestBodyFraming::Chunked;
                }
            }
        }
        panic!("OpenAI request body framing");
    }

    fn write_success_response(stream: &mut TcpStream) {
        let body = r#"{"text":"captured transcript"}"#;
        write!(
            stream,
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        )
        .expect("write OpenAI response");
    }
}
