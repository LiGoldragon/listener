use std::{
    io::{Read, Write},
    os::unix::net::UnixStream,
    sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}},
};

use listener::{
    MetaProviderPolicyClient, MetaProviderPolicyServer, MetaProviderPolicyService,
    MetaProviderPolicySocket, ProviderIdentifier, ProviderPolicyStore,
};
use meta_signal_listener::{
    Frame, FrameBody, Input, Output, TranscriptionProviderConfigurationRejectionReason,
    TranscriptionProviderId,
    TranscriptionProviderIds, TranscriptionProviderPolicy,
};
use signal_frame::{ExchangeIdentifier, ExchangeLane, LaneSequence, Reply, SessionEpoch, SubReply};

fn wispr_then_openai() -> TranscriptionProviderPolicy {
    TranscriptionProviderPolicy::new(TranscriptionProviderIds::new(vec![
        TranscriptionProviderId::WisprFlow,
        TranscriptionProviderId::OpenAi,
    ]))
}

#[test]
fn owner_policy_round_trips_after_restart_with_a_monotonic_generation() {
    let directory = tempfile::tempdir().expect("temporary policy store");
    let path = directory.path().join("listener.sema");

    let first_generation = {
        let store = ProviderPolicyStore::open(&path).expect("open new store");
        let service = MetaProviderPolicyService::new(store);
        let Output::TranscriptionProvidersConfigured(receipt) = service.handle(
            Input::ConfigureTranscriptionProviders(wispr_then_openai()),
        ) else {
            panic!("provider policy must be accepted");
        };
        receipt.payload().payload().value()
    };

    let store = ProviderPolicyStore::open(&path).expect("reopen stored policy");
    let restored = store
        .current()
        .expect("read stored policy")
        .expect("stored policy");
    assert_eq!(restored.generation(), first_generation);
    assert_eq!(
        restored.providers(),
        &[ProviderIdentifier::WisprFlow, ProviderIdentifier::OpenAi]
    );

    let service = MetaProviderPolicyService::new(store);
    let Output::TranscriptionProvidersConfigured(receipt) = service.handle(
        Input::ConfigureTranscriptionProviders(TranscriptionProviderPolicy::new(
            TranscriptionProviderIds::new(vec![TranscriptionProviderId::OpenAi]),
        )),
    ) else {
        panic!("replacement policy must be accepted");
    };
    assert_eq!(receipt.payload().payload().value(), first_generation + 1);
}

#[test]
fn owner_policy_rejects_empty_and_duplicate_provider_orders() {
    let directory = tempfile::tempdir().expect("temporary policy store");
    let service = MetaProviderPolicyService::new(
        ProviderPolicyStore::open(directory.path().join("listener.sema")).expect("open store"),
    );

    for policy in [
        TranscriptionProviderPolicy::new(TranscriptionProviderIds::new(Vec::new())),
        TranscriptionProviderPolicy::new(TranscriptionProviderIds::new(vec![
            TranscriptionProviderId::WisprFlow,
            TranscriptionProviderId::WisprFlow,
        ])),
    ] {
        let Output::TranscriptionProviderConfigurationRejected(rejection) =
            service.handle(Input::ConfigureTranscriptionProviders(policy))
        else {
            panic!("invalid policy must be rejected");
        };
        assert!(matches!(
            rejection.payload(),
            TranscriptionProviderConfigurationRejectionReason::Empty
                | TranscriptionProviderConfigurationRejectionReason::Duplicate
        ));
    }
}

#[test]
fn concurrent_meta_updates_receive_distinct_ordered_generations() {
    let directory = tempfile::tempdir().expect("temporary policy store");
    let service = Arc::new(MetaProviderPolicyService::new(
        ProviderPolicyStore::open(directory.path().join("listener.sema")).expect("open store"),
    ));
    let generations = Arc::new(Mutex::new(Vec::new()));
    std::thread::scope(|scope| {
        for _ in 0..2 {
            let service = Arc::clone(&service);
            let generations = Arc::clone(&generations);
            scope.spawn(move || {
                let Output::TranscriptionProvidersConfigured(receipt) = service.handle(
                    Input::ConfigureTranscriptionProviders(wispr_then_openai()),
                ) else {
                    panic!("valid policy must be accepted");
                };
                generations
                    .lock()
                    .expect("generation recorder")
                    .push(receipt.payload().payload().value());
            });
        }
    });
    let mut generations = generations.lock().expect("generation recorder").clone();
    generations.sort_unstable();
    assert_eq!(generations, vec![1, 2]);
}

#[test]
fn privileged_socket_preserves_the_meta_exchange_and_returns_the_persisted_generation() {
    let directory = tempfile::tempdir().expect("temporary policy store");
    let service = Arc::new(MetaProviderPolicyService::new(
        ProviderPolicyStore::open(directory.path().join("listener.sema")).expect("open store"),
    ));
    let socket = MetaProviderPolicySocket::new(Arc::clone(&service));
    let (mut client, server_stream) = UnixStream::pair().expect("socket pair");
    let exchange = ExchangeIdentifier::new(
        SessionEpoch::new(7),
        ExchangeLane::Connector,
        LaneSequence::first(),
    );

    std::thread::scope(|scope| {
        scope.spawn(|| socket.handle_connection(server_stream).expect("serve meta exchange"));
        let bytes = Input::ConfigureTranscriptionProviders(wispr_then_openai())
            .into_frame(exchange)
            .encode_length_prefixed()
            .expect("encode meta request");
        client.write_all(&bytes).expect("send meta request");
        let mut prefix = [0_u8; 4];
        client.read_exact(&mut prefix).expect("read reply prefix");
        let length = u32::from_be_bytes(prefix) as usize;
        let mut bytes = prefix.to_vec();
        bytes.resize(length + prefix.len(), 0);
        client.read_exact(&mut bytes[prefix.len()..]).expect("read reply frame");
        let FrameBody::Reply { exchange: actual, reply } =
            Frame::decode_length_prefixed(&bytes).expect("decode meta reply").into_body()
        else {
            panic!("expected meta reply frame");
        };
        assert_eq!(actual, exchange);
        let Reply::Accepted { per_operation, .. } = reply else {
            panic!("expected accepted meta reply");
        };
        let SubReply::Ok(output) = per_operation.into_head() else {
            panic!("expected successful meta operation");
        };
        let Output::TranscriptionProvidersConfigured(receipt) = output else {
            panic!("expected configured policy reply");
        };
        assert_eq!(receipt.payload().payload().value(), 1);
    });
}

#[test]
fn privileged_meta_server_binds_the_configured_socket_and_stops_with_its_owner() {
    let directory = tempfile::tempdir().expect("temporary policy server");
    let socket_path = directory.path().join("listener-meta.sock");
    let service = Arc::new(MetaProviderPolicyService::new(
        ProviderPolicyStore::open(directory.path().join("listener.sema")).expect("open store"),
    ));
    let server = MetaProviderPolicyServer::bind(&socket_path, 0o600, service).expect("bind meta socket");
    let stopping = Arc::new(AtomicBool::new(false));
    let worker = server.spawn_until(Arc::clone(&stopping));

    let output = MetaProviderPolicyClient::new(&socket_path)
        .request(Input::ConfigureTranscriptionProviders(wispr_then_openai()))
        .expect("request durable policy");
    assert!(matches!(output, Output::TranscriptionProvidersConfigured(_)));
    stopping.store(true, Ordering::Release);
    worker.join().expect("server thread").expect("server exits cleanly");
}
