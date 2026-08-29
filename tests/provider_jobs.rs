use listener::{ProviderIdentifier, ProviderJobStore, ProviderPolicy};

#[test]
fn a_provider_result_survives_restart_and_retries_only_delivery_work() {
    let directory = tempfile::tempdir().expect("temporary job store");
    let path = directory.path().join("jobs.sema");
    let policy = ProviderPolicy::new(
        7,
        vec![ProviderIdentifier::WisprFlow, ProviderIdentifier::OpenAi],
    )
    .expect("valid provider policy");
    let first = ProviderJobStore::open(&path).expect("open jobs");
    let job = first
        .begin("capture-7", "/durable/capture-7.webm", policy)
        .expect("durable job");
    job.record_result("transcript without another provider call")
        .expect("persisted result");
    assert!(job.prepare_delivery().expect("delivery intent"));
    drop(job);
    drop(first);

    let reopened = ProviderJobStore::open(&path).expect("reopen jobs");
    let recovered = reopened.job("capture-7").expect("read durable job").expect("job exists");
    assert_eq!(
        recovered.result().expect("read durable result"),
        Some("transcript without another provider call".into())
    );
    assert!(recovered.prepare_delivery().expect("resume unreceipted delivery"));
    recovered.receipt_delivery().expect("persist delivery receipt");
    assert!(!recovered.prepare_delivery().expect("receipt prevents duplicate delivery"));
}
