//! Nexus-owned durable transcription-provider policy.
//!
//! The policy contains only ordered provider identities and a generation. It
//! deliberately has no credential field: session material stays behind the
//! request-time provider boundary.

use std::{fs, path::Path, sync::Mutex};

use meta_signal_listener::{
    Input, OperationKind, Output, ProviderPolicyGeneration, ProviderPolicyGenerationReceipt,
    Reason, RequestUnimplemented, TranscriptionProviderConfigurationRejectionReason,
    TranscriptionProviderId, TranscriptionProviderPolicy,
    UnimplementedOperationKind, UnimplementedReason,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use sema_engine::{
    Engine, EngineOpen, EngineRecord, FamilyName, QueryPlan, RecordKey,
    SchemaHash, SchemaVersion, TableDescriptor, TableName, TableReference,
};
use thiserror::Error;

use crate::{ProviderIdentifier, ProviderPolicy};

const SCHEMA_VERSION: SchemaVersion = SchemaVersion::new(1);
const POLICY_TABLE: TableName = TableName::new("listener_transcription_provider_policy");
const POLICY_KEY: &str = "current";

#[derive(Debug, Error)]
pub enum ProviderPolicyStoreError {
    #[error("provider policy sema engine: {0}")]
    Engine(#[from] sema_engine::Error),
    #[error("provider policy store path has no parent")]
    StorePathHasNoParent,
    #[error("provider policy store filesystem: {0}")]
    Filesystem(#[from] std::io::Error),
    #[error("provider policy store has {count} current-policy rows")]
    PolicyInvariant { count: usize },
    #[error("provider policy generation is exhausted")]
    GenerationExhausted,
}

#[derive(Archive, RkyvSerialize, RkyvDeserialize, Clone, PartialEq, Eq)]
struct StoredProviderPolicy {
    generation: u64,
    providers: Vec<TranscriptionProviderId>,
}

impl EngineRecord for StoredProviderPolicy {
    fn record_key(&self) -> RecordKey {
        RecordKey::new(POLICY_KEY)
    }
}

/// The internal validated shape of an owner-supplied provider order.
struct ValidatedProviderOrder {
    providers: Vec<TranscriptionProviderId>,
}

/// Validates the closed owner policy vocabulary before it reaches durable state.
trait ValidatesProviderOrder {
    fn from_policy(
        policy: &TranscriptionProviderPolicy,
    ) -> Result<Self, TranscriptionProviderConfigurationRejectionReason>
    where
        Self: Sized;
}

impl ValidatesProviderOrder for ValidatedProviderOrder {
    fn from_policy(
        policy: &TranscriptionProviderPolicy,
    ) -> Result<Self, TranscriptionProviderConfigurationRejectionReason> {
        let providers = policy.providers().to_vec();
        if providers.is_empty() {
            return Err(TranscriptionProviderConfigurationRejectionReason::Empty);
        }
        if providers
            .iter()
            .enumerate()
            .any(|(index, provider)| providers[..index].contains(provider))
        {
            return Err(TranscriptionProviderConfigurationRejectionReason::Duplicate);
        }
        Ok(Self { providers })
    }
}

/// The durable policy owner role.
trait StoresProviderPolicy {
    fn current_policy(&self) -> Result<Option<ProviderPolicy>, ProviderPolicyStoreError>;
    fn replace_policy(
        &mut self,
        policy: ValidatedProviderOrder,
    ) -> Result<u64, ProviderPolicyStoreError>;
}

pub struct ProviderPolicyStore {
    engine: Engine,
    policies: TableReference<StoredProviderPolicy>,
}

impl ProviderPolicyStore {
    // Exception: Too trivial. Opening is construction of this concrete Sema owner.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, ProviderPolicyStoreError> {
        let path = path.as_ref();
        let parent = path
            .parent()
            .ok_or(ProviderPolicyStoreError::StorePathHasNoParent)?;
        fs::create_dir_all(parent)?;
        let mut engine = Engine::open(EngineOpen::new(
            path.display().to_string(),
            SCHEMA_VERSION,
        ))?;
        let policies = engine.register_table(TableDescriptor::new(
            POLICY_TABLE,
            FamilyName::new("listener-transcription-provider-policy"),
            SchemaHash::for_label("listener-transcription-provider-policy-v1"),
        ))?;
        Ok(Self { engine, policies })
    }

    // Exception: Too trivial. This is the public read projection of its store role.
    pub fn current(&self) -> Result<Option<ProviderPolicy>, ProviderPolicyStoreError> {
        self.current_policy()
    }
}

impl StoresProviderPolicy for ProviderPolicyStore {
    fn current_policy(&self) -> Result<Option<ProviderPolicy>, ProviderPolicyStoreError> {
        match self.engine.match_records(QueryPlan::all(self.policies))?.records() {
            [] => Ok(None),
            [stored] => Ok(ProviderPolicy::new(
                stored.generation,
                stored
                    .providers
                    .iter()
                    .copied()
                    .map(ProviderIdentifier::from_meta)
                    .collect(),
            )),
            rows => Err(ProviderPolicyStoreError::PolicyInvariant { count: rows.len() }),
        }
    }

    fn replace_policy(
        &mut self,
        policy: ValidatedProviderOrder,
    ) -> Result<u64, ProviderPolicyStoreError> {
        let (generation, replaces_existing) = match self
            .engine
            .match_records(QueryPlan::all(self.policies))?
            .records()
        {
            [] => (1, false),
            [stored] => (
                stored
                    .generation
                    .checked_add(1)
                    .ok_or(ProviderPolicyStoreError::GenerationExhausted)?,
                true,
            ),
            rows => return Err(ProviderPolicyStoreError::PolicyInvariant { count: rows.len() }),
        };
        let stored = StoredProviderPolicy {
            generation,
            providers: policy.providers,
        };
        let commit = if replaces_existing {
            self
                .engine
                .begin_atomic_commit()
                .mutate(self.policies, stored)
        } else {
            self.engine.begin_atomic_commit().assert(self.policies, stored)
        };
        self.engine.commit_atomic(commit)?;
        Ok(generation)
    }
}

/// Privileged meta-policy operation handler. A service owns serialized access
/// to the policy store, so every accepted mutation receives a distinct,
/// durable generation.
pub struct MetaProviderPolicyService {
    store: Mutex<ProviderPolicyStore>,
}

/// Handles one owner-only meta operation using the configured policy store.
trait HandlesMetaProviderPolicy {
    fn handle(&self, input: Input) -> Output;
}

impl MetaProviderPolicyService {
    // Exception: Too trivial. Construction establishes the one serialized policy owner.
    pub fn new(store: ProviderPolicyStore) -> Self {
        Self {
            store: Mutex::new(store),
        }
    }
}

impl HandlesMetaProviderPolicy for MetaProviderPolicyService {
    fn handle(&self, input: Input) -> Output {
        let Input::ConfigureTranscriptionProviders(policy) = input else {
            return Self::unimplemented(input.kind());
        };
        let policy = match ValidatedProviderOrder::from_policy(&policy) {
            Ok(policy) => policy,
            Err(reason) => return Output::transcription_provider_configuration_rejected(reason),
        };
        let Ok(mut store) = self.store.lock() else {
            return Self::unavailable();
        };
        match store.replace_policy(policy) {
            Ok(generation) => Output::transcription_providers_configured(
                ProviderPolicyGenerationReceipt::new(ProviderPolicyGeneration::new(generation)),
            ),
            Err(_) => Self::unavailable(),
        }
    }
}

impl MetaProviderPolicyService {
    // Exception: Too trivial. This is the concrete typed service entrypoint.
    pub fn handle(&self, input: Input) -> Output {
        HandlesMetaProviderPolicy::handle(self, input)
    }

    fn unavailable() -> Output {
        Self::unimplemented(OperationKind::ConfigureTranscriptionProviders)
    }

    fn unimplemented(kind: OperationKind) -> Output {
        Output::unimplemented(RequestUnimplemented {
            unimplemented_operation_kind: UnimplementedOperationKind::new(kind),
            reason: Reason::new(UnimplementedReason::DependencyNotReady),
        })
    }
}

impl ProviderIdentifier {
    fn from_meta(identifier: TranscriptionProviderId) -> Self {
        match identifier {
            TranscriptionProviderId::WisprFlow => Self::WisprFlow,
            TranscriptionProviderId::OpenAi => Self::OpenAi,
        }
    }
}
