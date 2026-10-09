use std::{borrow::Cow, collections::HashSet};

use dolos_core::{
    BlockSlot, ChainError, ChainPoint, Domain, EraCbor, Genesis, MempoolAwareUtxoStore, MempoolTx,
    StateStore as _, TxoRef, UtxoMap,
};

use amaru_uplc::{
    arena::Arena,
    binder::DeBruijn,
    bumpalo::Bump,
    constant::Constant,
    data::PlutusData as UplcData,
    flat,
    machine::{ExBudget, PlutusVersion},
    program::Program,
    term::Term,
};
use itertools::Itertools as _;
use pallas::{
    codec::minicbor,
    ledger::{
        addresses::Address,
        primitives::{
            conway::{CostModels, ExUnits, Language, PlutusData, RedeemerTag, TransactionOutput},
            NetworkId, TransactionInput,
        },
        traverse::{MultiEraInput, MultiEraOutput, MultiEraRedeemer, MultiEraTx},
        validate::phase2::{
            error::Error as Phase2Error,
            script_context::{
                find_script, DataLookupTable, ResolvedInput, ScriptVersion, SlotConfig, TxInfoV1,
                TxInfoV2, TxInfoV3,
            },
            to_plutus_data::ToPlutusData as _,
        },
    },
};
use tracing::debug;

use crate::eras::ChainSummary;

pub fn validate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
    tip: Option<ChainPoint>,
    genesis: &Genesis,
) -> Result<MempoolTx, ChainError> {
    let tx = MultiEraTx::decode(cbor)?;
    let hash = tx.hash();

    let pparams = crate::load_effective_pparams::<D>(utxos.state())?;
    let pparams = crate::utils::pparams_to_pallas(&pparams);

    let network_id = match genesis.shelley.network_id.as_ref() {
        Some(network) => match network.as_str() {
            "Mainnet" => Some(NetworkId::Mainnet.into()),
            "Testnet" => Some(NetworkId::Testnet.into()),
            _ => None,
        },
        None => None,
    }
    .unwrap();

    let env = pallas::ledger::validate::utils::Environment {
        prot_params: pparams,
        prot_magic: genesis.shelley.network_magic.unwrap(),
        block_slot: tip.clone().unwrap().slot(),
        network_id,
        acnt: Some(pallas::ledger::validate::utils::AccountState::default()),
    };

    let input_refs = tx.requires().iter().map(From::from).collect();

    let utxos_matches = utxos.get_utxos(input_refs)?;

    let mut pallas_utxos = pallas::ledger::validate::utils::UTxOs::new();

    for (txoref, eracbor) in utxos_matches.iter() {
        let tx_in = TransactionInput {
            transaction_id: txoref.0,
            index: txoref.1.into(),
        };

        let input = MultiEraInput::AlonzoCompatible(<Box<Cow<'_, TransactionInput>>>::from(
            Cow::Owned(tx_in),
        ));

        let eracbor = eracbor.as_ref();

        let output = MultiEraOutput::try_from(eracbor)?;

        pallas_utxos.insert(input, output);
    }

    pallas::ledger::validate::phase1::validate_tx(
        &tx,
        0,
        &env,
        &pallas_utxos,
        &mut pallas::ledger::validate::utils::CertState::default(),
    )?;

    let report = evaluate_tx::<D>(cbor, utxos)?;

    for eval in report.iter() {
        if !eval.success {
            return Err(ChainError::Phase2ValidationRejected(eval.logs.clone()));
        }
    }

    debug!(
        phase1 = true,
        phase2 = true,
        redeemer_count = report.len(),
        "tx validated"
    );

    let era = u16::from(tx.era());
    let payload = EraCbor(era, cbor.into());

    let tx = MempoolTx::new(hash, payload, report);

    Ok(tx)
}

pub fn evaluate_tx<D: Domain>(
    cbor: &[u8],
    utxos: &MempoolAwareUtxoStore<D>,
) -> Result<pallas::ledger::validate::phase2::EvalReport, ChainError> {
    let tx = MultiEraTx::decode(cbor)?;

    let eras = crate::eras::load_era_summary::<D>(utxos.state())?;

    let pparams = crate::load_effective_pparams::<D>(utxos.state())?;

    let pparams = crate::utils::pparams_to_pallas(&pparams);

    // `eras` keeps slot_length/timestamp in seconds; the Plutus ScriptContext
    // needs POSIXTime in milliseconds. The conversion lives in the helper.
    let slot_config = eras.to_pallas_slot_config();

    let input_refs = tx.requires().iter().map(From::from).collect();

    let utxos: pallas::ledger::validate::utils::UtxoMap = utxos
        .get_utxos(input_refs)?
        .into_iter()
        .map(|(TxoRef(a, b), eracbor)| {
            let era = eracbor.era().try_into().expect("era out of range");

            (
                pallas::ledger::validate::utils::TxoRef::from((a, b)),
                pallas::ledger::validate::utils::EraCbor::from((era, eracbor.cbor().into())),
            )
        })
        .collect();

    let report = pallas::ledger::validate::phase2::evaluate_tx(&tx, &pparams, &utxos, &slot_config)
        .map_err(|e| ChainError::Phase2EvaluationError(e.to_string()))?;

    Ok(report)
}

/// Why the ledger cannot run one redeemer, or why its script fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedeemerFailure {
    /// The redeemer points to no item that a script locks.
    Extraneous,
    /// No Plutus script has the hash that the redeemer points to.
    MissingScript,
    /// The transaction lacks the datum with this hash.
    MissingDatum(String),
    /// The protocol parameters have no cost model for this language.
    MissingCostModel(Language),
    /// The UTxO set lacks this input, so the script context cannot exist.
    UnknownInput(TxoRef),
    /// The script context cannot exist for this reason.
    Context(String),
    /// The script ran and failed.
    Validation { error: String, traces: Vec<String> },
}

impl From<Phase2Error> for RedeemerFailure {
    fn from(error: Phase2Error) -> Self {
        match error {
            Phase2Error::MissingScriptForRedeemer
            | Phase2Error::ExtraneousRedeemer
            | Phase2Error::NonScriptWithdrawal
            | Phase2Error::NonScriptStakeCredential
            | Phase2Error::UnsupportedCertificateType
            | Phase2Error::NoGuardrailScriptForProcedure => Self::Extraneous,
            Phase2Error::MissingRequiredScript { .. } | Phase2Error::NativeScriptPhaseTwo => {
                Self::MissingScript
            }
            Phase2Error::MissingRequiredDatum { hash } => Self::MissingDatum(hash),
            Phase2Error::CostModelNotFound(language) => Self::MissingCostModel(language),
            Phase2Error::ResolvedInputNotFound(input) => {
                Self::UnknownInput(TxoRef(input.transaction_id, input.index as u32))
            }
            other => Self::Context(other.to_string()),
        }
    }
}

/// The result of one redeemer: the execution units it uses, or why it fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedeemerReport {
    pub tag: RedeemerTag,
    pub index: u32,
    pub result: Result<ExUnits, RedeemerFailure>,
}

/// The ledger values that a Plutus script run uses.
struct ScriptParams {
    protocol_major: u32,
    budget: ExUnits,
    cost_models: CostModels,
    /// The first slot that the ledger cannot convert to a time.
    horizon: BlockSlot,
}

impl ScriptParams {
    fn cost_model(&self, language: &Language) -> Option<&[i64]> {
        let model = match language {
            Language::PlutusV1 => self.cost_models.plutus_v1.as_deref(),
            Language::PlutusV2 => self.cost_models.plutus_v2.as_deref(),
            Language::PlutusV3 => self.cost_models.plutus_v3.as_deref(),
        }?;

        // amaru-uplc names the PlutusV3 parameters only in an array of 251 or
        // 297 values. The ledger appends new parameters at the end, so the
        // first 297 values keep their names.
        match language {
            Language::PlutusV3 if model.len() > 297 => Some(&model[..297]),
            _ => Some(model),
        }
    }
}

/// Evaluates each redeemer of a Conway transaction against `utxos` and the
/// current protocol parameters. Returns the reports ordered by purpose and
/// index.
pub fn evaluate_redeemers<D: Domain>(
    tx: &MultiEraTx,
    utxos: &UtxoMap,
    state: &D::State,
    genesis: &Genesis,
) -> Result<Vec<RedeemerReport>, ChainError> {
    let eras = crate::eras::load_era_summary::<D>(state)?;
    let pparams = crate::load_effective_pparams::<D>(state)?;
    let slot_config = eras.to_pallas_slot_config();
    let tip = state
        .read_cursor()?
        .map(|point| point.slot())
        .unwrap_or_default();

    let params = ScriptParams {
        protocol_major: u32::from(pparams.protocol_major_or_default()),
        budget: pparams.max_tx_ex_units_or_default(),
        cost_models: pparams.cost_models_for_script_languages(),
        horizon: time_horizon(&eras, tip, crate::utils::stability_window(genesis)),
    };

    // Scripts and the script context come from the spent and the reference
    // inputs only, never from the collateral.
    let script_inputs = tx
        .inputs()
        .iter()
        .chain(tx.reference_inputs().iter())
        .map(TxoRef::from)
        .collect::<HashSet<_>>();

    let resolved = utxos
        .iter()
        .filter(|(txo_ref, _)| script_inputs.contains(*txo_ref))
        .map(|(TxoRef(hash, index), utxo)| {
            Ok(ResolvedInput {
                input: TransactionInput {
                    transaction_id: *hash,
                    index: u64::from(*index),
                },
                output: minicbor::decode(utxo.cbor())?,
            })
        })
        .collect::<Result<Vec<_>, ChainError>>()?;

    let lookup_table = DataLookupTable::from_transaction(tx, &resolved);

    let mut reports = tx
        .redeemers()
        .iter()
        .map(|redeemer| {
            let result = if spends_no_known_script(redeemer, tx, &resolved) {
                Err(RedeemerFailure::Extraneous)
            } else {
                run_redeemer(
                    redeemer,
                    tx,
                    &resolved,
                    &lookup_table,
                    &slot_config,
                    &params,
                )
            };

            RedeemerReport {
                tag: redeemer.tag(),
                index: redeemer.index(),
                result,
            }
        })
        .collect::<Vec<_>>();

    reports.sort_by_key(|report| (report.tag as u8, report.index));

    Ok(reports)
}

/// Returns the first slot that a node cannot convert to a time. A node
/// forecasts the current era up to the first epoch boundary after the tip plus
/// the safe zone.
fn time_horizon(eras: &ChainSummary, tip: BlockSlot, safe_zone: u64) -> BlockSlot {
    let (epoch, _) = eras.slot_epoch(tip + safe_zone);
    eras.epoch_start(epoch + 1)
}

/// Tells if a spend redeemer points to an input that `utxos` lacks or that no
/// script locks. The ledger maps such a pointer to no script hash, but pallas
/// reports the first unknown input of the whole transaction instead.
fn spends_no_known_script(
    redeemer: &MultiEraRedeemer,
    tx: &MultiEraTx,
    utxos: &[ResolvedInput],
) -> bool {
    if redeemer.tag() != RedeemerTag::Spend {
        return false;
    }

    let Some(tx) = tx.as_conway() else {
        return false;
    };

    // pallas reports a pointer past the last input itself.
    let Some(input) = tx
        .transaction_body
        .inputs
        .iter()
        .sorted()
        .nth(redeemer.index() as usize)
    else {
        return false;
    };

    let Some(utxo) = utxos.iter().find(|utxo| &utxo.input == input) else {
        return true;
    };

    let address = match &utxo.output {
        TransactionOutput::Legacy(output) => output.address.as_ref(),
        TransactionOutput::PostAlonzo(output) => output.address.as_ref(),
    };

    !matches!(
        Address::from_bytes(address),
        Ok(Address::Shelley(address)) if address.payment().is_script()
    )
}

/// Runs the script of one redeemer like the pallas `eval_redeemer`, but with
/// the cost model and the transaction budget of the protocol parameters.
fn run_redeemer(
    redeemer: &MultiEraRedeemer,
    tx: &MultiEraTx,
    utxos: &[ResolvedInput],
    lookup_table: &DataLookupTable,
    slot_config: &SlotConfig,
    params: &ScriptParams,
) -> Result<ExUnits, RedeemerFailure> {
    let tx = tx.as_conway().ok_or(Phase2Error::WrongEra())?;
    let redeemer = redeemer
        .into_conway_deprecated()
        .ok_or(Phase2Error::WrongEra())?;

    let (language, script, datum) = match find_script(&redeemer, tx, utxos, lookup_table)? {
        (ScriptVersion::Native(_), _) => return Err(RedeemerFailure::MissingScript),
        (ScriptVersion::V1(script), datum) => (Language::PlutusV1, script.0, datum),
        (ScriptVersion::V2(script), datum) => (Language::PlutusV2, script.0, datum),
        (ScriptVersion::V3(script), datum) => (Language::PlutusV3, script.0, datum),
    };

    let cost_model = params
        .cost_model(&language)
        .ok_or_else(|| RedeemerFailure::MissingCostModel(language.clone()))?;

    let bounds = [
        tx.transaction_body.validity_interval_start,
        tx.transaction_body.ttl,
    ];

    if let Some(slot) = bounds
        .into_iter()
        .flatten()
        .find(|slot| *slot >= params.horizon)
    {
        return Err(RedeemerFailure::Context(format!(
            "Uncomputable slot arithmetic; transaction's validity bounds go beyond the \
             foreseeable end of the current era: slot {slot} is at or after slot {}",
            params.horizon
        )));
    }

    let tx_info = match &language {
        Language::PlutusV1 => TxInfoV1::from_transaction(tx, utxos, slot_config)?,
        Language::PlutusV2 => TxInfoV2::from_transaction(tx, utxos, slot_config)?,
        Language::PlutusV3 => TxInfoV3::from_transaction(tx, utxos, slot_config)?,
    };

    let context = tx_info
        .into_script_context(&redeemer, datum.as_ref())
        .ok_or(Phase2Error::ScriptContextBuildError)?
        .to_plutus_data();

    let version = match &language {
        Language::PlutusV1 => PlutusVersion::V1,
        Language::PlutusV2 => PlutusVersion::V2,
        Language::PlutusV3 => PlutusVersion::V3,
    };

    let arena = Arena::from_bump(Bump::with_capacity(1_024_000));

    let flat_bytes: minicbor::bytes::ByteVec =
        minicbor::decode(&script).map_err(Phase2Error::from)?;

    let program: &Program<DeBruijn> =
        flat::decode(&arena, &flat_bytes, version, params.protocol_major)
            .map_err(Phase2Error::from)?;

    let argument = |data: &PlutusData| {
        // Both crates encode Plutus data in the same CBOR form.
        let bytes = minicbor::to_vec(data).expect("Plutus data encodes");
        let data = UplcData::from_cbor(&arena, &bytes).expect("Plutus data decodes");
        Term::data(&arena, data)
    };

    let program = match &language {
        Language::PlutusV1 | Language::PlutusV2 => {
            let program = match &datum {
                Some(datum) => program.apply(&arena, argument(datum)),
                None => program,
            };

            program
                .apply(&arena, argument(&redeemer.to_plutus_data()))
                .apply(&arena, argument(&context))
        }
        Language::PlutusV3 => program.apply(&arena, argument(&context)),
    };

    let budget = ExBudget::new(params.budget.mem as i64, params.budget.steps as i64);
    let result = program.eval_with_params(&arena, version, cost_model, budget);

    let success = match (&result.term, &language) {
        (Ok(_), Language::PlutusV1 | Language::PlutusV2) => true,
        (Ok(term), Language::PlutusV3) => matches!(
            term,
            Term::Constant(constant) if matches!(**constant, Constant::Unit)
        ),
        (Err(_), _) => false,
    };

    if !success {
        let error = match &result.term {
            Err(error) => error.to_string(),
            Ok(_) => "the script returned a value other than unit".into(),
        };

        return Err(RedeemerFailure::Validation {
            error,
            traces: result.info.logs,
        });
    }

    let consumed = result.info.consumed_budget;

    Ok(ExUnits {
        mem: consumed.mem.max(0) as u64,
        steps: consumed.cpu.max(0) as u64,
    })
}
