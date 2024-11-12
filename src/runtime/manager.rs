use crate::{
    frontend::typecheck::Type,
    runtime::{
        lock::Lock,
        message::{Message, Val},
        transaction::{Txn, TxnId},
    },
};

use inline_colorization::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{Receiver, Sender};

pub enum WorkerKind {
    Var,
    Def,
}

pub struct LockWorkerInfo {
    pub lock: Lock,
    pub worker_name: String,
}

pub struct ValTxnInfo {
    pub val: Val,
    pub txn_id: TxnId,
}

pub struct Manager {
    // cloned and given to new workers when creating them
    pub sender_to_manager: Sender<Message>,
    pub receiver_from_workers: Receiver<Message>,
    pub senders_to_workers: HashMap<String, Sender<Message>>,
    pub typing_env: HashMap<String, Type>,
    pub worker_kind_env: HashMap<String, WorkerKind>,
    pub txn_locks_map: HashMap<TxnId, HashSet<LockWorkerInfo>>,
    // [name |-> subscribers]
    pub dependency_graph: HashMap<String, HashSet<String>>,
}

impl Manager {
    pub fn new() -> Self {
        todo!()
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
