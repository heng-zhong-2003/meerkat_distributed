use crate::{
    frontend::typecheck::Type,
    runtime::{
        lock::Lock,
        message::{self, Message, Val},
        transaction::{Txn, TxnId},
    },
};

use inline_colorization::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{self, Receiver, Sender};

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
    // { txn_id |-> { locks } }. What locks have this txn gotten
    pub txn_locks_map: HashMap<TxnId, HashSet<LockWorkerInfo>>,
    // { name |-> { subscribers } }
    pub dependency_graph: HashMap<String, HashSet<String>>,
}

impl Manager {
    pub fn new() -> Self {
        let (sndr, rcvr) = mpsc::channel(message::BUFFER_SIZE);
        Manager {
            sender_to_manager: sndr,
            receiver_from_workers: rcvr,
            senders_to_workers: HashMap::new(),
            typing_env: HashMap::new(),
            worker_kind_env: HashMap::new(),
            txn_locks_map: HashMap::new(),
            dependency_graph: HashMap::new(),
        }
    }

    pub fn handle_transaction(&mut self, txn: &Txn) {
        todo!()
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
