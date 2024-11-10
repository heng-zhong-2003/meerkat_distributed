use crate::{
    frontend::typecheck::Type,
    runtime::{
        lock::LockKind,
        message::{Message, Val},
        transaction::{Txn, TxnId},
    },
};

use std::collections::{HashMap, HashSet};

use tokio::sync::mpsc::{Receiver, Sender};

pub enum WorkerKind {
    Var,
    Def,
}

pub struct Manager {
    // cloned and given to new workers when creating them
    pub sender_to_manager: Sender<Message>,
    pub receiver_from_workers: Receiver<Message>,
    pub senders_to_workers: HashMap<String, Sender<Message>>,
    pub typing_env: HashMap<String, Type>,
    pub worker_kind_env: HashMap<String, WorkerKind>,
    // [name |-> subscribers]
    pub dependency_graph: HashMap<String, HashSet<String>>,
}

impl Manager {
    pub fn new() -> Self {
        todo!()
    }

    pub async fn instr_open_txn(&mut self, reads: HashSet<String>, writes: HashSet<String>) -> Txn {
        todo!()
    }

    pub async fn instr_read(&mut self, worker_name: String, txn: Txn) -> Val {
        todo!()
    }

    async fn read_var(&mut self, worker_name: String, txn: &Txn) -> Val {
        let req_r_lock_msg = Message::VarLockRequest {
            lock_kind: LockKind::Read,
            txn: txn.clone(),
        };
        todo!()
    }

    pub async fn instr_write(&mut self, worker_name: String, new_val: Val, txn: Txn) {
        todo!()
    }

    // Do we really need the instruction `close(txn_id)`?
}
