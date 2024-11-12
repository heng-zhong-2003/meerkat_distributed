use crate::{
    frontend::typecheck::Type,
    runtime::{
        lock::{Lock, LockKind},
        message::{self, Message, Val},
        transaction::{Txn, TxnId},
    },
};

use inline_colorization::*;
use std::collections::{HashMap, HashSet};
use tokio::sync::mpsc::{self, Receiver, Sender};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WorkerKind {
    Var,
    Def,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LockWorkerInfo {
    pub lock: Lock,
    pub worker_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
        let mut names_read_by_txn = HashSet::new();
        for w2n in txn.writes.iter() {
            let ex = w2n.expr.clone();
            names_read_by_txn.extend(ex.names_contained().into_iter());
        }
    }

    // locks stoed into self.txn_locks_map
    // return true if all granted, false if any abort occurs
    async fn require_read_locks(&mut self, var_names: &HashSet<String>, txn: &Txn) -> bool {
        let mut lock_cnt = 0;
        let mut locks_granted: HashSet<LockWorkerInfo> = HashSet::new();
        for var_nm in var_names.iter() {
            let sender_to_this_var = self.senders_to_workers.get(var_nm).unwrap().clone();
            let lock_req_msg = Message::VarLockRequest {
                lock_kind: LockKind::Read,
                txn: txn.clone(),
            };
            let _ = sender_to_this_var.send(lock_req_msg).await.unwrap();
        }
        while lock_cnt < var_names.len() {
            if let Some(grant_msg) = self.receiver_from_workers.recv().await {
                lock_cnt += 1;
                match grant_msg {
                    Message::VarLockGranted {
                        txn: resp_txn,
                        from_name,
                    } => {
                        assert_eq!(
                            resp_txn.id, txn.id,
                            "{color_red}should not receive grant \
msg for other txns, but is this implementation correct?{color_reset}"
                        );
                        locks_granted.insert(LockWorkerInfo {
                            lock: Lock {
                                lock_kind: LockKind::Read,
                                txn: resp_txn,
                            },
                            worker_name: from_name,
                        });
                    }
                    Message::VarLockAbort { txn: resp_txn } => {
                        assert_eq!(
                            resp_txn.id, txn.id,
                            "{color_red}should not receive grant \
msg for other txns, but is this implementation correct?{color_reset}"
                        );
                        println!(
                            "{color_yellow}read lock for txn {:?} aborted{color_reset}",
                            resp_txn.id
                        );
                        return false;
                    }
                    _ => panic!(
                        "{color_red}should not receive non-grant message when \
require read locks, but really?{color_reset}"
                    ),
                }
            }
        }
        true
    }

    async fn read_var(&mut self, var_name: String) -> Option<Val> {
        todo!()
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
