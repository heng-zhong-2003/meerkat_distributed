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
    // return true if all granted, return false immediately if any abort occurs
    /*     async fn read_vars(
            &mut self,
            var_names: &HashSet<String>,
            temp_val_env: &mut HashMap<String, Val>,
            txn: &Txn,
        ) -> bool {
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
            // sequentially receiving responses should be OK because manager
            // has only one (sequential) thread, cannot pass onto another transaction
            // if one has not yet finished.
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
                                worker_name: from_name.clone(),
                            });
                            let var_value = self.read_single_var(from_name.clone()).await.unwrap();
                            temp_val_env.insert(from_name, var_value);
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
                            temp_val_env.clear();
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
        } */

    // request lock, read and return. return None if LOCK ABORT.
    async fn read_single_var(&mut self, var_name: &str, txn: &Txn) -> Option<Val> {
        let sender_to_this_var = self.senders_to_workers.get(var_name).unwrap().clone();
        let lock_req_msg = Message::VarLockRequest {
            lock_kind: LockKind::Read,
            txn: txn.clone(),
        };
        let _ = sender_to_this_var.send(lock_req_msg).await.unwrap();
        if let Some(grant_msg) = self.receiver_from_workers.recv().await {
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
                    let txn_lock_ref = self.txn_locks_map.get_mut(&txn.id).unwrap();
                    txn_lock_ref.insert(LockWorkerInfo {
                        lock: Lock {
                            lock_kind: LockKind::Read,
                            txn: txn.clone(),
                        },
                        worker_name: from_name,
                    });
                    let read_req_msg = Message::UsrReadVarRequest { txn: txn.clone() };
                    let _ = sender_to_this_var.send(read_req_msg).await.unwrap();
                    if let Some(read_resp_msg) = self.receiver_from_workers.recv().await {
                        match read_resp_msg {
                            Message::UsrReadVarResult {
                                var_name: rslt_var_name,
                                result,
                                result_preds,
                                txn: read_rslt_txn,
                            } => {
                                assert_eq!(
                                    read_rslt_txn.id, txn.id,
                                    "{color_red}should not receive read rslt \
msg for other txns, but is this implementation correct?{color_reset}"
                                );
                                assert_eq!(rslt_var_name, var_name);
                                return Some(result.unwrap());
                            }
                            _ => panic!(
                                "{color_red}should not receive non read rslt message \
when require read locks, but really?{color_reset}"
                            ),
                        }
                    }
                }
                Message::VarLockAbort { txn: resp_txn } => {
                    assert_eq!(
                        resp_txn.id, txn.id,
                        "{color_red}should not receive abort \
msg for other txns, but is this implementation correct?{color_reset}"
                    );
                    return None;
                }
                _ => panic!(
                    "{color_red}should not receive non-grant message when \
require read locks, but really?{color_reset}"
                ),
            }
        }
        panic!("should not come to here!")
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
