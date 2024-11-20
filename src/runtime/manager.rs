use crate::{
    frontend::typecheck::Type,
    runtime::{
        eval_expr,
        lock::{Lock, LockKind},
        message::{self, Message, Val},
        transaction::{Txn, TxnId, WriteToName},
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
    pub most_recently_applied_txn: Option<Txn>,
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
            most_recently_applied_txn: None,
        }
    }

    pub async fn handle_transaction(&mut self, txn: &Txn) {
        let mut names_read_by_txn = HashSet::new();
        let mut names_written_by_txn = HashSet::new();
        for w2n in txn.writes.iter() {
            let ex = w2n.expr.clone();
            names_read_by_txn.extend(ex.names_contained().into_iter());
            names_written_by_txn.insert(w2n.name.clone());
        }
        loop {
            let mut temp_val_env: HashMap<String, Val> = HashMap::new();
            let mut read_abort = false;
            let mut write_abort = false;
            let mut this_txn_write_requires = HashSet::new();
            for nm in names_read_by_txn.iter() {
                let var_or_def = self.worker_kind_env.get(nm).unwrap();
                if *var_or_def == WorkerKind::Var {
                    let opt_val = self
                        .read_single_var(nm, txn, &mut this_txn_write_requires)
                        .await;
                    if opt_val == None {
                        read_abort = true;
                        break;
                    } else {
                        let val_of_nm = opt_val.unwrap();
                        temp_val_env.insert(nm.clone(), val_of_nm);
                    }
                } else {
                    todo!()
                }
            }
            if read_abort {
                for nm in names_read_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                continue;
            }
            for w2n in txn.writes.iter() {
                let var_or_def = self.worker_kind_env.get(&w2n.name).unwrap();
                assert_eq!(*var_or_def, WorkerKind::Var);
                let write_success = self.write_single_var(w2n, &temp_val_env, txn).await;
                if !write_success {
                    write_abort = true;
                    break;
                }
            }
            if write_abort {
                for nm in names_read_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                for nm in names_written_by_txn.iter() {
                    let sender_to_this_nm = self.senders_to_workers.get(nm).unwrap().clone();
                    let _ = sender_to_this_nm
                        .send(Message::VarLockAbort { txn: txn.clone() })
                        .await
                        .unwrap();
                }
                continue;
            }
            self.release_var_locks(txn, &this_txn_write_requires).await;
            break;
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
    async fn read_single_var(
        &mut self,
        var_name: &str,
        txn: &Txn,
        this_txn_write_requires: &mut HashSet<Txn>,
    ) -> Option<Val> {
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
                                let read_last_txn =
                                    result_preds.into_iter().max_by(|x, y| x.id.cmp(&y.id));
                                if read_last_txn != None {
                                    this_txn_write_requires.insert(read_last_txn.unwrap());
                                }
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

    // return true if write successful, return false if write lock abort occurs
    async fn write_single_var(
        &mut self,
        write_to_var: &WriteToName,
        val_env: &HashMap<String, Val>,
        txn: &Txn,
    ) -> bool {
        let mut opt_val_env = HashMap::new();
        for (nm, v) in val_env.iter() {
            opt_val_env.insert(nm.clone(), Some(v.clone()));
        }
        let write_val = eval_expr::evaluate_expr(&write_to_var.expr, &opt_val_env).unwrap();
        let lock_req_msg = Message::VarLockRequest {
            lock_kind: LockKind::Write,
            txn: txn.clone(),
        };
        let sender_to_this_var = self
            .senders_to_workers
            .get(&write_to_var.name)
            .unwrap()
            .clone();
        let _ = sender_to_this_var.send(lock_req_msg).await.unwrap();
        if let Some(grant_msg) = self.receiver_from_workers.recv().await {
            match grant_msg {
                Message::VarLockGranted {
                    txn: resp_txn,
                    from_name,
                } => {
                    assert_eq!(resp_txn.id, txn.id);
                    assert_eq!(from_name, write_to_var.name);
                    let txn_lock_ref = self.txn_locks_map.get_mut(&txn.id).unwrap();
                    txn_lock_ref.insert(LockWorkerInfo {
                        lock: Lock {
                            lock_kind: LockKind::Write,
                            txn: txn.clone(),
                        },
                        worker_name: from_name,
                    });
                    let write_msg = Message::UsrWriteVarRequest {
                        txn: txn.clone(),
                        write_val: write_val,
                    };
                    let _ = sender_to_this_var.send(write_msg).await.unwrap();
                }
                Message::VarLockAbort { txn: resp_txn } => {
                    assert_eq!(resp_txn.id, txn.id);
                    return false;
                }
                _ => panic!(),
            }
        }
        true
    }

    async fn release_var_locks(&mut self, txn: &Txn, this_txn_write_requires: &HashSet<Txn>) {
        let lwis_ref = self.txn_locks_map.get(&txn.id).unwrap();
        for lwi in lwis_ref.iter() {
            let sender_to_this_worker = self.senders_to_workers.get(&lwi.worker_name).unwrap();
            let var_lock_release_msg = Message::VarLockRelease {
                txn: txn.clone(),
                requires: this_txn_write_requires.clone(),
            };
            let _ = sender_to_this_worker
                .send(var_lock_release_msg)
                .await
                .unwrap();
        }
    }

    // Do we really need the instruction `close(txn_id)`?
    // Yes! Because need to remember {txn |-> lock_info set}
}
