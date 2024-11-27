use std::collections::{HashMap, HashSet};

use crate::{
    frontend::meerast::Expr,
    runtime::{
        lock::{Lock, LockKind},
        message::{Message, PropaChange, Val},
        transaction::{Txn, TxnId, WriteToName},
    },
};
use tokio::sync::mpsc::{self, Receiver, Sender};

use inline_colorization::*;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PendingWrite {
    pub w_lock: Lock,
    pub value: Val,
}

pub struct VarWorker {
    pub name: String,
    pub receiver_from_manager: Receiver<Message>,
    pub sender_to_manager: Sender<Message>,
    pub senders_to_subscribers: HashMap<String, Sender<Message>>,
    // Can abstract all four into Worker, see hig-demo repo
    pub value: Option<Val>,
    pub next_value: Option<Val>,

    pub locks: HashSet<Lock>,
    pub lock_queue: HashSet<Lock>,
    // pub pending_writes: HashSet<PendingWrite>,

    // supplement page 2 top:
    // R is required to be applied before P:
    // For state var f:
    //      - R contains the last update’s P set, that is <oldT, oldW> i.e.
    //      the previous transaction that wrote to this variable
    //      - R also contains { v.lastWrite | v in reads(t) } where reads(t)
    //      be the set of variables (state vars?) read by t
    // R = latest_write_txn + what is computed by manager
    // so is `pred_txns` redundant?
    pub latest_write_txn: Option<Txn>,
    pub pred_txns: HashSet<Txn>,     // applied transactions
    pub next_pred_set: HashSet<Txn>, // pred set send to subscribers
}

impl VarWorker {
    pub fn new(
        name: &str,
        receiver_from_manager: Receiver<Message>,
        sender_to_manager: Sender<Message>,
    ) -> Self {
        VarWorker {
            name: name.to_string(),
            receiver_from_manager,
            sender_to_manager,
            senders_to_subscribers: HashMap::new(),
            value: None,
            next_value: None,
            locks: HashSet::new(),
            lock_queue: HashSet::new(),
            // pending_writes: HashSet::new(),
            latest_write_txn: None,
            pred_txns: HashSet::new(),
            next_pred_set: HashSet::new(),
        }
    }

    pub fn has_write_lock(&self) -> bool {
        for lk in self.locks.iter() {
            if lk.lock_kind == LockKind::Write {
                return true;
            }
        }
        false
    }

    pub async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::VarLockRequest { lock_kind, txn } => {
                let this_lock_held = self.locks.contains(&Lock {
                    lock_kind: lock_kind.clone(),
                    txn: txn.clone(),
                });
                let older_lock_queued = self.lock_queue.iter().any(|x| x.txn.id < txn.id);
                let one_is_write = lock_kind == LockKind::Write
                    || self
                        .lock_queue
                        .iter()
                        .any(|x| x.txn.id < txn.id && x.lock_kind == LockKind::Write);
                if this_lock_held && older_lock_queued && one_is_write {
                    let abort_msg = Message::VarLockAbort { txn: txn };
                    self.sender_to_manager.send(abort_msg).await.unwrap();
                } else {
                    self.lock_queue.insert(Lock {
                        lock_kind: lock_kind,
                        txn: txn,
                    });
                }
            }
            /* Heng Zhong 9:45 PM
            Hi Julia, how should the new variable semantics act with respect to the requires set
            in the VarLockRelease { txn: Txn, requires: Txn set } message?

            Julia Freeman 9:47 PM
            since the var keeps track of all previously applied txns (P in the semantics), it should
            union this requires set with its set of previously applied txns and store it back into P

            Heng Zhong 9:54 PM
            Thanks. Is there any case that the var should temporarily hold on the update and wait
            for some transactions to come first in the new design?

            Julia Freeman 9:57 PM
            good question. thankfully no, vars do not ever have to wait for transactions to come in
            based on provides / requires sets */
            Message::VarLockRelease { txn, requires } => {
                let to_be_removed: HashSet<Lock> = self
                    .locks
                    .iter()
                    .cloned()
                    .filter(|t| t.txn.id == txn.id)
                    .collect();
                self.pred_txns.extend(requires.into_iter());
                self.value = self.next_value.clone();
                for tbr in to_be_removed.iter() {
                    self.locks.remove(tbr);
                }
            }
            Message::VarLockAbort { txn } => {
                todo!()
            }
            Message::Subscribe {
                subscribe_who,
                subscriber_name,
                sender_to_subscriber,
            } => {
                self.senders_to_subscribers
                    .insert(subscriber_name, sender_to_subscriber.clone());
                let respond_msg = Message::SubscriptionGranted {
                    name: self.name.clone(),
                    value: self.value.clone(),
                    provides: {
                        let mut pvd = HashSet::new();
                        match self.latest_write_txn {
                            Some(ref t) => {
                                pvd.insert(t.clone());
                            }
                            None => {}
                        }
                        pvd
                    },
                    trans_dep_set: HashSet::from_iter(vec![self.name.clone()]),
                };
                let _ = sender_to_subscriber.send(respond_msg).await.unwrap();
            }
            Message::UsrReadVarRequest { txn } => {
                let mut self_r_locks_held_by_txn: HashSet<Lock> = HashSet::new();
                for rl in self.locks.iter() {
                    if rl.lock_kind == LockKind::Read {
                        if txn.id == rl.txn.id {
                            self_r_locks_held_by_txn.insert(rl.clone());
                        }
                    }
                }
                // now txn's read has been applied
                self.pred_txns.insert(txn.clone());

                let _ = self
                    .sender_to_manager
                    .send(Message::UsrReadVarResult {
                        var_name: self.name.clone(),
                        result: self.value.clone(),
                        result_preds: self.pred_txns.clone(),
                        txn: txn,
                    })
                    .await;
                for rl in self_r_locks_held_by_txn.into_iter() {
                    self.locks.remove(&rl);
                }
            }
            /* Initially, and when there is no current write txn, current_value is the
            same as next_value. But when there is a write lock held, and the write
            lock has provided a new value, next_value is where that new value goes.
            Then, when the write txn is released, we assign current_value = next_value */
            Message::UsrWriteVarRequest { txn, write_val } => {
                assert!(self
                    .locks
                    .iter()
                    .any(|x| x.lock_kind == LockKind::Write && x.txn.id == txn.id));
                self.next_value = Some(write_val);
            }
            #[allow(unreachable_patterns)]
            _ => panic!(),
        }
    }

    pub async fn tick(&mut self) {
        if self.lock_queue.is_empty() {
            return;
        }
        let oldest_lock = self
            .lock_queue
            .iter()
            .min_by(|x, y| x.txn.id.cmp(&y.txn.id))
            .unwrap()
            .clone();
        self.lock_queue.remove(&oldest_lock);
        self.locks.insert(oldest_lock.clone());
        let lock_granted_msg = Message::VarLockGranted {
            txn: oldest_lock.txn,
            from_name: self.name.clone(),
        };
        self.sender_to_manager.send(lock_granted_msg).await.unwrap();
    }

    pub async fn run_varworker(mut self) {
        while let Some(msg) = self.receiver_from_manager.recv().await {
            let _ = self.handle_message(msg).await;
            let _ = self.tick().await;
        }
    }
}

#[tokio::test]
async fn write_then_read() {
    let (sndr_to_worker, rcvr_from_manager) = mpsc::channel(1024);
    let (sndr_to_manager, mut rcvr_from_worker) = mpsc::channel(1024);
    let worker = VarWorker::new("a", rcvr_from_manager, sndr_to_manager.clone());
    tokio::spawn(worker.run_varworker());
    let write_txn = Txn {
        id: TxnId::new(),
        writes: vec![WriteToName {
            name: "a".to_string(),
            expr: Expr::IntConst { val: 5 },
        }],
    };
    let w_lock_msg = Message::VarLockRequest {
        lock_kind: LockKind::Write,
        txn: write_txn.clone(),
    };
    let _ = sndr_to_worker.send(w_lock_msg).await.unwrap();
    if let Some(msg) = rcvr_from_worker.recv().await {
        match msg {
            Message::VarLockGranted { txn, from_name: _ } => {
                println!(
                    "{color_green}var write lock granted for txn {:?}{color_reset}",
                    txn.id
                );
            }
            _ => panic!(),
        }
    }
    let write_msg = Message::UsrWriteVarRequest {
        txn: write_txn.clone(),
        write_val: Val::Int(5),
    };
    let _ = sndr_to_worker.send(write_msg).await.unwrap();
    let lock_release_msg = Message::VarLockRelease {
        txn: write_txn.clone(),
        requires: HashSet::new(),
    };
    sndr_to_worker.send(lock_release_msg).await.unwrap();

    let read_txn = Txn {
        id: TxnId::new(),
        writes: vec![],
    };
    let r_lock_msg = Message::VarLockRequest {
        lock_kind: LockKind::Read,
        txn: read_txn.clone(),
    };
    let _ = sndr_to_worker.send(r_lock_msg.clone()).await.unwrap();
    if let Some(msg) = rcvr_from_worker.recv().await {
        match msg {
            Message::VarLockGranted { txn, from_name: _ } => {
                println!("var read lock granted {:?}", txn);
            }
            _ => panic!("should not receive msg {:?}", msg),
        }
    }
    let read_msg = Message::UsrReadVarRequest {
        txn: read_txn.clone(),
    };
    let _ = sndr_to_worker.send(read_msg).await.unwrap();
    if let Some(msg) = rcvr_from_worker.recv().await {
        match msg {
            Message::UsrReadVarResult {
                var_name,
                result,
                result_preds,
                txn,
            } => {
                println!(
                    "{color_green}manager receive ReadVarResult: \
var_name={:?}, result={:?}, result_provides={:?}{color_reset}",
                    var_name,
                    result,
                    result_preds
                        .iter()
                        .cloned()
                        .map(|x| x.id)
                        .collect::<Vec<_>>()
                );
                assert_eq!(Some(Val::Int(5)), result);
            }
            _ => panic!(),
        }
    }
}

#[tokio::test]
async fn batch_writes_to_a_and_b_mutually_independent() {
    // creat var workers a and b
    // write Val::Int(3) to a, Val::Int(5) to b
    todo!()
}

#[tokio::test]
async fn try_reading_after_granted_write_lock() {
    // create var worker a
    // request write lock for txn 1, receive lock granted message
    // then send read lock request for a younger txn 2
    // should get lock abort message!
}
