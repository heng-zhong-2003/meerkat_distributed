// defworker 
// read lock 

// append following to existing meerkat project:
/*
    var a = 0;
    var b = 0;
    def f = a + b;
    def g = f;
*/
/*
Service Manager:
- create all worker a, b, f OR require RLocks for {a, b} and WLock for {f}
- for code update, processing line l+1 will be blocked by line l (dependency)
- for newly created 
- gather all typing inforation permitted by RLock, info directly from typing env
  maintained by service manager 
- defworker process Write: 
    - establish all dependencies (graph's structure) by subscribption,
    - gain all dependencies' values (current inputs)
*/

// One problem:
// for a defwork, code update vs pending change message (v, P, R), which apply first
// - idea 1 : ignore all pending change messages 
// - idea 2 : wait for all pending change messages to finish
/*
update {
    var c = 1;
    def f' = a * b * c; // if read(f') >= read(f), then batch validity remains 
    def g = f;
}
*/

/*
update {
    var c = 1;
    def f' = a;         // wait for pending messages to finish, then apply code update
    def g = f;          // since now batch validity changes
}
*/



/*
update {
    var c = 1;
    def f' = a * b * c;  
    def g = f;
}
 |concurrently|
update {
    var c = 1;
    def f' = a;         
    def g = f;          
}
*/
// one WLock rejected 


/*
var c = 1;
def f1 = a; 
def f2 = b;
def g = f1 + f2;

dev1 update {
    def f2 = b + c;
}
 |concurrently|
dev2 update {
    def f1 = a + c; 
}
*/
// dev1:: WLock: {f1}; RLock: {b, c, g}
// dev2:: WLock: {f2}; RLock: {a, c, g}



// grant write lock to developer

// process Write Message from developer
// process write lock

use std::{collections::{HashMap, HashSet}, hash::Hash};

use crate::{
    frontend::meerast::Expr,
    runtime::{
        lock::{Lock, LockKind},
        message::{Message, Val, PropaChange, TxnAndName, _PropaChange},
        transaction::{Txn, TxnId, WriteToName},
        def_batch_utils::{apply_batch, search_batch},
    },
};

use tokio::sync::mpsc::{self, Receiver, Sender};

use inline_colorization::*;

use super::varworker::PendingWrite;


pub struct DefWorker {
    pub name: String,
    pub receiver_from_manager: Receiver<Message>,
    pub sender_to_manager: Sender<Message>,
    pub senders_to_subscribers: HashMap<String, Sender<Message>>,

    pub value: Option<Val>,
    pub pred_txns: Vec<Txn>,
    pub prev_batch_provides: HashSet<Txn>,
    // data structure maintaining all propa_changes to be apply
    pub propa_changes_to_apply: HashMap<TxnAndName, _PropaChange>,

    // for now expr is list of name or values calculating their sum
    pub expr: Vec<Val>,
    // direct dependency and their current value
    pub replica: HashMap<String, Option<Val>>,
    // transtitive dependencies: handled by srvmanager for local dependencies
    // and SubscribeRequest/Grant for global dependencies
    pub transtitive_deps: HashMap<String, HashSet<String>>,
    // var ->->-> input(def)
    // input(def) -> var
    /*
       a   b   c 
        \  |  /
           d 
        a -> [] // or reflexively contain a ? 
        b -> [] // or reflexively contain b ? 
        c -> [] // or reflexively contain c ?
     */
    pub counter: i32,

    pub locks: HashSet<Lock>,
    pub lock_queue: HashSet<Lock>,
    pub pending_writes: HashSet<PendingWrite>,
}

impl DefWorker {
    pub fn new(
        name: &str,
        receiver_from_manager: Receiver<Message>,
        sender_to_manager: mpsc::Sender<Message>,
        expr: Vec<Val>,
        replica: HashMap<String, Option<Val>>, // HashMap { dependent name -> None }
        transtitive_deps: HashMap<String, HashSet<String>>,
    ) -> DefWorker {
        DefWorker {
            name: name.to_string(), 
            receiver_from_manager, 
            sender_to_manager,
            senders_to_subscribers: HashMap::new(),

            value: None,
            pred_txns: Vec::new(),
            prev_batch_provides: HashSet::new(),
            propa_changes_to_apply: HashMap::new(),

            expr,
            replica,
            transtitive_deps,
            counter: 0,

            locks: HashSet::new(),
            lock_queue: HashSet::new(),
            pending_writes: HashSet::new(),
        }
    }

    pub fn next_count(counter_ref: &mut i32) -> i32 {
        *counter_ref += 1;
        *counter_ref
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
            Message::DefLockRequest { lock_kind, txn } => {
                let mut oldest_txn_id = txn.id.clone();
                for lock in self.locks.iter() {
                    if lock.txn.id <= oldest_txn_id {
                        oldest_txn_id = lock.txn.id.clone();
                    }
                }
                if txn.id == oldest_txn_id 
                    || (!self.has_write_lock() && self.pending_writes.is_empty())
                {
                    self.lock_queue.insert(Lock {
                        lock_kind: lock_kind,
                        txn: txn,
                    });
                } else {
                    let _ = self
                        .sender_to_manager
                        .send(Message::DefLockAbort { txn: txn })
                        .await
                        .unwrap();
                }
            }
            Message::DevReadDefRequest { txn } => {
                let mut WLock: Option<Lock> = None;
                for lock in self.locks.iter() {
                    if lock.lock_kind == LockKind::Read && txn.id == lock.txn.id {
                        WLock = Some(lock.clone());
                        break;
                    }
                }
                match WLock {
                    // todo!("should we do");
                    //     // self.pred_txns.push(txn.clone());
                    Some(l) => {
                        let _ = self
                        .sender_to_manager
                        .send(Message::DevReadDefResult {
                            name: self.name.clone(),
                            txn: txn,
                        })
                        .await;
                        self.locks.remove(&l);
                    },
                    None => panic!(),
                }
            }
            Message::DevWriteRequest { txn, write_expr } => {
                todo!()
            }
            
            Message::DefLockRelease { txn } => {
                let to_be_removed: HashSet<Lock> = self
                    .locks
                    .iter()
                    .cloned()
                    .filter(|t| t.txn.id == txn.id)
                    .collect();
                for tbr in to_be_removed.iter() {
                    self.locks.remove(tbr);
                }
            }
            Message::UsrReadDefRequest { txn, requires } => {
                let result_pred = self.pred_txns.clone().into_iter().collect();
                let msg_back = Message::UsrReadDefResult { // TODO(Opt): set smaller than applied_txns should also work ...
                    txn: txn.clone(), 
                    name: self.name.clone(),
                    result: self.value.clone(),   
                    result_pred, // send back pred_txn (applied txns) to manager
                };
                let _ = self.sender_to_manager.send(msg_back).await;
            }
            Message::Propagate { propa_change } => {
                println!("{color_blue}PropaMessage{color_reset}");
                let _propa_change =
                    Self::processed_propachange(&mut self.counter, &propa_change, &mut self.transtitive_deps);

                for txn in &propa_change.preds {
                    println!("{color_blue}insert propa_changes_to_apply{color_reset}");
                    self.propa_changes_to_apply.insert(
                        TxnAndName {
                            txn: txn.clone(),
                            name: propa_change.from_name.clone(),
                        },
                        _propa_change.clone(),
                    );
                }
                println!("after receiving propamsg, the graph is {:#?}", &self.propa_changes_to_apply);
            }

            // for test only
            // Message::ManagerRetrieve => {
            //     let msg = Message::ManagerRetrieveResult {
            //         name: worker.name.clone(),
            //         result: curr_val.clone(),
            //     };
            //     let _ = worker.sender_to_manager.send(msg).await;
            // }
            _ => panic!(),
        }
    }

    pub async fn run_defworker(mut def_worker: DefWorker) {
        while let Some(msg) = def_worker.receiver_from_manager.recv().await {
            println!("{color_red}defworker receive msg {:?}{color_reset}", msg);
            let _ = DefWorker::handle_message(
                &mut def_worker,
                msg,
            )
            .await;

            // search for valid batch
            let valid_batch = search_batch(
                &def_worker.propa_changes_to_apply,
                &def_worker.pred_txns,
            );

            // apply valid batch
            println!("{color_yellow}apply batch called{color_reset}");
            let (all_provides, new_value) = apply_batch(
                valid_batch,
                // &def_worker.worker,
                &mut def_worker.value,
                &mut def_worker.pred_txns,
                &mut def_worker.prev_batch_provides,
                &mut def_worker.propa_changes_to_apply,
                &mut def_worker.replica,
            );

            // for test, ack srvmanager
            // if new_value != None {
            //     let msg = Message::ManagerRetrieveResult {
            //         name: def_worker.worker.name.clone(),
            //         result: new_value.clone(),
            //     };
            //     let _ = def_worker.worker.sender_to_manager.send(msg).await;

            //     // broadcast the update to subscribers
            //     let msg_propa = Message::PropaMessage {
            //         propa_change: PropaChange {
            //             name: def_worker.worker.name.clone(),
            //             new_val: new_value.unwrap(),
            //             provides: all_provides.clone(),
            //             requires: all_requires.clone(),
            //         },
            //     };
            //     for succ in def_worker.worker.senders_to_succs.iter() {
            //         let _ = succ.send(msg_propa.clone()).await;
            //     }
            // }

            // println!(
            //     "{color_red}run def worker, def_worker.value after apply_batch: {:?}{color_reset}",
            //     def_worker.value
            // );
        }
    }

    pub fn processed_propachange(
        counter_ref: &mut i32,
        propa_change: &PropaChange,
        transtitive_deps: &HashMap<String, HashSet<String>>,
        // expect inputs(d) maps to transtitively depending vars
    ) -> _PropaChange {
        println!("def should have inputs: {:?}", transtitive_deps);
        let mut deps: HashSet<TxnAndName> = HashSet::new();

        for txn in propa_change.preds.iter() {
            for write in txn.writes.iter() {
                let var_name = write.name.clone();
                println!("def name: {:?}", var_name);

                let mut inputs: Vec<String> = Vec::new();
                for (i, dep_vars) in transtitive_deps.iter() {
                    match dep_vars.get(&var_name) {
                        Some(_) => {
                            println!("def add input: {:?}", i);
                            inputs.push(i.clone());
                        }
                        None => {}
                    }
                }
                println!("def has inputs: {:?}", inputs);

                for i_name in inputs.iter() {
                    let txn_name = TxnAndName {
                        txn: txn.clone(),
                        name: i_name.clone(),
                    };
                    deps.insert(txn_name);
                }
            }
        }

        // Not fully sure about below:

        // for txn in propa_change.requires.iter() {
        //     for write in txn.writes.iter() {
        //         let var_name = write.name.clone();

        //         let mut inputs: Vec<String> = Vec::new();
        //         for (i, dep_vars) in transtitive_deps.iter() {
        //             match dep_vars.get(&var_name) {
        //                 Some(_) => {
        //                     inputs.push(i.clone());
        //                 }
        //                 None => {}
        //             }
        //         }

        //         for i_name in inputs.iter() {
        //             let txn_name = TxnAndName {
        //                 txn: txn.clone(),
        //                 name: i_name.clone(),
        //             };
        //             deps.insert(txn_name);
        //         }
        //     }
        // }

        _PropaChange {
            propa_id: Self::next_count(counter_ref),
            propa_change: propa_change.clone(),
            deps,
        }
    }
}
