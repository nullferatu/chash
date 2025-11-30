// src/hash_table.rs
use std::sync::{Arc, RwLock};
use crate::common::{Record, jenkins_one_at_a_time};
use crate::logger::Logger;

#[derive(Debug)]
struct Node {
    rec: Record,
    next: Option<Box<Node>>,
}

#[derive(Default, Debug)]
struct HashList {
    head: Option<Box<Node>>,
}

impl HashList {
    fn find_mut(&mut self, hash: u32) -> Option<&mut Record> {
        let mut cur = self.head.as_mut();
        while let Some(n) = cur {
            if n.rec.hash == hash {
                return Some(&mut n.rec);
            }
            cur = n.next.as_mut();
        }
        None
    }

    // Returns true if inserted, false if duplicate
    fn insert(&mut self, name: String, salary: u32, hash: u32) -> bool {
        if self.find_mut(hash).is_some() {
            return false; // Duplicate
        }
        let new_node = Box::new(Node {
            rec: Record { hash, name, salary },
            next: self.head.take(),
        });
        self.head = Some(new_node);
        true
    }

    fn delete(&mut self, hash: u32) -> bool {
        if let Some(mut head) = self.head.take() {
            if head.rec.hash == hash {
                self.head = head.next.take();
                return true;
            } else {
                let mut prev = &mut head;
                while let Some(mut next) = prev.next.take() {
                    if next.rec.hash == hash {
                        prev.next = next.next.take();
                        // Reattach the rest of the list
                        self.head = Some(head); 
                        return true;
                    }
                    prev.next = Some(next);
                    prev = prev.next.as_mut().unwrap();
                }
                self.head = Some(head);
            }
        }
        false
    }

    fn search(&self, hash: u32) -> Option<Record> {
        let mut cur = self.head.as_deref();
        while let Some(n) = cur {
            if n.rec.hash == hash {
                return Some(n.rec.clone());
            }
            cur = n.next.as_deref();
        }
        None
    }

    fn to_vec(&self) -> Vec<Record> {
        let mut out = Vec::new();
        let mut cur = self.head.as_deref();
        while let Some(n) = cur {
            out.push(n.rec.clone());
            cur = n.next.as_deref();
        }
        out
    }
}

pub struct ConcurrentHash {
    inner: Arc<RwLock<HashList>>,
    logger: Arc<Logger>,
}

impl ConcurrentHash {
    pub fn new(logger: Arc<Logger>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashList::default())),
            logger,
        }
    }

    // Helper to log locking mechanics
    fn log_action(&self, thread_id: u32, lock_type: &str, action: &str) {
        self.logger.log(format!("THREAD {} {} LOCK {}", thread_id, lock_type, action));
    }
    
    fn log_wait(&self, thread_id: u32) {
        self.logger.log(format!("THREAD {} WAITING FOR MY TURN", thread_id));
    }

    pub fn insert(&self, thread_id: u32, name: String, salary: u32) {
        self.logger.log(format!("THREAD {},INSERT,{},{}", thread_id, name, salary));
        let hash = jenkins_one_at_a_time(&name);

        self.log_wait(thread_id);
        // Scoping the lock guard
        {
            self.log_action(thread_id, "WRITE", "ACQUIRED"); // Technically we log this AFTER acquiring
            let mut list = self.inner.write().unwrap();
            
            if list.insert(name.clone(), salary, hash) {
                println!("Inserted {},{},{}", hash, name, salary);
            } else {
                println!("Insert failed. Entry {} is a duplicate.", hash);
            }
        } // Drop lock
        self.log_action(thread_id, "WRITE", "RELEASED");
    }

    pub fn delete(&self, thread_id: u32, name: String) {
        self.logger.log(format!("THREAD {},DELETE,{}", thread_id, name));
        let hash = jenkins_one_at_a_time(&name);

        self.log_wait(thread_id);
        {
            self.log_action(thread_id, "WRITE", "ACQUIRED");
            let mut list = self.inner.write().unwrap();
            if list.delete(hash) {
                println!("Deleted record for {},{},... (values not stored in delete cmd)", hash, name);
            } else {
                println!("Entry {} not deleted. Not in database.", hash);
            }
        }
        self.log_action(thread_id, "WRITE", "RELEASED");
    }

    pub fn update_salary(&self, thread_id: u32, name: String, salary: u32) {
        self.logger.log(format!("THREAD {},UPDATE,{},{}", thread_id, name, salary));
        let hash = jenkins_one_at_a_time(&name);

        self.log_wait(thread_id);
        {
            self.log_action(thread_id, "WRITE", "ACQUIRED");
            let mut list = self.inner.write().unwrap();
            
            if let Some(rec) = list.find_mut(hash) {
                println!("Updated record {} from {},{},{} to {},{},{}", 
                    rec.hash, rec.hash, rec.name, rec.salary, 
                    rec.hash, rec.name, salary);
                rec.salary = salary;
            } else {
                println!("Update failed. Entry {} not found.", hash);
            }
        }
        self.log_action(thread_id, "WRITE", "RELEASED");
    }

    pub fn search(&self, thread_id: u32, name: String) {
        self.logger.log(format!("THREAD {},SEARCH,{}", thread_id, name));
        let hash = jenkins_one_at_a_time(&name);

        self.log_wait(thread_id);
        {
            self.log_action(thread_id, "READ", "ACQUIRED");
            let list = self.inner.read().unwrap();
            match list.search(hash) {
                Some(r) => println!("Found: {},{},{}", r.hash, r.name, r.salary),
                None => println!("{} not found.", name),
            }
        }
        self.log_action(thread_id, "READ", "RELEASED");
    }

    pub fn print(&self, thread_id: u32) {
        self.logger.log(format!("THREAD {},PRINT", thread_id));

        self.log_wait(thread_id);
        {
            self.log_action(thread_id, "READ", "ACQUIRED");
            let list = self.inner.read().unwrap();
            let mut v = list.to_vec();
            v.sort_by_key(|r| r.hash);
            
            println!("Current Database:");
            for r in v {
                println!("{},{},{}", r.hash, r.name, r.salary);
            }
        }
        self.log_action(thread_id, "READ", "RELEASED");
    }

    // For the final dump 
    pub fn final_print(&self) {
        let list = self.inner.read().unwrap();
        let mut v = list.to_vec();
        v.sort_by_key(|r| r.hash);
        
        println!("Final Database State:"); 
        for r in v {
            println!("{},{},{}", r.hash, r.name, r.salary);
        }
    }
}