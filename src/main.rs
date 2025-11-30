// Jonathan Denker
// COP-4600 Fall 25
// Programming Assignment 2

// ---------- Dependencies ----------
use std::{
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    thread,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Debug)]
struct Record {
    hash: u32,
    name: String,
    salary: u32,
}

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

  fn find_prev_and_cur(&mut self, hash: u32) -> (Option<*mut Box<Node>>, Option<*mut Box<Node>>) {
      // unsafe only for raw ptr identity; we never deref raw outside of &mut borrow scope
      let mut prev: Option<*mut Box<Node>> = None;
      let mut cur: Option<*mut Box<Node>> = self.head.as_mut().map(|b| b as *mut _);
      while let Some(c) = cur {
          // SAFETY: c points to a live Box<Node> within &mut self scope
          let is_target = unsafe { (*c).rec.hash == hash };
          if is_target {
              return (prev, cur);
          }
          let next_ptr = unsafe { (*c).next.as_mut().map(|b| b as *mut _) };
          prev = cur;
          cur = next_ptr;
      }
      (prev, None)
  }

  fn insert_or_update(&mut self, name: String, salary: u32, hash: u32) {
      if let Some(r) = self.find_mut(hash) {
          r.name = name;
          r.salary = salary;
          return;
      }
      // prepend for O(1); order doesn’t matter except on final print
      let new_node = Box::new(Node {
          rec: Record { hash, name, salary },
          next: self.head.take(),
      });
      self.head = Some(new_node);
  }

  fn delete(&mut self, hash: u32) -> bool {
      // special case head
      if let Some(mut head) = self.head.take() {
          if head.rec.hash == hash {
              self.head = head.next.take();
              return true;
          } else {
              let mut prev = &mut head;
              while let Some(mut next) = prev.next.take() {
                  if next.rec.hash == hash {
                      prev.next = next.next.take();
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
      // Start as Option<&Node>
      let mut cur: Option<&Node> = self.head.as_deref();
      while let Some(n) = cur {
          if n.rec.hash == hash {
              return Some(n.rec.clone());
          }
          // Move to the next node, still as Option<&Node>
          cur = n.next.as_deref();
      }
      None
  }

  fn to_vec(&self) -> Vec<Record> {
      let mut out = Vec::new();
      let mut cur: Option<&Node> = self.head.as_deref();
      while let Some(n) = cur {
          out.push(n.rec.clone());
          cur = n.next.as_deref();
      }
      out
  }
}

// ---------- Jenkins one-at-a-time (32-bit) ----------
fn jenkins_one_at_a_time(s: &str) -> u32 {
    let mut hash: u32 = 0;
    for &b in s.as_bytes() {
        hash = hash.wrapping_add(b as u32);
        hash = hash.wrapping_add(hash << 10);
        hash ^= hash >> 6;
    }
    hash = hash.wrapping_add(hash << 3);
    hash ^= hash >> 11;
    hash = hash.wrapping_add(hash << 15);
    hash
}

// ---------- Logging + counters ----------
#[derive(Default)]
struct LockCounters {
    acquire: AtomicUsize,
    release: AtomicUsize,
}

fn ts() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

fn log_line(out: &Arc<RwLock<File>>, line: &str) {
    let mut f = out.write().expect("log write lock poisoned");
    let _ = writeln!(f, "{}", line);
}

fn log_lock(out: &Arc<RwLock<File>>, kind: &str, what: &str, counters: &LockCounters) {
    match what {
        "ACQUIRED" => {
            counters.acquire.fetch_add(1, Ordering::Relaxed);
            log_line(out, &format!("{},{} LOCK ACQUIRED", ts(), kind.to_uppercase()));
        }
        "RELEASED" => {
            counters.release.fetch_add(1, Ordering::Relaxed);
            log_line(out, &format!("{},{} LOCK RELEASED", ts(), kind.to_uppercase()));
        }
        _ => {}
    }
}

// Helpers to acquire RwLocks while logging exactly once each side.
fn with_read<'a, T, R>(
    table: &'a Arc<RwLock<T>>,
    out: &Arc<RwLock<File>>,
    counters: &LockCounters,
    f: impl FnOnce(&T) -> R,
) -> R {
    log_lock(out, "READ", "ACQUIRED", counters);
    let guard = table.read().expect("rwlock read poisoned");
    let r = f(&*guard);
    drop(guard);
    log_lock(out, "READ", "RELEASED", counters);
    r
}

fn with_write<'a, T, R>(
    table: &'a Arc<RwLock<T>>,
    out: &Arc<RwLock<File>>,
    counters: &LockCounters,
    f: impl FnOnce(&mut T) -> R,
) -> R {
    log_lock(out, "WRITE", "ACQUIRED", counters);
    let mut guard = table.write().expect("rwlock write poisoned");
    let r = f(&mut *guard);
    drop(guard);
    log_lock(out, "WRITE", "RELEASED", counters);
    r
}

// ---------- Concurrent Hash “Table” (linked list under a single RW lock) ----------
struct ConcurrentHash {
    // Entire list protected by one RwLock, per spec
    inner: Arc<RwLock<HashList>>,
    // global output writer (also under a lock so threads can write safely)
    out: Arc<RwLock<File>>,
    // counters
    locks: Arc<LockCounters>,
}

impl ConcurrentHash {
    fn new(output_path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(output_path)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(HashList::default())),
            out: Arc::new(RwLock::new(file)),
            locks: Arc::new(LockCounters::default()),
        })
    }

    fn clone_handles(&self) -> (Arc<RwLock<HashList>>, Arc<RwLock<File>>, Arc<LockCounters>) {
        (Arc::clone(&self.inner), Arc::clone(&self.out), Arc::clone(&self.locks))
    }

    fn insert(&self, thread_id: u32, name: &str, salary: u32) {
        log_line(&self.out, &format!("THREAD {}: INSERT,{name},{salary}", thread_id));
        let hash = jenkins_one_at_a_time(name);
        let (inner, out, locks) = self.clone_handles();
        with_write(&inner, &out, &locks, |list| {
            list.insert_or_update(name.to_string(), salary, hash);
        });
    }

    fn delete(&self, thread_id: u32, name: &str) {
        log_line(&self.out, &format!("THREAD {}: DELETE,{name}", thread_id));
        let hash = jenkins_one_at_a_time(name);
        let (inner, out, locks) = self.clone_handles();
        with_write(&inner, &out, &locks, |list| {
            let _ = list.delete(hash);
        });
    }

    fn update_salary(&self, thread_id: u32, name: &str, salary: u32) {
        log_line(&self.out, &format!("THREAD {}: UPDATE_SALARY,{name},{salary}", thread_id));
        let hash = jenkins_one_at_a_time(name);
        let (inner, out, locks) = self.clone_handles();
        with_write(&inner, &out, &locks, |list| {
            if let Some(r) = list.find_mut(hash) {
                r.salary = salary;
            }
        });
    }

    fn search(&self, thread_id: u32, name: &str) {
        log_line(&self.out, &format!("THREAD {}: SEARCH,{name}", thread_id));
        let hash = jenkins_one_at_a_time(name);
        let (inner, out, locks) = self.clone_handles();
        let res = with_read(&inner, &out, &locks, |list| list.search(hash));
        match res {
            Some(r) => log_line(&self.out, &format!("{},{},{}", r.hash, r.name, r.salary)),
            None => log_line(&self.out, "No Record Found"),
        }
    }

    fn print(&self, thread_id: u32) {
        log_line(&self.out, &format!("THREAD {}: PRINT", thread_id));
        let (inner, out, locks) = self.clone_handles();
        let mut v = with_read(&inner, &out, &locks, |list| list.to_vec());
        v.sort_by_key(|r| r.hash);
        for r in v {
            log_line(&out, &format!("{},{},{}", r.hash, r.name, r.salary));
        }
    }

    fn final_print_and_counts(&self) {
        log_line(&self.out, ""); // blank line
        let acq = self.locks.acquire.load(Ordering::Relaxed);
        let rel = self.locks.release.load(Ordering::Relaxed);
        log_line(&self.out, &format!("Number of lock acquisitions: {}", acq));
        log_line(&self.out, &format!("Number of lock releases: {}", rel));
        log_line(&self.out, "Final Table:");
        // final table sorted
        let (inner, out, locks) = self.clone_handles();
        let mut v = with_read(&inner, &out, &locks, |list| list.to_vec());
        v.sort_by_key(|r| r.hash);
        for r in v {
            log_line(&out, &format!("{},{},{}", r.hash, r.name, r.salary));
        }
    }
}

// ---------- Command parsing & driver ----------
#[derive(Debug)]
enum Cmd {
    Insert { name: String, salary: u32, pri: u32 },
    Delete { name: String, pri: u32 },
    Search { name: String, pri: u32 },
    Print { pri: u32 },
    UpdateSalary { name: String, salary: u32, pri: u32 }, // optional extension
}

fn parse_line(s: &str) -> Option<Cmd> {
    // Expect formats:
    // insert,Name With Spaces,40000,1
    // delete,Name With Spaces,8
    // search,Name With Spaces,10
    // print,7
    let mut parts = s.trim().splitn(2, ',');
    let cmd = parts.next()?.trim().to_lowercase();
    let rest = parts.next().unwrap_or("").trim();

    match cmd.as_str() {
        "insert" => {
            let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
            if fields.len() != 3 { return None; }
            let name = fields[0].to_string();
            let salary: u32 = fields[1].parse().ok()?;
            let pri: u32 = fields[2].parse().ok()?;
            Some(Cmd::Insert { name, salary, pri })
        }
        "delete" => {
            let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
            if fields.len() != 2 { return None; }
            let name = fields[0].to_string();
            let pri: u32 = fields[1].parse().ok()?;
            Some(Cmd::Delete { name, pri })
        }
        "search" => {
            let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
            if fields.len() != 2 { return None; }
            let name = fields[0].to_string();
            let pri: u32 = fields[1].parse().ok()?;
            Some(Cmd::Search { name, pri })
        }
        "print" => {
            let pri: u32 = rest.parse().ok()?;
            Some(Cmd::Print { pri })
        }
        // allow “updatesalary,name,val,pri” as a convenience
        "updatesalary" => {
            let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
            if fields.len() != 3 { return None; }
            let name = fields[0].to_string();
            let salary: u32 = fields[1].parse().ok()?;
            let pri: u32 = fields[2].parse().ok()?;
            Some(Cmd::UpdateSalary { name, salary, pri })
        }
        _ => None,
    }
}

fn main() -> std::io::Result<()> {
    // Hard-coded file names per spec
    let commands_path = "commands.txt";
    let output_path = "output.txt";

    // Build table
    let table = Arc::new(ConcurrentHash::new(output_path)?);

    // Read commands
    let file = File::open(commands_path)?;
    let reader = BufReader::new(file);
    let mut cmds = Vec::new();
    for line in reader.lines() {
        let l = line?;
        if l.trim().is_empty() { continue; }
        if let Some(c) = parse_line(&l) {
            cmds.push(c);
        }
    }

    // Spawn a thread per command (simple concurrency model)
    // We pass each command’s “priority” as the printed THREAD id to match examples.
    let mut handles = Vec::new();
    for cmd in cmds {
        let t = Arc::clone(&table);
        let handle = thread::spawn(move || {
            match cmd {
                Cmd::Insert { name, salary, pri } => t.insert(pri, &name, salary),
                Cmd::Delete { name, pri } => t.delete(pri, &name),
                Cmd::Search { name, pri } => t.search(pri, &name),
                Cmd::Print { pri } => t.print(pri),
                Cmd::UpdateSalary { name, salary, pri } => t.update_salary(pri, &name, salary),
            }
        });
        handles.push(handle);
    }

    for h in handles {
        let _ = h.join();
    }

    // Final summary & table
    table.final_print_and_counts();

    Ok(())
}
