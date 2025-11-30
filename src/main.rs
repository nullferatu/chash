// src/main.rs
mod common;
mod hash_table;
mod logger;

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::sync::Arc;
use std::thread;
use hash_table::ConcurrentHash;
use logger::Logger;

#[derive(Debug)]
enum Cmd {
  Insert { name: String, salary: u32, pri: u32 },
  Delete { name: String, pri: u32 },
  Search { name: String, pri: u32 },
  Print { pri: u32 },
  Update { name: String, salary: u32, pri: u32 },
}

fn parse_line(s: &str) -> Option<Cmd> {
  let mut parts = s.trim().splitn(2, ',');
  let cmd = parts.next()?.trim().to_lowercase();
  let rest = parts.next().unwrap_or("").trim();

  match cmd.as_str() {
    "insert" => {
      let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
      if fields.len() != 3 { return None; }
      Some(Cmd::Insert { 
          name: fields[0].to_string(), 
          salary: fields[1].parse().ok()?, 
          pri: fields[2].parse().ok()? 
      })
    }
    "delete" => {
      let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
      if fields.len() != 2 { return None; }
      Some(Cmd::Delete { 
          name: fields[0].to_string(), 
          pri: fields[1].parse().ok()? 
      })
    }
    "search" => {
      let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
      if fields.len() != 2 { return None; }
      Some(Cmd::Search { 
          name: fields[0].to_string(), 
          pri: fields[1].parse().ok()? 
      })
    }
    "print" => {
      Some(Cmd::Print { pri: rest.parse().ok()? })
    }
    "update" => {
      let fields: Vec<&str> = rest.split(',').map(|x| x.trim()).collect();
      if fields.len() != 3 { return None; }
      Some(Cmd::Update { 
          name: fields[0].to_string(), 
          salary: fields[1].parse().ok()?, 
          pri: fields[2].parse().ok()? 
      })
    }
    _ => None,
  }
}

fn main() -> std::io::Result<()> {
  let commands_path = "commands.txt";
  let log_path = "hash.log";

  let logger = Arc::new(Logger::new(log_path)?);
  let table = Arc::new(ConcurrentHash::new(logger));

  let file = File::open(commands_path)?;
  let reader = BufReader::new(file);

  let mut handles = Vec::new();

  for line in reader.lines() {
    let l = line?;
    if l.trim().is_empty() { continue; }
    
    if let Some(cmd) = parse_line(&l) {
      let t = Arc::clone(&table);
      let handle = thread::spawn(move || {
        match cmd {
          Cmd::Insert { name, salary, pri } => t.insert(pri, name, salary),
          Cmd::Delete { name, pri } => t.delete(pri, name),
          Cmd::Search { name, pri } => t.search(pri, name),
          Cmd::Print { pri } => t.print(pri),
          Cmd::Update { name, salary, pri } => t.update_salary(pri, name, salary),
        }
      });
      handles.push(handle);
    }
  }

  for h in handles {
    let _ = h.join();
  }

  // Required final dump
  table.final_print();

  Ok(())
}