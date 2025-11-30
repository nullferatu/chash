
# PA#2 – Concurrent Hash Table (Rust Implementation)

**Course:** COP-4600 — Operating Systems  
**Student:** Jonathan Denker  
**Due Date:** Fri Nov 21, 2025  

---

## Overview

This project implements a **thread-safe concurrent hash table** in Rust for Programming Assignment #2.  
The design emphasizes safe concurrency, deterministic behavior, and a clean modular structure, using Rust’s strict memory-safety guarantees.

Key characteristics:

- **Concurrency Safety:** Implemented using `std::sync::RwLock` to allow multiple readers and exclusive writers.
- **Hash Function:** Uses the **Jenkins One-at-a-Time** hash function as required by the assignment.
- **Deterministic Thread Behavior:** Threads perform lookups, insertions, and deletions with no data races.
- **Rust Memory Safety:** Ownership, borrowing, and `Arc` prevent undefined behavior or manual memory issues.

This implementation fulfills the **Extra Credit** requirement through a full Rust-based solution.

---

## Compilation & Execution

### Prerequisites
- Rust toolchain (`cargo`, `rustc`)
- GNU Make (optional, if using Makefile workflow)

---

## Method 1: Make (Recommended)

**Build** 
`make`

**Run**
`./chash`

**Clean**
`make clean`

---

## Method 2: Cargo (Native)

**Run (Debug)**
`cargo run`

**Run (Release)**
`cargo run --release`

---

## File Manifest

### Top-Level Files

- **Makefile** — Build automation wrapper.
- **commands.txt** — Hardcoded driver input.
- **RUST_EXPLAINED.md** — (Extra Credit) Documentation for C programmers explaining Rust memory concepts.

### Source Directory (`src/`)

| File | Description |
|------|-------------|
| `main.rs` | Program entry point; thread setup and command parsing. |
| `hash_table.rs` | Core concurrent hash table implementation using RwLock. |
| `logger.rs` | Thread-safe logging system used by concurrent operations. |
| `common.rs` | Shared helpers, types, and Jenkins One-at-a-Time hash function. |

---

## AI Use Citation

In accordance with UCF Academic Integrity policies, AI assistance was used in limited, allowed capacities:

- **Refactoring:** Helped reorganize an early prototype into cleaner modules (`hash_table.rs`, `logger.rs`).
- **API Consultation:** Clarified RwLock semantics and best practices for handling writer “Waiting” logging.
- **Documentation Support:** Assisted in drafting sections of `RUST_EXPLAINED.md` to explain Rust concepts (Arc, borrowing) from a C programmer’s perspective.

All code was **manually reviewed, tested, and validated** to comply with assignment requirements.

---