# Rust Concurrency Explained for C Programmers

## Overview
In C, concurrency requires manually coordinating `malloc/free`, unlocking mutexes correctly, and avoiding data races through discipline. Rust prevents these issues at compile time using ownership, borrowing, and built-in thread-safe wrappers.

---

## 1. Memory Safety Without Manual malloc/free

Box<T>
- Like malloc but automatically freed when it goes out of scope.
- No double frees or dangling pointers.

Option<T>
- Rust’s replacement for NULL pointer logic.
- Forces the programmer to handle Some(value) or None.
- Prevents null dereference crashes entirely.

---

## 2. Thread-Safe Shared Data

Arc<T>
- Atomically reference-counted shared pointer.
- Allows threads to share data safely.
- Data is freed only when the last reference is gone.

RwLock<T>
- Type-safe equivalent to pthread_rwlock_t.
- read(): multiple readers allowed.
- write(): exclusive writer.
- The data is *inside* the lock, so you cannot access it without locking.

Mutex<T>
- Used for the logger.
- Ensures only one thread writes at a time.
- Unlocking happens automatically when the guard goes out of scope.

---

## 3. Move Semantics for Threads

When spawning a thread:

thread::spawn(move || { ... });

The “move” keyword transfers ownership of captured variables into the thread.
This prevents:
- dangling pointers
- use-after-free
- borrowing data that outlives its owner

The compiler enforces that everything inside the thread is safe to share or own.

---

## 4. Why Rust Prevents C-Style Concurrency Bugs

Rust enforces:
- Any number of immutable references, OR
- One mutable reference,
- But never both at the same time.

This eliminates:
- data races
- concurrent write errors
- iterator invalidation
- undefined behavior

If something is unsafe in C, Rust catches it at compile time.

---

## Summary Table

Rust Feature | C Analogy | Safety Benefit
------------ | ---------- | --------------
Box<T> | malloc | automatic free, no leaks
Arc<T> | shared pointer | guaranteed lifetime correctness
RwLock<T> | pthread_rwlock_t | safe enforced access
Mutex<T> | pthread_mutex_t | no forgotten unlocks
Option<T> | NULL handling | null-safety enforced

---

Rust’s strict compile-time rules remove entire classes of concurrency bugs—while still giving low-level control similar to C.