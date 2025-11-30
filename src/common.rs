// src/common.rs

#[derive(Clone, Debug)]
pub struct Record {
    pub hash: u32,
    pub name: String,
    pub salary: u32,
}

// Jenkins one-at-a-time (32-bit)
pub fn jenkins_one_at_a_time(s: &str) -> u32 {
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