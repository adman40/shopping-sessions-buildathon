#![doc = "Deterministic identity graph and canonicalization for Shopping Sessions V2."]
#![forbid(unsafe_code)]

use std::mem::size_of;

use shopping_sessions_domain::{
    ByteHandle, ByteSpan, CanonicalPerson, Canonicalize, PersonCode, PipelineError,
    PipelineErrorKind,
};

const NO_INDEX: u32 = u32::MAX;
const MIN_HASH_SLOTS: usize = 16;
const HASH_LOAD_NUMERATOR: usize = 7;
const HASH_LOAD_DENOMINATOR: usize = 8;

/// The planning envelope for identity construction at representative final scale.
pub const IDENTITY_MEMORY_ENVELOPE_BYTES: usize = 512 * 1024 * 1024;

type ByteHasher = fn(&[u8]) -> u64;

/// Mutable identity construction state.
///
/// Register every event/click ID with [`Self::ingest_id`] and every semantically
/// valid link with [`Self::ingest_link`]. Finishing consumes the mutable state and
/// produces an immutable graph with deterministic handles and person codes.
pub struct IdentityBuilder {
    ids: ExactByteIndex,
    dsu: DisjointSet,
}

impl Default for IdentityBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl IdentityBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self {
            ids: ExactByteIndex::new(hash_bytes),
            dsu: DisjointSet::default(),
        }
    }

    #[cfg(test)]
    fn with_hasher(hasher: ByteHasher) -> Self {
        Self {
            ids: ExactByteIndex::new(hasher),
            dsu: DisjointSet::default(),
        }
    }

    /// Registers an ID that may remain a singleton.
    ///
    /// Returns `false` for an empty, semantically invalid ID. Exact duplicate IDs
    /// are accepted without allocating another arena entry.
    ///
    /// # Errors
    ///
    /// Returns a resource-limit error if the compact arena cannot represent the ID.
    pub fn ingest_id(&mut self, user_id: &[u8]) -> Result<bool, PipelineError> {
        if user_id.is_empty() {
            return Ok(false);
        }
        self.intern(user_id)?;
        Ok(true)
    }

    /// Ingests one link after CSV grammar validation.
    ///
    /// Empty endpoints are invalid and return `false`. Repeated, reversed, and
    /// self-links are valid and idempotent.
    ///
    /// # Errors
    ///
    /// Returns a resource-limit error if the compact arena or DSU cannot represent
    /// another distinct ID.
    pub fn ingest_link(&mut self, left: &[u8], right: &[u8]) -> Result<bool, PipelineError> {
        if left.is_empty() || right.is_empty() {
            return Ok(false);
        }

        let left_id = self.intern(left)?;
        let right_id = self.intern(right)?;
        self.dsu.union(left_id, right_id);
        Ok(true)
    }

    /// Number of exact distinct IDs registered so far.
    #[must_use]
    pub fn id_count(&self) -> usize {
        self.ids.len()
    }

    /// Accounting of heap allocation capacities owned by the builder.
    #[must_use]
    pub fn estimated_heap_bytes(&self) -> usize {
        self.ids
            .estimated_heap_bytes()
            .saturating_add(self.dsu.estimated_heap_bytes())
    }

    /// Conservative projected peak while [`Self::finish`] freezes this builder.
    ///
    /// The estimate uses observed allocation capacities and includes the original
    /// index, a second immutable index, DSU storage, all post-pass arrays, and the
    /// final canonical mapping.
    #[must_use]
    pub fn estimated_finish_peak_bytes(&self) -> usize {
        let per_id_post_pass = 5_usize
            .saturating_mul(size_of::<u32>())
            .saturating_add(size_of::<CanonicalPerson>());
        self.estimated_heap_bytes()
            .saturating_add(self.ids.estimated_frozen_heap_bytes())
            .saturating_add(self.id_count().saturating_mul(per_id_post_pass))
    }

    /// Freezes connectivity, byte-minimum names, deterministic handles, and codes.
    ///
    /// # Errors
    ///
    /// Returns a resource-limit or invariant error if compact indexes cannot be
    /// represented consistently.
    pub fn finish(mut self) -> Result<IdentityGraph, PipelineError> {
        let id_count = self.ids.len();
        let mut roots = Vec::with_capacity(id_count);
        for id in 0..id_count {
            let id = u32::try_from(id).map_err(|_| resource_error("too many identity IDs"))?;
            roots.push(self.dsu.find(id));
        }

        let mut sorted_old_ids: Vec<u32> = (0..id_count)
            .map(|id| u32::try_from(id).map_err(|_| resource_error("too many identity IDs")))
            .collect::<Result<_, _>>()?;
        sorted_old_ids
            .sort_unstable_by(|left, right| self.ids.bytes(*left).cmp(self.ids.bytes(*right)));

        let mut canonical_by_root = vec![NO_INDEX; id_count];
        for old_id in &sorted_old_ids {
            let root = roots[index(*old_id)];
            let canonical = &mut canonical_by_root[index(root)];
            if *canonical == NO_INDEX {
                *canonical = *old_id;
            }
        }

        let total_bytes = sorted_old_ids.iter().try_fold(0_usize, |total, id| {
            total
                .checked_add(self.ids.bytes(*id).len())
                .ok_or_else(|| resource_error("identity byte arena size overflow"))
        })?;
        let mut frozen_ids =
            ExactByteIndex::with_capacity(id_count, total_bytes, self.ids.hasher)?;
        let mut old_to_sorted = vec![NO_INDEX; id_count];
        for old_id in &sorted_old_ids {
            let sorted_id = frozen_ids.insert(self.ids.bytes(*old_id))?;
            old_to_sorted[index(*old_id)] = sorted_id;
        }

        let mut code_by_canonical = vec![NO_INDEX; id_count];
        let mut next_code = 0_u32;
        for old_id in &sorted_old_ids {
            let root = roots[index(*old_id)];
            let canonical_old = canonical_by_root[index(root)];
            let canonical_sorted = old_to_sorted[index(canonical_old)];
            let code = &mut code_by_canonical[index(canonical_sorted)];
            if *code == NO_INDEX {
                *code = next_code;
                next_code = next_code
                    .checked_add(1)
                    .ok_or_else(|| resource_error("too many canonical people"))?;
            }
        }

        let mut people = vec![
            CanonicalPerson {
                code: PersonCode(0),
                canonical_name: ByteHandle(0),
            };
            id_count
        ];
        for old_id in 0..id_count {
            let old_id =
                u32::try_from(old_id).map_err(|_| resource_error("too many identity mappings"))?;
            let root = roots[index(old_id)];
            let canonical_old = canonical_by_root[index(root)];
            let canonical_sorted = old_to_sorted[index(canonical_old)];
            let sorted_id = old_to_sorted[index(old_id)];
            people[index(sorted_id)] = CanonicalPerson {
                code: PersonCode(code_by_canonical[index(canonical_sorted)]),
                canonical_name: ByteHandle(canonical_sorted),
            };
        }

        Ok(IdentityGraph {
            ids: frozen_ids,
            people: people.into_boxed_slice(),
            person_count: next_code,
        })
    }

    fn intern(&mut self, bytes: &[u8]) -> Result<u32, PipelineError> {
        if let Some(id) = self.ids.lookup(bytes) {
            return Ok(id);
        }
        let id = self.ids.insert(bytes)?;
        let dsu_id = self.dsu.push()?;
        if id != dsu_id {
            return Err(invariant_error("identity arena and DSU indexes diverged"));
        }
        Ok(id)
    }
}

/// Immutable exact-ID index and canonical mapping used by reducer slices.
pub struct IdentityGraph {
    ids: ExactByteIndex,
    people: Box<[CanonicalPerson]>,
    person_count: u32,
}

impl IdentityGraph {
    /// Resolves a canonical-name handle into byte-exact arena storage.
    #[must_use]
    pub fn resolve(&self, handle: ByteHandle) -> Option<&[u8]> {
        self.ids.get(handle.0)
    }

    /// Number of IDs known to this graph, including unlinked singletons.
    #[must_use]
    pub fn id_count(&self) -> usize {
        self.ids.len()
    }

    /// Number of connected components, including singleton components.
    #[must_use]
    pub const fn person_count(&self) -> u32 {
        self.person_count
    }

    /// Conservative accounting of retained heap allocations.
    #[must_use]
    pub fn estimated_heap_bytes(&self) -> usize {
        self.ids.estimated_heap_bytes().saturating_add(
            self.people
                .len()
                .saturating_mul(size_of::<CanonicalPerson>()),
        )
    }
}

impl Canonicalize for IdentityGraph {
    fn canonicalize(&self, user_bytes: &[u8]) -> Result<CanonicalPerson, PipelineError> {
        let id = self.ids.lookup(user_bytes).ok_or_else(|| {
            invariant_error(
                "canonicalization requested an ID not registered during identity construction",
            )
        })?;
        self.people
            .get(index(id))
            .copied()
            .ok_or_else(|| invariant_error("identity mapping index is out of bounds"))
    }
}

struct ExactByteIndex {
    bytes: Vec<u8>,
    spans: Vec<ByteSpan>,
    hash_slots: Vec<u32>,
    hasher: ByteHasher,
}

impl ExactByteIndex {
    fn new(hasher: ByteHasher) -> Self {
        Self {
            bytes: Vec::new(),
            spans: Vec::new(),
            hash_slots: Vec::new(),
            hasher,
        }
    }

    fn with_capacity(
        ids: usize,
        bytes: usize,
        hasher: ByteHasher,
    ) -> Result<Self, PipelineError> {
        let hash_capacity = hash_capacity_for(ids)
            .ok_or_else(|| resource_error("identity hash index capacity overflow"))?;
        Ok(Self {
            bytes: Vec::with_capacity(bytes),
            spans: Vec::with_capacity(ids),
            hash_slots: vec![NO_INDEX; hash_capacity],
            hasher,
        })
    }

    fn len(&self) -> usize {
        self.spans.len()
    }

    fn get(&self, id: u32) -> Option<&[u8]> {
        self.spans
            .get(index(id))
            .and_then(|span| span.resolve(&self.bytes))
    }

    fn bytes(&self, id: u32) -> &[u8] {
        self.get(id)
            .expect("exact-byte index contains only arena-valid spans")
    }

    fn lookup(&self, bytes: &[u8]) -> Option<u32> {
        if self.hash_slots.is_empty() {
            return None;
        }
        let mut slot = hash_slot((self.hasher)(bytes), self.hash_slots.len());
        loop {
            let candidate = self.hash_slots[slot];
            if candidate == NO_INDEX {
                return None;
            }
            if self.bytes(candidate) == bytes {
                return Some(candidate);
            }
            slot = (slot + 1) & (self.hash_slots.len() - 1);
        }
    }

    fn insert(&mut self, bytes: &[u8]) -> Result<u32, PipelineError> {
        let id = u32::try_from(self.spans.len())
            .map_err(|_| resource_error("identity ID count exceeds u32"))?;
        let offset = u32::try_from(self.bytes.len())
            .map_err(|_| resource_error("identity byte arena exceeds 4 GiB"))?;
        let length = u32::try_from(bytes.len())
            .map_err(|_| resource_error("one identity ID exceeds 4 GiB"))?;
        let span = ByteSpan::new(offset, length)
            .ok_or_else(|| resource_error("identity byte arena exceeds 4 GiB"))?;
        let new_len = self
            .bytes
            .len()
            .checked_add(bytes.len())
            .ok_or_else(|| resource_error("identity byte arena size overflow"))?;
        if new_len > u32::MAX as usize {
            return Err(resource_error("identity byte arena exceeds 4 GiB"));
        }

        self.reserve_hash_slot()?;
        let hash = (self.hasher)(bytes);
        let mut slot = hash_slot(hash, self.hash_slots.len());
        while self.hash_slots[slot] != NO_INDEX {
            slot = (slot + 1) & (self.hash_slots.len() - 1);
        }
        self.bytes.extend_from_slice(bytes);
        self.spans.push(span);
        self.hash_slots[slot] = id;
        Ok(id)
    }

    fn reserve_hash_slot(&mut self) -> Result<(), PipelineError> {
        let max_entries = self
            .hash_slots
            .len()
            .saturating_div(HASH_LOAD_DENOMINATOR)
            .saturating_mul(HASH_LOAD_NUMERATOR);
        if self.spans.len() < max_entries {
            return Ok(());
        }

        let new_capacity = if self.hash_slots.is_empty() {
            MIN_HASH_SLOTS
        } else {
            self.hash_slots
                .len()
                .checked_mul(2)
                .ok_or_else(|| resource_error("identity hash index capacity overflow"))?
        };
        let mut new_slots = vec![NO_INDEX; new_capacity];
        for raw_id in 0..self.spans.len() {
            let id = u32::try_from(raw_id)
                .map_err(|_| resource_error("identity ID count exceeds u32"))?;
            let mut slot = hash_slot((self.hasher)(self.bytes(id)), new_slots.len());
            while new_slots[slot] != NO_INDEX {
                slot = (slot + 1) & (new_slots.len() - 1);
            }
            new_slots[slot] = id;
        }
        self.hash_slots = new_slots;
        Ok(())
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.bytes
            .capacity()
            .saturating_add(self.spans.capacity().saturating_mul(size_of::<ByteSpan>()))
            .saturating_add(
                self.hash_slots
                    .capacity()
                    .saturating_mul(size_of::<u32>()),
            )
    }

    fn estimated_frozen_heap_bytes(&self) -> usize {
        let hash_capacity = hash_capacity_for(self.len()).unwrap_or(usize::MAX);
        self.bytes
            .len()
            .saturating_add(self.spans.len().saturating_mul(size_of::<ByteSpan>()))
            .saturating_add(hash_capacity.saturating_mul(size_of::<u32>()))
    }
}

fn hash_capacity_for(ids: usize) -> Option<usize> {
    if ids == 0 {
        return Some(0);
    }
    let whole = (ids / HASH_LOAD_NUMERATOR).checked_mul(HASH_LOAD_DENOMINATOR)?;
    let tail = (ids % HASH_LOAD_NUMERATOR) * HASH_LOAD_DENOMINATOR;
    let required = whole
        .checked_add(tail.div_ceil(HASH_LOAD_NUMERATOR))?
        .max(MIN_HASH_SLOTS);
    required.checked_next_power_of_two()
}

fn hash_slot(hash: u64, capacity: usize) -> usize {
    let mask = u64::try_from(capacity - 1).expect("identity hash capacity fits u64");
    usize::try_from(hash & mask).expect("masked identity hash fits usize")
}

#[derive(Default)]
struct DisjointSet {
    parent_or_size: Vec<i32>,
}

impl DisjointSet {
    fn push(&mut self) -> Result<u32, PipelineError> {
        let id = u32::try_from(self.parent_or_size.len())
            .map_err(|_| resource_error("identity DSU count exceeds u32"))?;
        if self.parent_or_size.len() >= i32::MAX as usize {
            return Err(resource_error("identity DSU count exceeds i32"));
        }
        self.parent_or_size.push(-1);
        Ok(id)
    }

    fn find(&mut self, id: u32) -> u32 {
        let mut root = id;
        while self.parent_or_size[index(root)] >= 0 {
            root = u32::try_from(self.parent_or_size[index(root)])
                .expect("nonnegative DSU parents fit u32");
        }

        let mut cursor = id;
        while cursor != root {
            let parent = u32::try_from(self.parent_or_size[index(cursor)])
                .expect("non-root DSU parents are nonnegative");
            self.parent_or_size[index(cursor)] =
                i32::try_from(root).expect("DSU root count is bounded by i32::MAX");
            cursor = parent;
        }
        root
    }

    fn union(&mut self, left: u32, right: u32) {
        let mut left_root = self.find(left);
        let mut right_root = self.find(right);
        if left_root == right_root {
            return;
        }

        if self.parent_or_size[index(left_root)] > self.parent_or_size[index(right_root)] {
            std::mem::swap(&mut left_root, &mut right_root);
        }
        let right_size = self.parent_or_size[index(right_root)];
        self.parent_or_size[index(left_root)] += right_size;
        self.parent_or_size[index(right_root)] =
            i32::try_from(left_root).expect("DSU root count is bounded by i32::MAX");
    }

    fn estimated_heap_bytes(&self) -> usize {
        self.parent_or_size
            .capacity()
            .saturating_mul(size_of::<i32>())
    }
}

fn hash_bytes(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn index(value: u32) -> usize {
    usize::try_from(value).expect("u32 identity index fits usize on supported targets")
}

fn resource_error(message: &'static str) -> PipelineError {
    PipelineError::new(PipelineErrorKind::ResourceLimit, message)
}

fn invariant_error(message: &'static str) -> PipelineError {
    PipelineError::new(PipelineErrorKind::Invariant, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn canonical_bytes<'a>(graph: &'a IdentityGraph, id: &[u8]) -> &'a [u8] {
        let person = graph.canonicalize(id).unwrap();
        graph.resolve(person.canonical_name).unwrap()
    }

    #[test]
    fn l1_links_ids_before_session_consumers() {
        let mut builder = IdentityBuilder::new();
        for id in [b"u1".as_slice(), b"u2", b"u3"] {
            assert!(builder.ingest_id(id).unwrap());
        }
        assert!(builder.ingest_link(b"u2", b"u1").unwrap());
        let graph = builder.finish().unwrap();

        assert_eq!(canonical_bytes(&graph, b"u1"), b"u1");
        assert_eq!(canonical_bytes(&graph, b"u2"), b"u1");
        assert_eq!(canonical_bytes(&graph, b"u3"), b"u3");
        assert_eq!(graph.person_count(), 2);
    }

    #[test]
    fn l2_chains_across_files_and_keeps_link_only_minimum() {
        let mut builder = IdentityBuilder::new();
        for id in [b"u3".as_slice(), b"u2", b"u10"] {
            builder.ingest_id(id).unwrap();
        }
        for (left, right) in [
            (b"u3".as_slice(), b"u9".as_slice()),
            (b"u9".as_slice(), b"u2".as_slice()),
            (b"U7".as_slice(), b"u9".as_slice()),
            (b"u10".as_slice(), b"u10".as_slice()),
        ] {
            assert!(builder.ingest_link(left, right).unwrap());
        }
        let graph = builder.finish().unwrap();

        for id in [b"U7".as_slice(), b"u2", b"u3", b"u9"] {
            assert_eq!(canonical_bytes(&graph, id), b"U7");
        }
        assert_eq!(canonical_bytes(&graph, b"u10"), b"u10");
        assert_eq!(graph.person_count(), 2);
    }

    #[test]
    fn l3_ingests_only_valid_links_and_preserves_exact_bytes() {
        let mut builder = IdentityBuilder::new();
        for id in [b"u1".as_slice(), b"u2", b"u3", b"u4", b"u5", b"u6"] {
            builder.ingest_id(id).unwrap();
        }
        assert!(!builder.ingest_link(b"u1", b"").unwrap());
        assert!(!builder.ingest_link(b"", b"u3").unwrap());
        for (left, right) in [
            (b"u4".as_slice(), b"u1".as_slice()),
            (b"u4".as_slice(), b"u1".as_slice()),
            (b"u6".as_slice(), b"u6".as_slice()),
            (b"user_id_a".as_slice(), b"user_id_b".as_slice()),
            (b"u4".as_slice(), b"user_id_b".as_slice()),
            (b"A\tB".as_slice(), b"user_id_a".as_slice()),
        ] {
            assert!(builder.ingest_link(left, right).unwrap());
        }
        let graph = builder.finish().unwrap();

        for id in [b"A\tB".as_slice(), b"u1", b"u4", b"user_id_a", b"user_id_b"] {
            assert_eq!(canonical_bytes(&graph, id), b"A\tB");
        }
        for id in [b"u2".as_slice(), b"u3", b"u5", b"u6"] {
            assert_eq!(canonical_bytes(&graph, id), id);
        }
    }

    #[test]
    fn hash_collisions_still_require_full_byte_equality() {
        let ids: Vec<Vec<u8>> = (0..40)
            .map(|id| format!("id-{id:02}").into_bytes())
            .collect();
        let mut builder = IdentityBuilder::with_hasher(|_| 7);
        for id in &ids {
            builder.ingest_id(id).unwrap();
        }
        builder.ingest_link(&ids[38], &ids[1]).unwrap();
        let graph = builder.finish().unwrap();

        for (index, id) in ids.iter().enumerate() {
            let expected = if index == 38 { &ids[1] } else { id };
            assert_eq!(canonical_bytes(&graph, id), expected);
        }
    }

    #[test]
    fn unknown_ids_fail_closed_but_registered_unlinked_ids_are_singletons() {
        let mut builder = IdentityBuilder::new();
        builder.ingest_id(b"singleton").unwrap();
        let graph = builder.finish().unwrap();

        assert_eq!(canonical_bytes(&graph, b"singleton"), b"singleton");
        let error = graph.canonicalize(b"not-registered").unwrap_err();
        assert_eq!(error.kind, PipelineErrorKind::Invariant);
    }

    #[test]
    fn union_by_size_and_path_compression_flatten_long_inputs() {
        let mut dsu = DisjointSet::default();
        for _ in 0..256 {
            dsu.push().unwrap();
        }
        for id in 1..256 {
            dsu.union(id - 1, id);
        }
        let root = dsu.find(255);
        for id in 0..256 {
            assert_eq!(dsu.find(id), root);
            if id != root {
                assert_eq!(dsu.parent_or_size[index(id)], i32::try_from(root).unwrap());
            }
        }
        assert_eq!(dsu.parent_or_size[index(root)], -256);
    }

    #[test]
    fn randomized_edge_and_file_orders_have_identical_results() {
        let ids: Vec<Vec<u8>> = (0..96)
            .map(|id| {
                if id % 17 == 0 {
                    format!("U{id:03}").into_bytes()
                } else {
                    format!("u{id:03}").into_bytes()
                }
            })
            .collect();
        let mut edges = Vec::new();
        for id in 1..ids.len() {
            if id % 11 != 0 {
                edges.push((id, id - 1));
            }
        }
        edges.extend([(4, 3), (3, 4), (20, 20), (70, 12), (95, 70)]);

        let baseline = build_snapshot(&ids, &edges);
        for seed in 1..=128 {
            let mut shuffled_ids: Vec<usize> = (0..ids.len()).collect();
            let mut shuffled_edges = edges.clone();
            shuffle(&mut shuffled_ids, seed);
            shuffle(&mut shuffled_edges, seed.wrapping_mul(0x9e37_79b9));

            let mut builder = IdentityBuilder::new();
            for id in shuffled_ids {
                builder.ingest_id(&ids[id]).unwrap();
            }
            // Alternating chunks model files discovered and consumed in different orders.
            for file in (0..5).rev() {
                for &(left, right) in shuffled_edges.iter().skip(file).step_by(5) {
                    builder.ingest_link(&ids[left], &ids[right]).unwrap();
                }
            }
            let graph = builder.finish().unwrap();
            assert_eq!(snapshot(&graph, &ids), baseline, "seed {seed}");
        }
    }

    #[test]
    fn representative_full_population_stays_below_half_gibibyte() {
        const REPRESENTATIVE_IDS: usize = 4_500_000;

        let mut builder = IdentityBuilder::new();
        for id in 0..REPRESENTATIVE_IDS {
            builder.ingest_id(&representative_id(id)).unwrap();
        }
        assert_eq!(builder.id_count(), REPRESENTATIVE_IDS);
        let measured_peak = builder.estimated_finish_peak_bytes();
        let envelope = IDENTITY_MEMORY_ENVELOPE_BYTES;
        println!(
            "identity memory: full_population={REPRESENTATIVE_IDS} measured_peak={measured_peak} envelope={envelope}"
        );
        assert!(
            measured_peak < envelope,
            "representative identity peak {measured_peak} exceeds {envelope}"
        );
    }

    fn representative_id(mut value: usize) -> [u8; 9] {
        let mut id = *b"u00000000";
        for digit in id[1..].iter_mut().rev() {
            *digit = b'0' + u8::try_from(value % 10).unwrap();
            value /= 10;
        }
        id
    }

    fn build_snapshot(ids: &[Vec<u8>], edges: &[(usize, usize)]) -> Vec<(u32, u32, Vec<u8>)> {
        let mut builder = IdentityBuilder::new();
        for id in ids {
            builder.ingest_id(id).unwrap();
        }
        for &(left, right) in edges {
            builder.ingest_link(&ids[left], &ids[right]).unwrap();
        }
        let graph = builder.finish().unwrap();
        snapshot(&graph, ids)
    }

    fn snapshot(graph: &IdentityGraph, ids: &[Vec<u8>]) -> Vec<(u32, u32, Vec<u8>)> {
        ids.iter()
            .map(|id| {
                let person = graph.canonicalize(id).unwrap();
                (
                    person.code.0,
                    person.canonical_name.0,
                    graph.resolve(person.canonical_name).unwrap().to_vec(),
                )
            })
            .collect()
    }

    fn shuffle<T>(values: &mut [T], mut state: u64) {
        for index in (1..values.len()).rev() {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let swap_with = usize::try_from(state % u64::try_from(index + 1).unwrap()).unwrap();
            values.swap(index, swap_with);
        }
    }
}
