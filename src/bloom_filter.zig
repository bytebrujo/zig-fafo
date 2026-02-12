const std = @import("std");
const Allocator = std.mem.Allocator;

pub const BloomFilter = struct {
    bits: []u64,
    num_hash_fns: u32,

    /// Initialize a Bloom filter sized for the given expected item count and
    /// desired false-positive rate.  Uses the classical formulas:
    ///   m = -(n * ln(p)) / (ln(2)^2)
    ///   k = (m / n) * ln(2)
    pub fn init(allocator: Allocator, expected_items: u32, false_positive_rate: f64) !BloomFilter {
        const n: f64 = @floatFromInt(expected_items);
        const ln2: f64 = @log(2.0);

        // Total number of bits.
        const m_f: f64 = -(n * @log(false_positive_rate)) / (ln2 * ln2);
        const m: usize = @max(64, @as(usize, @intFromFloat(@ceil(m_f))));

        // Number of hash functions.
        const k_f: f64 = (@as(f64, @floatFromInt(m)) / n) * ln2;
        const k: u32 = @max(1, @as(u32, @intFromFloat(@ceil(k_f))));

        // Round up to whole u64 words.
        const num_words = (m + 63) / 64;
        const words = try allocator.alloc(u64, num_words);
        @memset(words, 0);

        return .{
            .bits = words,
            .num_hash_fns = k,
        };
    }

    pub fn deinit(self: *BloomFilter, allocator: Allocator) void {
        allocator.free(self.bits);
        self.bits = &.{};
    }

    /// Insert a key into the filter.
    pub fn insert(self: *BloomFilter, key: []const u8) void {
        const total_bits: u64 = @as(u64, self.bits.len) * 64;
        for (0..self.num_hash_fns) |i| {
            const h = hash(key, @intCast(i));
            const bit_index = h % total_bits;
            const word_index = bit_index / 64;
            const bit_offset: u6 = @intCast(bit_index % 64);
            self.bits[word_index] |= @as(u64, 1) << bit_offset;
        }
    }

    /// Test whether the filter probably contains the key.
    pub fn contains(self: *const BloomFilter, key: []const u8) bool {
        const total_bits: u64 = @as(u64, self.bits.len) * 64;
        for (0..self.num_hash_fns) |i| {
            const h = hash(key, @intCast(i));
            const bit_index = h % total_bits;
            const word_index = bit_index / 64;
            const bit_offset: u6 = @intCast(bit_index % 64);
            if (self.bits[word_index] & (@as(u64, 1) << bit_offset) == 0) {
                return false;
            }
        }
        return true;
    }

    /// Return true if any bit is set in both filters (i.e. the bitwise AND is
    /// non-zero).  The filters must have the same length.
    pub fn intersects(self: *const BloomFilter, other: *const BloomFilter) bool {
        std.debug.assert(self.bits.len == other.bits.len);
        for (self.bits, other.bits) |a, b| {
            if (a & b != 0) return true;
        }
        return false;
    }

    /// Clear all bits.
    pub fn reset(self: *BloomFilter) void {
        @memset(self.bits, 0);
    }

    // ---- internal --------------------------------------------------------

    /// Produce a deterministic hash for the given key and seed index by running
    /// SipHash-2-4 with a seed derived from `index`.
    fn hash(key: []const u8, index: u32) u64 {
        // Build a 128-bit SipHash key from the seed index.
        const seed_lo: u64 = @as(u64, index) *% 0x517cc1b727220a95;
        const seed_hi: u64 = @as(u64, index) *% 0x6c62272e07bb0142 +% 0x9e3779b97f4a7c15;
        const sip_key: [2]u64 = .{ seed_lo, seed_hi };

        var hasher = std.hash.SipHash64(2, 4).init(&@as([16]u8, @bitCast(sip_key)));
        hasher.update(key);
        return hasher.finalInt();
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "insert and contains" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilter.init(allocator, 100, 0.01);
    defer bf.deinit(allocator);

    bf.insert("hello");
    bf.insert("world");

    try std.testing.expect(bf.contains("hello"));
    try std.testing.expect(bf.contains("world"));
    // Not inserted -- very unlikely to be a false positive for a single item.
    try std.testing.expect(!bf.contains("missing"));
}

test "reset clears all bits" {
    const allocator = std.testing.allocator;
    var bf = try BloomFilter.init(allocator, 64, 0.01);
    defer bf.deinit(allocator);

    bf.insert("key");
    try std.testing.expect(bf.contains("key"));

    bf.reset();
    try std.testing.expect(!bf.contains("key"));
}

test "intersects detects overlap" {
    const allocator = std.testing.allocator;
    var a = try BloomFilter.init(allocator, 100, 0.01);
    defer a.deinit(allocator);
    var b = try BloomFilter.init(allocator, 100, 0.01);
    defer b.deinit(allocator);

    // No overlap yet.
    try std.testing.expect(!a.intersects(&b));

    a.insert("shared");
    b.insert("shared");
    try std.testing.expect(a.intersects(&b));
}

test "intersects returns false for disjoint filters" {
    const allocator = std.testing.allocator;
    var a = try BloomFilter.init(allocator, 1000, 0.001);
    defer a.deinit(allocator);
    var b = try BloomFilter.init(allocator, 1000, 0.001);
    defer b.deinit(allocator);

    a.insert("alpha");
    b.insert("beta");
    // With a large filter and only one element each, intersection is extremely
    // unlikely (though theoretically possible).  We accept this as a valid
    // probabilistic test.
    try std.testing.expect(!a.intersects(&b));
}
