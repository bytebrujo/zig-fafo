const std = @import("std");
const Allocator = std.mem.Allocator;
const bloom = @import("bloom_filter.zig");
const BloomFilter = bloom.BloomFilter;

pub const Batch = struct {
    tx_ids: []const u64,
};

const Transaction = struct {
    tx_id: u64,
    read_keys: std.ArrayList([32]u8),
    write_keys: std.ArrayList([32]u8),

    fn deinit(self: *Transaction, gpa: Allocator) void {
        self.read_keys.deinit(gpa);
        self.write_keys.deinit(gpa);
    }
};

/// Bloom filter pair for a single transaction (read-set + write-set).
const TxFilters = struct {
    read_bf: BloomFilter,
    write_bf: BloomFilter,
};

pub const ParallelScheduler = struct {
    allocator: Allocator,
    transactions: std.ArrayList(Transaction),

    pub fn init(allocator: Allocator) !ParallelScheduler {
        return .{
            .allocator = allocator,
            .transactions = .empty,
        };
    }

    pub fn deinit(self: *ParallelScheduler) void {
        for (self.transactions.items) |*tx| {
            tx.deinit(self.allocator);
        }
        self.transactions.deinit(self.allocator);
    }

    /// Submit a transaction with its read and write key sets.
    pub fn submit(
        self: *ParallelScheduler,
        tx_id: u64,
        read_keys: []const [32]u8,
        write_keys: []const [32]u8,
    ) !void {
        var rk: std.ArrayList([32]u8) = .empty;
        errdefer rk.deinit(self.allocator);
        try rk.appendSlice(self.allocator, read_keys);

        var wk: std.ArrayList([32]u8) = .empty;
        errdefer wk.deinit(self.allocator);
        try wk.appendSlice(self.allocator, write_keys);

        try self.transactions.append(self.allocator, .{
            .tx_id = tx_id,
            .read_keys = rk,
            .write_keys = wk,
        });
    }

    /// Build a parallel schedule.  Returns an array of batches where all
    /// transactions within a batch are independent and can execute in parallel.
    ///
    /// The caller owns the returned memory (allocated via `alloc`).
    pub fn schedule(self: *ParallelScheduler, alloc: Allocator) ![]const Batch {
        const txs = self.transactions.items;
        if (txs.len == 0) return try alloc.alloc(Batch, 0);

        // Expected items per filter -- use a reasonable lower bound so the
        // filters are not degenerate when a transaction touches few keys.
        const expected: u32 = 64;
        const fp_rate: f64 = 0.01;

        // Build per-transaction Bloom filters.
        var filters = try alloc.alloc(TxFilters, txs.len);
        defer {
            for (filters) |*f| {
                f.read_bf.deinit(alloc);
                f.write_bf.deinit(alloc);
            }
            alloc.free(filters);
        }

        for (txs, 0..) |tx, i| {
            var rbf = try BloomFilter.init(alloc, expected, fp_rate);
            errdefer rbf.deinit(alloc);
            for (tx.read_keys.items) |key| {
                rbf.insert(&key);
            }

            var wbf = try BloomFilter.init(alloc, expected, fp_rate);
            errdefer wbf.deinit(alloc);
            for (tx.write_keys.items) |key| {
                wbf.insert(&key);
            }

            filters[i] = .{ .read_bf = rbf, .write_bf = wbf };
        }

        // Greedy batching.
        var batches: std.ArrayList(Batch) = .empty;
        errdefer {
            for (batches.items) |b| alloc.free(b.tx_ids);
            batches.deinit(alloc);
        }

        // Track which transactions have been assigned.
        var assigned = try alloc.alloc(bool, txs.len);
        defer alloc.free(assigned);
        @memset(assigned, false);

        var remaining: usize = txs.len;

        while (remaining > 0) {
            // Indices of transactions placed in the current batch.
            var batch_indices: std.ArrayList(usize) = .empty;
            defer batch_indices.deinit(alloc);

            for (txs, 0..) |_, tx_i| {
                if (assigned[tx_i]) continue;

                // Check this transaction against every transaction already in
                // the current batch.
                const compatible = blk: {
                    for (batch_indices.items) |other_i| {
                        // WAW conflict: write-write
                        if (filters[tx_i].write_bf.intersects(&filters[other_i].write_bf))
                            break :blk false;
                        // WAR conflict: new tx writes vs existing tx reads
                        if (filters[tx_i].write_bf.intersects(&filters[other_i].read_bf))
                            break :blk false;
                        // RAW conflict: new tx reads vs existing tx writes
                        if (filters[tx_i].read_bf.intersects(&filters[other_i].write_bf))
                            break :blk false;
                    }
                    break :blk true;
                };

                if (compatible) {
                    try batch_indices.append(alloc, tx_i);
                    assigned[tx_i] = true;
                    remaining -= 1;
                }
            }

            // Convert indices to tx_ids.
            const ids = try alloc.alloc(u64, batch_indices.items.len);
            for (batch_indices.items, 0..) |idx, i| {
                ids[i] = txs[idx].tx_id;
            }
            try batches.append(alloc, .{ .tx_ids = ids });
        }

        return batches.toOwnedSlice(alloc);
    }

    /// Remove all submitted transactions so the scheduler can be reused.
    pub fn reset(self: *ParallelScheduler) void {
        for (self.transactions.items) |*tx| {
            tx.deinit(self.allocator);
        }
        self.transactions.clearRetainingCapacity();
    }
};

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

fn makeKey(comptime seed: u8) [32]u8 {
    var k: [32]u8 = undefined;
    @memset(&k, seed);
    return k;
}

test "independent transactions land in one batch" {
    const allocator = std.testing.allocator;
    var sched = try ParallelScheduler.init(allocator);
    defer sched.deinit();

    const rk1 = [_][32]u8{makeKey(1)};
    const wk1 = [_][32]u8{makeKey(2)};
    const rk2 = [_][32]u8{makeKey(3)};
    const wk2 = [_][32]u8{makeKey(4)};

    try sched.submit(100, &rk1, &wk1);
    try sched.submit(200, &rk2, &wk2);

    const batches = try sched.schedule(allocator);
    defer {
        for (batches) |b| allocator.free(b.tx_ids);
        allocator.free(batches);
    }

    // Both should be in one batch since key sets are disjoint.
    try std.testing.expectEqual(@as(usize, 1), batches.len);
    try std.testing.expectEqual(@as(usize, 2), batches[0].tx_ids.len);
}

test "conflicting transactions are serialized" {
    const allocator = std.testing.allocator;
    var sched = try ParallelScheduler.init(allocator);
    defer sched.deinit();

    const shared = [_][32]u8{makeKey(42)};

    // TX 1 writes key, TX 2 reads same key -> RAW conflict.
    try sched.submit(1, &.{}, &shared); // writes shared
    try sched.submit(2, &shared, &.{}); // reads shared

    const batches = try sched.schedule(allocator);
    defer {
        for (batches) |b| allocator.free(b.tx_ids);
        allocator.free(batches);
    }

    try std.testing.expectEqual(@as(usize, 2), batches.len);
    try std.testing.expectEqual(@as(usize, 1), batches[0].tx_ids.len);
    try std.testing.expectEqual(@as(usize, 1), batches[1].tx_ids.len);
}

test "write-write conflict" {
    const allocator = std.testing.allocator;
    var sched = try ParallelScheduler.init(allocator);
    defer sched.deinit();

    const shared = [_][32]u8{makeKey(99)};

    try sched.submit(1, &.{}, &shared);
    try sched.submit(2, &.{}, &shared);

    const batches = try sched.schedule(allocator);
    defer {
        for (batches) |b| allocator.free(b.tx_ids);
        allocator.free(batches);
    }

    try std.testing.expectEqual(@as(usize, 2), batches.len);
}

test "reset allows reuse" {
    const allocator = std.testing.allocator;
    var sched = try ParallelScheduler.init(allocator);
    defer sched.deinit();

    const k = [_][32]u8{makeKey(1)};
    try sched.submit(1, &k, &.{});
    sched.reset();

    try std.testing.expectEqual(@as(usize, 0), sched.transactions.items.len);
}

test "empty schedule" {
    const allocator = std.testing.allocator;
    var sched = try ParallelScheduler.init(allocator);
    defer sched.deinit();

    const batches = try sched.schedule(allocator);
    defer allocator.free(batches);

    try std.testing.expectEqual(@as(usize, 0), batches.len);
}
