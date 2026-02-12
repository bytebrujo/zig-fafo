# zig-fafo

Bloom filter-based transaction dependency analyzer and parallel scheduler.

## Overview

zig-fafo determines which transactions can safely execute in parallel by using
Bloom filters to approximate read/write set intersection tests.  Transactions
whose key sets do not conflict are grouped into batches that can run
concurrently.

## Components

- **BloomFilter** -- Probabilistic set membership with configurable
  false-positive rate.  Uses SipHash-2-4 with distinct seeds for each hash
  function.

- **ParallelScheduler** -- Accepts transactions annotated with read and write
  key sets, then produces a sequence of batches via greedy conflict detection.

## Usage

```zig
const fafo = @import("zig-fafo");

var sched = try fafo.ParallelScheduler.init(allocator);
defer sched.deinit();

try sched.submit(tx_id, &read_keys, &write_keys);
const batches = try sched.schedule(allocator);
```

## Build & Test

```sh
zig build test
```
