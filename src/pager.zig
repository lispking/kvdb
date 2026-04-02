/// Public facade for the split pager implementation.
///
/// This file keeps the stable import surface while the actual implementation
/// lives under `src/pager/`.
const page = @import("pager/page.zig");
const io = @import("pager/io.zig");
const _tests = @import("pager/tests.zig");

/// In-memory database page wrapper used across pager, B-tree, and tests.
pub const Page = page.Page;
/// Page I/O, allocation, cache, metadata, and freelist API.
pub const Pager = io.Pager;
