const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");
const btree = @import("../btree.zig");

const Pager = pager.Pager;
const BTree = btree.BTree;
const BTreeNode = btree.BTreeNode;
const NodeHeader = constants.NodeHeader;
const NodeType = constants.NodeType;
const HEADER_SIZE = btree.HEADER_SIZE;
const MAX_KEYS = btree.MAX_KEYS;
const DATA_START_OFFSET = btree.DATA_START_OFFSET;
const VerifyStats = BTree.VerifyStats;
const InspectStats = BTree.InspectStats;
const PAGE_CACHE_LIMIT = @import("../pager/types.zig").PAGE_CACHE_LIMIT;

fn initTestPager(allocator: std.mem.Allocator, path: []const u8) !Pager {
    return Pager.init(allocator, path, .always, PAGE_CACHE_LIMIT);
}
const Error = constants.Error;
const KeyInfo = btree.KeyInfo;

fn initEmptyRootLeaf(p: *Pager) !void {
    // Reuse the reserved root page as a pristine leaf before each test scenario.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    const header = NodeHeader{
        .node_type = .leaf,
        .num_keys = 0,
    };
    @memcpy(root_page.data[0..HEADER_SIZE], std.mem.asBytes(&header));
    root_page.markDirty();
}

fn keyInfoPtr(node: *BTreeNode) [*]KeyInfo {
    return @ptrCast(@alignCast(&node.page.data[HEADER_SIZE]));
}

test "btree: verify rejects invalid node type header" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_invalid_node_type.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    root_page.data[0] = 99;
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try std.testing.expectError(Error.CorruptedData, tree.verify(&p));
    try std.testing.expectError(Error.CorruptedData, tree.inspect(&p));
}

test "btree: verify rejects out-of-bounds leaf payload" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_invalid_payload.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    var node = BTreeNode.init(root_page);
    try node.insertLeaf("alpha", "one");

    const key_infos = keyInfoPtr(&node);
    key_infos[0].value_offset = constants.PAGE_SIZE - 1;
    key_infos[0].value_len = 4;
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try std.testing.expectError(Error.CorruptedData, tree.verify(&p));
    try std.testing.expectError(Error.CorruptedData, tree.inspect(&p));
}

test "btree: verify rejects invalid internal child page id" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_invalid_child.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    root_page.clear();
    var root_node = BTreeNode.init(root_page);
    root_node.header = .{
        .node_type = .internal,
        .num_keys = 1,
    };
    root_node.saveHeader();

    const left_page = try p.allocatePage();
    left_page.clear();
    var left_node = BTreeNode.init(left_page);
    left_node.header = .{
        .node_type = .leaf,
        .num_keys = 0,
    };
    left_node.saveHeader();
    try left_node.insertLeaf("a", "left");

    const key_infos = keyInfoPtr(&root_node);
    @memcpy(root_page.data[DATA_START_OFFSET..][0..1], "z");
    key_infos[0] = .{
        .key_offset = DATA_START_OFFSET,
        .key_len = 1,
        .value_offset = DATA_START_OFFSET + 1,
        .value_len = 0,
    };
    root_node.setChildPageId(0, left_page.id);
    root_node.setChildPageId(1, constants.INVALID_PAGE_ID);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try std.testing.expectError(Error.CorruptedData, tree.verify(&p));
    try std.testing.expectError(Error.CorruptedData, tree.inspect(&p));
}

test "btree: verify rejects overlapping leaf entries" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_overlapping_entries.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    var node = BTreeNode.init(root_page);
    try node.insertLeaf("alpha", "one");
    try node.insertLeaf("beta", "two");

    const key_infos = keyInfoPtr(&node);
    key_infos[1].key_offset = key_infos[0].key_offset;
    key_infos[1].value_offset = key_infos[1].key_offset + key_infos[1].key_len;
    root_page.markDirty();

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try std.testing.expectError(Error.CorruptedData, tree.verify(&p));
    try std.testing.expectError(Error.CorruptedData, tree.inspect(&p));
}

test "btree: large random insert workload stays correct across multi-level growth" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_large_random_growth.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    var ids: [10000]usize = undefined;
    for (&ids, 0..) |*slot, i| {
        slot.* = i;
    }
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    prng.random().shuffle(usize, &ids);

    for (ids, 0..) |id, insert_index| {
        var key_buf: [32]u8 = undefined;
        var value_buf: [48]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "bench-key-{d:0>8}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "bench-value-{d:0>12}", .{id});
        try tree.put(&p, key, value);

        if ((insert_index + 1) % 250 == 0) {
            _ = try tree.verify(&p);
        }
    }

    const stats = try tree.inspect(&p);
    try std.testing.expect(stats.tree_height >= 3);
    try std.testing.expectEqual(@as(usize, 10000), stats.entry_count);
    _ = try tree.verify(&p);

    for (0..10000) |id| {
        var key_buf: [32]u8 = undefined;
        var expected_value_buf: [48]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "bench-key-{d:0>8}", .{id});
        const expected_value = try std.fmt.bufPrint(&expected_value_buf, "bench-value-{d:0>12}", .{id});
        const value = try tree.get(&p, key, allocator);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings(expected_value, value.?);
        allocator.free(value.?);
    }
}

test "btree: duplicate put rejects existing keys across tree growth" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_duplicate_put.db";
    // Reset the fixture file so duplicate-key assertions only observe this run's tree growth.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    // Confirm duplicate detection works before and after the tree grows beyond one leaf.
    try tree.put(&p, "dup", "original");
    try std.testing.expectError(Error.KeyAlreadyExists, tree.put(&p, "dup", "again"));

    const first_value = try tree.get(&p, "dup", allocator);
    try std.testing.expect(first_value != null);
    try std.testing.expectEqualStrings("original", first_value.?);
    defer allocator.free(first_value.?);

    for (0..180) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "g{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    try std.testing.expectError(Error.KeyAlreadyExists, tree.put(&p, "g0042", "changed"));
    const grown_value = try tree.get(&p, "g0042", allocator);
    try std.testing.expect(grown_value != null);
    try std.testing.expectEqualStrings("val0042", grown_value.?);
    defer allocator.free(grown_value.?);
}

test "btree: update missing key returns not found" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_update_missing.db";
    // Reset the fixture file so the missing-key update path starts from a clean single-leaf tree.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    // Insert a base key so the failed update cannot be explained by an empty tree.
    try tree.put(&p, "known", "value");

    try std.testing.expectError(Error.KeyNotFound, tree.update(&p, "missing", "new-value"));

    const known = try tree.get(&p, "known", allocator);
    try std.testing.expect(known != null);
    try std.testing.expectEqualStrings("value", known.?);
    defer allocator.free(known.?);
}

test "btree: delete missing key returns not found" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_delete_missing.db";
    // Reset the fixture file so the missing-delete assertion is isolated from prior runs.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    // Populate enough keys to ensure the missing delete is checked after multi-page growth.
    for (0..140) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "m{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    try std.testing.expectError(Error.KeyNotFound, tree.delete(&p, "m9999"));

    const still_there = try tree.get(&p, "m0064", allocator);
    try std.testing.expect(still_there != null);
    try std.testing.expectEqualStrings("val0064", still_there.?);
    defer allocator.free(still_there.?);
}

test "btree: empty tree iterator returns null" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_empty_iterator.db";
    // Reset the fixture file so the iterator null case starts from an empty persisted tree.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    var iter = try tree.iterator(&p);
    defer iter.deinit();

    // An empty tree should yield no first entry at all.
    try std.testing.expect((try iter.next()) == null);
}

test "btree: verify and inspect summarize multi-level tree" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_verify_inspect.db";
    // Reset the fixture file so verify/inspect statistics reflect only this generated tree.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    for (0..220) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "v{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "row{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    const verify_stats: VerifyStats = try tree.verify(&p);
    try std.testing.expect(verify_stats.checked_pages >= 3);
    try std.testing.expectEqual(@as(usize, 220), verify_stats.checked_entries);

    const inspect_stats: InspectStats = try tree.inspect(&p);
    try std.testing.expect(inspect_stats.tree_height >= 2);
    try std.testing.expect(inspect_stats.node_count >= 3);
    try std.testing.expect(inspect_stats.leaf_count >= 2);
    try std.testing.expect(inspect_stats.internal_count >= 1);
    try std.testing.expectEqual(@as(usize, 220), inspect_stats.entry_count);
}

test "btree: iterator stays sorted after mixed-order inserts" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_iterator_mixed_sorted.db";
    // Reset the fixture file so sorted-iteration assertions see only this mixed insert workload.
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    for (0..120) |i| {
        const id = i * 2;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "s{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }
    for (0..120) |i| {
        const id = i * 2 + 1;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "s{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }

    var iter = try tree.iterator(&p);
    defer iter.deinit();

    var expected_index: usize = 0;
    while (try iter.next()) |entry| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [20]u8 = undefined;
        const expected_key = try std.fmt.bufPrint(&key_buf, "s{d:0>4}", .{expected_index});
        const expected_value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{expected_index});
        try std.testing.expectEqualStrings(expected_key, entry.key);
        try std.testing.expectEqualStrings(expected_value, entry.value);
        expected_index += 1;
    }
}

test "btree: basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reinitialize the reserved root page as an empty leaf so the test can
    // exercise the single-page insert/search/delete path directly.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Seed a few entries so lookups cover sorted insertion and retrieval.
    try tree.put(&p, "hello", "world");
    try tree.put(&p, "foo", "bar");
    try tree.put(&p, "zig", "awesome");

    // Confirm the first inserted key can be found with its stored payload.
    const value1 = try tree.get(&p, "hello", allocator);
    try std.testing.expect(value1 != null);
    try std.testing.expectEqualStrings("world", value1.?);
    defer allocator.free(value1.?);

    // Check an additional key so the test does not only validate one slot.
    const foo_value = try tree.get(&p, "foo", allocator);
    try std.testing.expect(foo_value != null);
    try std.testing.expectEqualStrings("bar", foo_value.?);
    defer allocator.free(foo_value.?);

    // Remove one key and verify the leaf-level delete path makes it disappear.
    try tree.delete(&p, "foo");
    const value2 = try tree.get(&p, "foo", allocator);
    try std.testing.expect(value2 == null);
}

test "btree: root split handles first overflow" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_root_split.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Start from an empty leaf root so the test drives the first overflow path.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Fill the original root leaf to capacity with ordered keys.
    for (0..MAX_KEYS) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>3}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>3}", .{i});
        try tree.put(&p, key, value);
    }

    // One more insert should trigger the first root split instead of NodeFull.
    try tree.put(&p, "k999", "v999");

    // The original root page should now be an internal node.
    const new_root = BTreeNode.init(try p.getPage(constants.ROOT_PAGE_ID));
    try std.testing.expectEqual(NodeType.internal, new_root.header.node_type);
    try std.testing.expectEqual(@as(u16, 1), new_root.header.num_keys);

    // Reads should still succeed from both children after the split.
    const low = try tree.get(&p, "k000", allocator);
    try std.testing.expect(low != null);
    try std.testing.expectEqualStrings("v000", low.?);
    defer allocator.free(low.?);

    const high = try tree.get(&p, "k999", allocator);
    try std.testing.expect(high != null);
    try std.testing.expectEqualStrings("v999", high.?);
    defer allocator.free(high.?);
}

test "btree: internal insert split propagation keeps tree searchable" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_internal_split.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reset the reserved root page so this test exercises multi-level growth
    // from a clean leaf root under deterministic key ordering.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert enough ordered keys to force repeated child splits and at least one
    // internal-root split into a three-level tree.
    for (0..260) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // The root should remain internal once the tree has grown past one level.
    const new_root = BTreeNode.init(try p.getPage(constants.ROOT_PAGE_ID));
    try std.testing.expectEqual(NodeType.internal, new_root.header.node_type);
    try std.testing.expect(new_root.header.num_keys >= 1);

    // Boundary and interior keys should still resolve correctly after multiple
    // promoted separators and internal-node rewrites.
    const first = try tree.get(&p, "k0000", allocator);
    try std.testing.expect(first != null);
    try std.testing.expectEqualStrings("v0000", first.?);
    defer allocator.free(first.?);

    const middle = try tree.get(&p, "k0128", allocator);
    try std.testing.expect(middle != null);
    try std.testing.expectEqualStrings("v0128", middle.?);
    defer allocator.free(middle.?);

    const last = try tree.get(&p, "k0259", allocator);
    try std.testing.expect(last != null);
    try std.testing.expectEqualStrings("v0259", last.?);
    defer allocator.free(last.?);
}

test "btree: ordered multi-level search hits separator boundaries" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_ordered_boundaries.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Start from a clean leaf root so ordered inserts alone determine every
    // separator promoted into the internal levels.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Drive the tree deep enough that searches must cross several promoted
    // boundaries instead of staying in the original root split.
    for (0..320) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "v{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // Probe keys around likely split boundaries plus a miss between them to make
    // sure child selection stays stable at separator edges.
    const probe_keys = [_][]const u8{ "k0000", "k0063", "k0064", "k0127", "k0128", "k0191", "k0192", "k0255", "k0256", "k0319" };
    const probe_values = [_][]const u8{ "v0000", "v0063", "v0064", "v0127", "v0128", "v0191", "v0192", "v0255", "v0256", "v0319" };
    for (probe_keys, probe_values) |probe_key, expected_value| {
        const value = try tree.get(&p, probe_key, allocator);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings(expected_value, value.?);
        defer allocator.free(value.?);
    }

    const missing = try tree.get(&p, "k0325", allocator);
    try std.testing.expect(missing == null);
}

test "btree: mixed-order multi-level inserts remain searchable" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_mixed_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reset the root so a non-monotonic insert order can exercise recursive
    // routing across multiple branches from a clean tree state.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert evens first and odds second so the tree grows across mixed ranges
    // instead of only along a purely ordered append pattern.
    for (0..100) |i| {
        const id = i * 2;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }
    for (0..100) |i| {
        const id = i * 2 + 1;
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const value = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        try tree.put(&p, key, value);
    }

    // Re-read a spread of inserted keys to confirm mixed-order routing still
    // lands in the correct leaf after internal-node promotions.
    const check_ids = [_]u16{ 0, 7, 31, 63, 64, 99, 127, 128, 173, 199 };
    for (check_ids) |id| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "r{d:0>4}", .{id});
        const expected = try std.fmt.bufPrint(&value_buf, "value-{d:0>4}", .{id});
        const value = try tree.get(&p, key, allocator);
        try std.testing.expect(value != null);
        try std.testing.expectEqualStrings(expected, value.?);
        defer allocator.free(value.?);
    }
}

test "btree: iterator traverses multi-page tree in sorted order" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_iterator_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reset the root so iterator coverage starts from a clean tree that must
    // grow beyond a single page before traversal begins.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Insert enough ordered rows to force multiple leaves and internal levels.
    for (0..150) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "it{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    var iter = try tree.iterator(&p);
    defer iter.deinit();

    var expected_index: usize = 0;
    while (try iter.next()) |entry| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const expected_key = try std.fmt.bufPrint(&key_buf, "it{d:0>4}", .{expected_index});
        const expected_value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{expected_index});
        try std.testing.expectEqualStrings(expected_key, entry.key);
        try std.testing.expectEqualStrings(expected_value, entry.value);
        expected_index += 1;
    }

    // The iterator should visit every inserted row exactly once.
    try std.testing.expectEqual(@as(usize, 150), expected_index);
}

test "btree: multi-page delete updates surviving separator search paths" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_delete_multilevel.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reset the root so delete coverage starts from a deterministic multi-page
    // tree instead of inheriting earlier structural state.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Build a multi-page tree large enough that deleting one boundary key forces
    // parent separators to be refreshed for the surviving right subtree.
    for (0..130) |i| {
        var key_buf: [16]u8 = undefined;
        var value_buf: [16]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "d{d:0>4}", .{i});
        const value = try std.fmt.bufPrint(&value_buf, "val{d:0>4}", .{i});
        try tree.put(&p, key, value);
    }

    // Remove the first key from a non-root right subtree and ensure nearby keys
    // remain searchable through the updated separator chain.
    try tree.delete(&p, "d0064");

    const removed = try tree.get(&p, "d0064", allocator);
    try std.testing.expect(removed == null);

    const next_value = try tree.get(&p, "d0065", allocator);
    try std.testing.expect(next_value != null);
    try std.testing.expectEqualStrings("val0065", next_value.?);
    defer allocator.free(next_value.?);

    const far_value = try tree.get(&p, "d0129", allocator);
    try std.testing.expect(far_value != null);
    try std.testing.expectEqualStrings("val0129", far_value.?);
    defer allocator.free(far_value.?);
}

test "btree: multi-page delete rejects unsupported leaf underflow" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_delete_underflow.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Reset the root so this test can create a small manual multi-page layout
    // with a single-entry non-root leaf and no rebalance support.
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);
    root_page.clear();
    var root_node = BTreeNode.init(root_page);
    root_node.header = .{
        .node_type = .internal,
        .num_keys = 1,
    };
    root_node.saveHeader();

    const left_page = try p.allocatePage();
    var left_page_node = BTreeNode.init(left_page);
    left_page_node.header = .{
        .node_type = .leaf,
        .num_keys = 0,
    };
    left_page_node.saveHeader();
    try left_page_node.insertLeaf("a000", "left");

    const right_page = try p.allocatePage();
    var right_page_node = BTreeNode.init(right_page);
    right_page_node.header = .{
        .node_type = .leaf,
        .num_keys = 0,
    };
    right_page_node.saveHeader();
    try right_page_node.insertLeaf("z000", "right");

    const separator_key = "z000";
    const key_infos = root_node.getKeyInfoPtr();
    @memcpy(root_page.data[DATA_START_OFFSET..][0..separator_key.len], separator_key);
    key_infos[0] = .{
        .key_offset = @intCast(DATA_START_OFFSET),
        .key_len = @intCast(separator_key.len),
        .value_offset = @intCast(DATA_START_OFFSET + separator_key.len),
        .value_len = 0,
    };
    root_page.markDirty();
    root_node.setChildPageId(0, left_page.id);
    root_node.setChildPageId(1, right_page.id);

    var tree = BTree.init(constants.ROOT_PAGE_ID);

    // Deleting the lone entry from a non-root leaf would require borrow/merge,
    // so the current staged implementation should reject it cleanly.
    try std.testing.expectError(Error.NodeEmpty, tree.delete(&p, "z000"));

    const value = try tree.get(&p, "z000", allocator);
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("right", value.?);
    defer allocator.free(value.?);
}

test "btree: repeated updates repack leaf payloads" {
    const allocator = std.testing.allocator;
    const test_path = "test_btree_update_repack.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path);
    defer p.deinit();

    // Start from a clean leaf root so repeated updates exercise the repack path
    // without multi-page structure affecting the payload layout assertions.
    try initEmptyRootLeaf(&p);

    var tree = BTree.init(constants.ROOT_PAGE_ID);
    try tree.put(&p, "name", "alpha");

    // Repeatedly overwrite the same key with larger payloads; repacking should
    // keep only one logical entry and leave the latest value readable.
    for (0..40) |i| {
        var value_buf: [96]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "updated-value-{d:0>2}-with-extra-padding", .{i});
        try tree.update(&p, "name", value);
    }

    const page = try p.getPage(constants.ROOT_PAGE_ID);
    var node = BTreeNode.init(page);
    try std.testing.expectEqual(@as(u16, 1), node.header.num_keys);

    const next_offset = node.getNextDataOffset();
    try std.testing.expect(next_offset < DATA_START_OFFSET + 96);

    const value = try tree.get(&p, "name", allocator);
    try std.testing.expect(value != null);
    try std.testing.expectEqualStrings("updated-value-39-with-extra-padding", value.?);
    defer allocator.free(value.?);
}
