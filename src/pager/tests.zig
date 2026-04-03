const std = @import("std");
const constants = @import("../constants.zig");
const pager = @import("../pager.zig");

const PAGE_CACHE_LIMIT = @import("types.zig").PAGE_CACHE_LIMIT;
const Page = pager.Page;
const Pager = pager.Pager;
const PageId = constants.PageId;
const INVALID_PAGE_ID = constants.INVALID_PAGE_ID;

fn initTestPager(allocator: std.mem.Allocator, path: []const u8, fsync_policy: anytype) !pager.Pager {
    return pager.Pager.init(allocator, path, fsync_policy, PAGE_CACHE_LIMIT);
}

test "pager: basic operations" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    {
        var p = try initTestPager(allocator, test_path, .always);
        defer p.deinit();

        try std.testing.expectEqual(@as(PageId, 2), p.pageCount());

        const meta_page = try p.getPage(0);
        try std.testing.expectEqual(@as(PageId, 0), meta_page.id);

        const new_page = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), new_page.id);

        try p.flush();
    }

    {
        var p = try initTestPager(allocator, test_path, .always);
        defer p.deinit();

        try std.testing.expectEqual(@as(PageId, 3), p.pageCount());
    }
}

test "pager: freed pages are reused before file growth" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_freelist.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    {
        var p = try initTestPager(allocator, test_path, .always);
        defer p.deinit();

        const first = try p.allocatePage();
        const second = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), first.id);
        try std.testing.expectEqual(@as(PageId, 3), second.id);

        try p.freePage(first.id);
        const recycled = try p.allocatePage();
        try std.testing.expectEqual(first.id, recycled.id);

        try std.testing.expectEqual(@as(PageId, 4), p.pageCount());
        const metadata = try p.readMetadata();
        try std.testing.expectEqual(INVALID_PAGE_ID, metadata.freelist_page);
        try std.testing.expectEqual(@as(PageId, 3), metadata.last_page_id);
    }
}

test "pager: cache lookup returns same page instance" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_cache_lookup.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path, .always);
    defer p.deinit();

    const first = try p.getPage(constants.ROOT_PAGE_ID);
    const second = try p.getPage(constants.ROOT_PAGE_ID);

    try std.testing.expect(first == second);
    try std.testing.expectEqual(@as(usize, 2), p.cache_index.count());
}

test "pager: flush prunes clean cache entries to configured limit" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_cache_prune.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path, .always);
    defer p.deinit();

    var pages: [PAGE_CACHE_LIMIT + 4]*Page = undefined;
    for (&pages, 0..) |*slot, index| {
        const page = try p.allocatePage();
        page.asSlice()[0] = @as(u8, @intCast(index));
        page.markDirty();
        slot.* = page;
    }

    try std.testing.expectEqual(@as(usize, PAGE_CACHE_LIMIT + 6), p.cache.items.len);
    try p.flush();

    try std.testing.expectEqual(PAGE_CACHE_LIMIT, p.cache.items.len);
    try std.testing.expectEqual(PAGE_CACHE_LIMIT, p.cache_index.count());
    try std.testing.expect(try p.getPage(constants.META_PAGE_ID) == try p.getPage(constants.META_PAGE_ID));
    try std.testing.expect(try p.getPage(constants.ROOT_PAGE_ID) == try p.getPage(constants.ROOT_PAGE_ID));
}

test "pager: flush keeps dirty pages resident until they become clean" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_cache_dirty.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path, .always);
    defer p.deinit();

    var pages: [PAGE_CACHE_LIMIT + 4]*Page = undefined;
    for (&pages) |*slot| {
        slot.* = try p.allocatePage();
    }

    const recycled_page_id = pages[pages.len - 1].id;

    try p.flush();
    try std.testing.expectEqual(PAGE_CACHE_LIMIT, p.cache.items.len);

    const dirty_page = try p.getPage(recycled_page_id);
    dirty_page.asSlice()[0] = 99;
    dirty_page.markDirty();

    try std.testing.expectEqual(@as(usize, PAGE_CACHE_LIMIT + 1), p.cache.items.len);
    try p.flush();

    try std.testing.expectEqual(PAGE_CACHE_LIMIT, p.cache.items.len);
    const reloaded = try p.getPage(recycled_page_id);
    try std.testing.expectEqual(@as(u8, 99), reloaded.asSlice()[0]);
}

test "pager: flush keeps reserved pages resident" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_cache_reserved.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    var p = try initTestPager(allocator, test_path, .always);
    defer p.deinit();

    const meta_page = try p.getPage(constants.META_PAGE_ID);
    const root_page = try p.getPage(constants.ROOT_PAGE_ID);

    var pages: [PAGE_CACHE_LIMIT + 4]*Page = undefined;
    for (&pages) |*slot| {
        slot.* = try p.allocatePage();
    }

    try p.flush();

    try std.testing.expect(meta_page == try p.getPage(constants.META_PAGE_ID));
    try std.testing.expect(root_page == try p.getPage(constants.ROOT_PAGE_ID));
}

test "pager: freelist survives reopen" {
    const allocator = std.testing.allocator;
    const test_path = "test_pager_freelist_reopen.db";
    defer std.fs.cwd().deleteFile(test_path) catch {};

    {
        var p = try initTestPager(allocator, test_path, .always);
        defer p.deinit();

        const reusable = try p.allocatePage();
        _ = try p.allocatePage();
        try p.freePage(reusable.id);
        try p.flush();
    }

    {
        var p = try initTestPager(allocator, test_path, .always);
        defer p.deinit();

        const recycled = try p.allocatePage();
        try std.testing.expectEqual(@as(PageId, 2), recycled.id);
        try std.testing.expectEqual(@as(PageId, 4), p.pageCount());

        const metadata = try p.readMetadata();
        try std.testing.expectEqual(INVALID_PAGE_ID, metadata.freelist_page);
        try std.testing.expectEqual(@as(PageId, 3), metadata.last_page_id);
    }
}
