const std = @import("std");
const kvdb = @import("kvdb");

/// Example demonstrating basic KVDB usage.
///
/// This example shows how to:
/// - Open and close a database
/// - Insert, query, update, and delete key-value pairs
/// - Iterate over all entries
/// - Get database statistics
pub fn main() !void {
    // Initialize GPA allocator for memory management
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Database file path (temporary location)
    const db_path = "/tmp/example_kvdb.db";

    // Cleanup any existing database files from previous runs
    defer std.fs.cwd().deleteFile(db_path) catch {};
    defer std.fs.cwd().deleteFile("/tmp/example_kvdb.db.wal") catch {};

    std.debug.print("=== KVDB Example ===\n\n", .{});

    // ====================================================================
    // Open Database
    // ====================================================================
    var db = try kvdb.open(allocator, db_path);
    defer db.close();
    std.debug.print("Database opened: {s}\n", .{db_path});

    // ====================================================================
    // Insert Data
    // ====================================================================
    std.debug.print("\nInserting data...\n", .{});
    try db.put("language", "Zig");
    try db.put("version", "0.15.0");
    try db.put("project", "KVDB");
    try db.put("author", "Your Name");
    std.debug.print("Inserted 4 key-value pairs\n", .{});

    // ====================================================================
    // Query Data
    // ====================================================================
    std.debug.print("\nQuerying data...\n", .{});
    const keys = [_][]const u8{ "language", "version", "project", "author" };
    for (keys) |key| {
        const value = try db.get(key);
        if (value) |v| {
            // db.get() returns an owned copy, so each successful lookup result
            // must be freed by the caller after printing or otherwise consuming it.
            defer allocator.free(v);
            std.debug.print("  {s} = {s}\n", .{ key, v });
        }
    }

    // ====================================================================
    // Update Data
    // ====================================================================
    std.debug.print("\nUpdating data...\n", .{});
    try db.put("version", "0.15.1");
    const new_version = try db.get("version");
    if (new_version) |v| {
        defer allocator.free(v);
        std.debug.print("  version updated to: {s}\n", .{v});
    }

    // ====================================================================
    // Delete Data
    // ====================================================================
    std.debug.print("\nDeleting 'author'...\n", .{});
    try db.delete("author");
    const author = try db.get("author");
    if (author) |v| {
        defer allocator.free(v);
    } else {
        std.debug.print("  'author' deleted successfully\n", .{});
    }

    // ====================================================================
    // Iterate All Entries
    // ====================================================================
    std.debug.print("\nIterating all entries:\n", .{});
    // The iterator now walks every reachable leaf page in sorted order, so the
    // example should use `try` when advancing through the fallible iterator.
    var iter = try db.iterator();
    var count: usize = 0;
    while (try iter.next()) |entry| {
        std.debug.print("  {s} = {s}\n", .{ entry.key, entry.value });
        count += 1;
    }
    std.debug.print("Total entries: {d}\n", .{count});

    // ====================================================================
    // Database Statistics
    // ====================================================================
    std.debug.print("\nDatabase Statistics:\n", .{});
    const stats = db.stats();
    std.debug.print("  Pages: {d}\n", .{stats.page_count});
    std.debug.print("  Page Size: {d} bytes\n", .{stats.page_size});
    std.debug.print("  Total Size: {d} bytes\n", .{stats.db_size});

    std.debug.print("\n=== Example Complete ===\n", .{});
}
