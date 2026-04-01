const std = @import("std");
const kvdb = @import("kvdb");

const Database = kvdb.Database;

/// Print usage information for the CLI tool.
///
/// Displays available commands and their syntax.
fn printUsage() void {
    std.debug.print(
        \\Usage: kvdb-cli <database-file> <command> [args...]
        \\
        \\Commands:
        \\  get <key>              Get value by key
        \\  put <key> <value>      Set key-value pair
        \\  delete <key>           Delete key
        \\  list                   List all key-value pairs
        \\  stats                  Show database statistics
        \\  compact                Compact database (remove deleted entries)
        \\
    , .{});
}

/// Main entry point for the CLI application.
///
/// Parses command line arguments and dispatches to appropriate handlers.
pub fn main() !void {
    // Initialize GPA allocator for memory management
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse command line arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // Validate argument count
    if (args.len < 3) {
        printUsage();
        return;
    }

    const db_path = args[1];
    const command = args[2];

    // Open database
    var db = Database.open(allocator, db_path, .{}) catch |err| {
        std.debug.print("Failed to open database: {s}\n", .{@errorName(err)});
        return;
    };
    defer db.close();

    // Dispatch to command handler
    if (std.mem.eql(u8, command, "get")) {
        // Handle: get <key>
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> get <key>\n", .{});
            return;
        }
        const key = args[3];
        const value = try db.get(key);
        if (value) |v| {
            defer allocator.free(v);
            std.debug.print("{s}\n", .{v});
        } else {
            std.debug.print("Key not found: {s}\n", .{key});
        }
    } else if (std.mem.eql(u8, command, "put")) {
        // Handle: put <key> <value>
        if (args.len < 5) {
            std.debug.print("Usage: kvdb-cli <db> put <key> <value>\n", .{});
            return;
        }
        const key = args[3];
        const value = args[4];
        try db.put(key, value);
        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "delete")) {
        // Handle: delete <key>
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> delete <key>\n", .{});
            return;
        }
        const key = args[3];
        try db.delete(key);
        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "list")) {
        // Handle: list (enumerate all entries)
        var iter = try db.iterator();
        var count: usize = 0;
        while (iter.next()) |entry| {
            std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
            count += 1;
        }
        std.debug.print("\nTotal: {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "stats")) {
        // Handle: stats (show database info)
        const stats = db.stats();
        std.debug.print("Database Statistics:\n", .{});
        std.debug.print("  Pages: {d}\n", .{stats.page_count});
        std.debug.print("  Page Size: {d} bytes\n", .{stats.page_size});
        std.debug.print("  Database Size: {d} bytes ({d:.2} MB)\n", .{
            stats.db_size,
            @as(f64, @floatFromInt(stats.db_size)) / (1024 * 1024),
        });
    } else {
        // Unknown command
        std.debug.print("Unknown command: {s}\n", .{command});
        printUsage();
    }
}
