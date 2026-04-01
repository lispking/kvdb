const std = @import("std");
const kvdb = @import("kvdb");
const config = @import("config");

const Database = kvdb.Database;
const InspectStats = kvdb.InspectStats;
const VerifyStats = kvdb.VerifyStats;
const Error = kvdb.Error;

/// Print a compact structural summary for a database file.
fn printInspectStats(stats: InspectStats) void {
    std.debug.print("Database\n", .{});
    std.debug.print("  Pages: {d}\n", .{stats.page_count});
    std.debug.print("  Page Size: {d} bytes\n", .{stats.page_size});
    std.debug.print("  Database Size: {d} bytes ({d:.2} MB)\n", .{
        stats.db_size,
        @as(f64, @floatFromInt(stats.db_size)) / (1024 * 1024),
    });

    std.debug.print("Metadata\n", .{});
    std.debug.print("  Root Page ID: {d}\n", .{stats.root_page_id});
    std.debug.print("  Freelist Head: {d}\n", .{stats.freelist_page});
    std.debug.print("  Freelist Pages: {d}\n", .{stats.freelist_page_count});
    std.debug.print("  Last Page ID: {d}\n", .{stats.last_page_id});
    std.debug.print("  WAL Offset: {d}\n", .{stats.wal_offset});

    std.debug.print("B-Tree\n", .{});
    std.debug.print("  Height: {d}\n", .{stats.tree_height});
    std.debug.print("  Nodes: {d}\n", .{stats.node_count});
    std.debug.print("  Leaf Nodes: {d}\n", .{stats.leaf_count});
    std.debug.print("  Internal Nodes: {d}\n", .{stats.internal_count});
    std.debug.print("  Entries: {d}\n", .{stats.entry_count});
}

/// Print a compact verification summary for a database file.
fn printVerifyStats(stats: VerifyStats) void {
    std.debug.print("Verification OK\n", .{});
    std.debug.print("  Tree pages checked: {d}\n", .{stats.checked_tree_pages});
    std.debug.print("  Entries checked: {d}\n", .{stats.checked_entries});
    std.debug.print("  WAL records checked: {d}\n", .{stats.checked_wal_records});
}

/// Print version information.
fn printVersion() void {
    std.debug.print("kvdb-cli version {s} (git: {s})\n", .{
        config.version,
        config.git_commit,
    });
}

/// Print usage information for the CLI tool.
///
/// Displays available commands and their syntax.
fn printUsage() void {
    std.debug.print(
        \\Usage: kvdb-cli [options] <database-file> <command> [args...]
        \\
        \\Options:
        \\  -h, --help       Show this help message
        \\  -v, --version    Show version information
        \\
        \\Commands:
        \\  get <key>              Get value by key
        \\  put <key> <value>      Set key-value pair
        \\  delete <key>           Delete key
        \\  list                   List all key-value pairs
        \\  stats                  Show database statistics
        \\  inspect                Show metadata and tree summary
        \\  export <file>          Export all entries to a binary file
        \\  import <file>          Import entries from a binary file
        \\  compact                Compact database (remove deleted entries)
        \\  verify                 Verify metadata, tree, and WAL integrity
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
    if (args.len < 2) {
        printUsage();
        return;
    }

    // Check for global options (--version, --help) before database path
    const first_arg = args[1];
    if (std.mem.eql(u8, first_arg, "--version") or std.mem.eql(u8, first_arg, "-v")) {
        printVersion();
        return;
    }
    if (std.mem.eql(u8, first_arg, "--help") or std.mem.eql(u8, first_arg, "-h")) {
        printUsage();
        return;
    }

    // Now we need at least 3 args: program, db_path, command
    if (args.len < 3) {
        printUsage();
        return;
    }

    const db_path = args[1];
    const command = args[2];

    // Open the database with default WAL-backed behavior so CLI commands
    // follow the same persistence and recovery path as library callers.
    var db = Database.open(allocator, db_path, .{}) catch |err| {
        std.debug.print("Failed to open database: {s}\n", .{@errorName(err)});
        return;
    };
    defer db.close();

    // Dispatch to the selected command after global option parsing and
    // database open have already succeeded.
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
        db.delete(key) catch |err| switch (err) {
            Error.NodeEmpty => {
                // Surface the staged multi-page delete limitation explicitly so
                // callers understand this key would require rebalance support.
                std.debug.print("Delete not supported yet for this tree shape: {s}\n", .{@errorName(err)});
                return;
            },
            else => return err,
        };
        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "list")) {
        // Handle: list (enumerate all entries)
        // The iterator now walks every reachable leaf in sorted key order, so
        // CLI listing reflects the full multi-page tree instead of only root data.
        var iter = try db.iterator();
        defer iter.deinit();
        var count: usize = 0;
        while (try iter.next()) |entry| {
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
    } else if (std.mem.eql(u8, command, "inspect")) {
        // Handle: inspect (show metadata plus structural tree summary)
        const stats = try db.inspect();
        printInspectStats(stats);
    } else if (std.mem.eql(u8, command, "export")) {
        // Handle: export <file> (write all logical entries into a binary dump)
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> export <file>\n", .{});
            return;
        }

        const export_path = args[3];
        const file = try std.fs.cwd().createFile(export_path, .{ .truncate = true });
        defer file.close();

        var writer_buffer: [4096]u8 = undefined;
        var writer = file.writer(&writer_buffer);
        const count = try db.exportToWriter(&writer.interface);
        try writer.interface.flush();
        try file.sync();
        std.debug.print("Exported {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "import")) {
        // Handle: import <file> (load logical entries from a binary dump)
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> import <file>\n", .{});
            return;
        }

        const import_path = args[3];
        const file = try std.fs.cwd().openFile(import_path, .{});
        defer file.close();

        var reader_buffer: [4096]u8 = undefined;
        var reader = file.reader(&reader_buffer);
        const count = try db.importFromReader(&reader.interface);
        std.debug.print("Imported {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "compact")) {
        // Handle: compact (compact the database)
        const new_stats = try db.compact();
        std.debug.print("Compacted database successfully.\n", .{});
        std.debug.print("New size: {d} pages ({d:.2} MB)\n", .{
            new_stats.page_count,
            @as(f64, @floatFromInt(new_stats.db_size)) / (1024 * 1024),
        });
    } else if (std.mem.eql(u8, command, "verify")) {
        // Handle: verify (validate metadata, tree ordering, freelist, and WAL)
        const stats = try db.verify();
        printVerifyStats(stats);
    } else {
        // Unknown command
        std.debug.print("Unknown command: {s}\n", .{command});
        printUsage();
    }
}
