const std = @import("std");
const kvdb = @import("kvdb");
const config = @import("config");

const Database = kvdb.Database;
const InspectStats = kvdb.InspectStats;
const VerifyStats = kvdb.VerifyStats;
const Error = kvdb.Error;

const ErrorContext = enum {
    open,
    get,
    put,
    delete,
    list,
    stats,
    inspect,
    export_cmd,
    import_cmd,
    compact,
    verify,
};

/// Return a user-facing message for a known database or CLI error.
fn userErrorMessage(context: ErrorContext, err: anyerror) ?[]const u8 {
    return switch (err) {
        Error.InvalidArgument => switch (context) {
            .open => "Failed to open database: invalid argument.",
            .get => "Get failed: invalid key.",
            .put => "Put failed: key or value exceeds engine limits, or the key is empty.",
            .delete => "Delete failed: key must not be empty.",
            .list => "List failed: invalid argument.",
            .inspect => "Inspect failed: invalid argument.",
            .export_cmd => "Export failed: invalid argument.",
            .import_cmd => "Import failed: invalid dump format or entry sizes.",
            .compact => "Compact failed: invalid argument.",
            .verify => "Verify failed: invalid argument.",
            else => "Operation failed: invalid argument.",
        },
        Error.KeyNotFound => switch (context) {
            .delete => "Delete failed: key not found.",
            else => "Key not found.",
        },
        Error.NodeEmpty => "Delete failed: this tree shape needs rebalance support before the key can be removed.",
        Error.CorruptedData => switch (context) {
            .inspect => "Inspect failed: database file is corrupted.",
            .verify => "Verification failed: database file is corrupted.",
            else => "Operation failed: database file is corrupted.",
        },
        Error.WalReplayFailed => "Failed to open database: WAL recovery failed. The WAL file may be corrupted; run verify or restore from backup.",
        Error.WalCorrupted => "Operation failed: WAL file is corrupted.",
        Error.TransactionConflict => "Operation failed: transaction conflict.",
        Error.DiskFull => "Operation failed: disk is full.",
        Error.IoError, error.InputOutput => "Operation failed: I/O error.",
        error.FileNotFound => switch (context) {
            .import_cmd => "Import failed: input file not found.",
            else => "Operation failed: file not found.",
        },
        error.AccessDenied => "Operation failed: access denied.",
        else => null,
    };
}

/// Print a user-facing error and fall back to the raw error name when needed.
fn printUserError(context: ErrorContext, err: anyerror) void {
    if (userErrorMessage(context, err)) |message| {
        std.debug.print("{s}\n", .{message});
        return;
    }
    switch (context) {
        .open => std.debug.print("Failed to open database: {s}\n", .{@errorName(err)}),
        .get => std.debug.print("Get failed: {s}\n", .{@errorName(err)}),
        .put => std.debug.print("Put failed: {s}\n", .{@errorName(err)}),
        .delete => std.debug.print("Delete failed: {s}\n", .{@errorName(err)}),
        .list => std.debug.print("List failed: {s}\n", .{@errorName(err)}),
        .stats => std.debug.print("Stats failed: {s}\n", .{@errorName(err)}),
        .inspect => std.debug.print("Inspect failed: {s}\n", .{@errorName(err)}),
        .export_cmd => std.debug.print("Export failed: {s}\n", .{@errorName(err)}),
        .import_cmd => std.debug.print("Import failed: {s}\n", .{@errorName(err)}),
        .compact => std.debug.print("Compact failed: {s}\n", .{@errorName(err)}),
        .verify => std.debug.print("Verify failed: {s}\n", .{@errorName(err)}),
    }
}

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
        \\  prefix <prefix>        List entries whose keys start with prefix
        \\  range <start> <end>   List entries where start <= key < end
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
        printUserError(.open, err);
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
        const value = db.get(key) catch |err| {
            printUserError(.get, err);
            return;
        };
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
        db.put(key, value) catch |err| {
            printUserError(.put, err);
            return;
        };
        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "delete")) {
        // Handle: delete <key>
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> delete <key>\n", .{});
            return;
        }
        const key = args[3];
        db.delete(key) catch |err| {
            printUserError(.delete, err);
            return;
        };
        std.debug.print("OK\n", .{});
    } else if (std.mem.eql(u8, command, "list")) {
        // Handle: list (enumerate all entries)
        // The iterator now walks every reachable leaf in sorted key order, so
        // CLI listing reflects the full multi-page tree instead of only root data.
        var iter = db.iterator() catch |err| {
            printUserError(.list, err);
            return;
        };
        defer iter.deinit();
        var count: usize = 0;
        while (iter.next() catch |err| {
            printUserError(.list, err);
            return;
        }) |entry| {
            std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
            count += 1;
        }
        std.debug.print("\nTotal: {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "prefix")) {
        // Handle: prefix <prefix> (enumerate keys sharing one prefix)
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> prefix <prefix>\n", .{});
            return;
        }
        const prefix = args[3];
        var iter = db.iterator() catch |err| {
            printUserError(.list, err);
            return;
        };
        defer iter.deinit();
        var count: usize = 0;
        while (iter.next() catch |err| {
            printUserError(.list, err);
            return;
        }) |entry| {
            if (!std.mem.startsWith(u8, entry.key, prefix)) {
                continue;
            }
            std.debug.print("{s} = {s}\n", .{ entry.key, entry.value });
            count += 1;
        }
        std.debug.print("\nTotal: {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "range")) {
        // Handle: range <start> <end> (enumerate a half-open key interval)
        if (args.len < 5) {
            std.debug.print("Usage: kvdb-cli <db> range <start> <end>\n", .{});
            return;
        }
        const start = args[3];
        const end = args[4];
        if (std.mem.order(u8, start, end) == .gt) {
            std.debug.print("Range failed: start key must be <= end key.\n", .{});
            return;
        }
        var iter = db.iterator() catch |err| {
            printUserError(.list, err);
            return;
        };
        defer iter.deinit();
        var count: usize = 0;
        while (iter.next() catch |err| {
            printUserError(.list, err);
            return;
        }) |entry| {
            if (std.mem.order(u8, entry.key, start) == .lt) {
                continue;
            }
            if (std.mem.order(u8, entry.key, end) != .lt) {
                break;
            }
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
        const stats = db.inspect() catch |err| {
            printUserError(.inspect, err);
            return;
        };
        printInspectStats(stats);
    } else if (std.mem.eql(u8, command, "export")) {
        // Handle: export <file> (write all logical entries into a binary dump)
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> export <file>\n", .{});
            return;
        }

        const export_path = args[3];
        const file = std.fs.cwd().createFile(export_path, .{ .truncate = true }) catch |err| {
            printUserError(.export_cmd, err);
            return;
        };
        defer file.close();

        var writer_buffer: [4096]u8 = undefined;
        var writer = file.writer(&writer_buffer);
        const count = db.exportToWriter(&writer.interface) catch |err| {
            printUserError(.export_cmd, err);
            return;
        };
        writer.interface.flush() catch |err| {
            printUserError(.export_cmd, err);
            return;
        };
        file.sync() catch |err| {
            printUserError(.export_cmd, err);
            return;
        };
        std.debug.print("Exported {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "import")) {
        // Handle: import <file> (load logical entries from a binary dump)
        if (args.len < 4) {
            std.debug.print("Usage: kvdb-cli <db> import <file>\n", .{});
            return;
        }

        const import_path = args[3];
        const file = std.fs.cwd().openFile(import_path, .{}) catch |err| {
            printUserError(.import_cmd, err);
            return;
        };
        defer file.close();

        var reader_buffer: [4096]u8 = undefined;
        var reader = file.reader(&reader_buffer);
        const count = db.importFromReader(&reader.interface) catch |err| {
            printUserError(.import_cmd, err);
            return;
        };
        std.debug.print("Imported {d} entries\n", .{count});
    } else if (std.mem.eql(u8, command, "compact")) {
        // Handle: compact (compact the database)
        const new_stats = db.compact() catch |err| {
            printUserError(.compact, err);
            return;
        };
        std.debug.print("Compacted database successfully.\n", .{});
        std.debug.print("New size: {d} pages ({d:.2} MB)\n", .{
            new_stats.page_count,
            @as(f64, @floatFromInt(new_stats.db_size)) / (1024 * 1024),
        });
    } else if (std.mem.eql(u8, command, "verify")) {
        // Handle: verify (validate metadata, tree ordering, freelist, and WAL)
        const stats = db.verify() catch |err| {
            printUserError(.verify, err);
            return;
        };
        printVerifyStats(stats);
    } else {
        // Unknown command
        std.debug.print("Unknown command: {s}\n", .{command});
        printUsage();
    }
}
