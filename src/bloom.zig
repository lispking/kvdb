const std = @import("std");

/// Bloom filter configuration.
const FNV_OFFSET_BASIS: u64 = 14695981039346656037;
const FNV_PRIME: u64 = 1099511628211;

/// Simple Bloom filter with two hash functions (double-hashing).
///
/// Uses a fixed-size bit array stored inline. Designed to fit within
/// a page's free space without requiring heap allocation.
pub fn BloomFilter(comptime num_bits: comptime_int) type {
    return struct {
        /// Bit array stored as bytes.
        bits: [byteCount(num_bits)]u8,

        fn byteCount(n: comptime_int) usize {
            return (n + 7) / 8;
        }

        /// Initialize an empty bloom filter.
        pub fn init() @This() {
            return .{
                .bits = [_]u8{0} ** byteCount(num_bits),
            };
        }

        fn hash1(key: []const u8) u64 {
            return std.hash.Fnv1a.hash(key);
        }

        fn hash2(key: []const u8) u64 {
            var h: u64 = FNV_OFFSET_BASIS;
            for (key) |b| {
                h ^= b;
                h *%= FNV_PRIME;
            }
            h ^= 0x5555555555555555;
            h *%= FNV_PRIME;
            return h;
        }

        /// Add a key to the bloom filter.
        pub fn add(self: *@This(), key: []const u8) void {
            const h1 = hash1(key);
            const h2 = hash2(key);

            var i: u64 = 0;
            while (i < 2) : (i += 1) {
                const combined = h1 +% i *% h2;
                const bit_idx = combined % num_bits;
                const byte_idx = bit_idx / 8;
                const bit_offset = @as(u3, @intCast(bit_idx % 8));
                self.bits[byte_idx] |= @as(u8, 1) << bit_offset;
            }
        }

        /// Check if a key might be in the filter.
        /// Returns false if definitely not present, true if possibly present.
        pub fn mightContain(self: *@This(), key: []const u8) bool {
            const h1 = hash1(key);
            const h2 = hash2(key);

            var i: u64 = 0;
            while (i < 2) : (i += 1) {
                const combined = h1 +% i *% h2;
                const bit_idx = combined % num_bits;
                const byte_idx = bit_idx / 8;
                const bit_offset = @as(u3, @intCast(bit_idx % 8));
                if (self.bits[byte_idx] & (@as(u8, 1) << bit_offset) == 0) {
                    return false;
                }
            }
            return true;
        }

        /// Clear the filter.
        pub fn clear(self: *@This()) void {
            @memset(&self.bits, 0);
        }
    };
}
