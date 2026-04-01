import ctypes
import pathlib
import subprocess
import sys

KVDB_STATUS_OK = 0
KVDB_STATUS_NOT_FOUND = 2


def build_library(repo_root: pathlib.Path) -> pathlib.Path:
    lib_path = repo_root / "zig-out" / "lib" / "libkvdb_python.dylib"
    subprocess.run(
        [
            "zig",
            "build-lib",
            "-dynamic",
            "src/kvdb.zig",
            "-femit-bin=" + str(lib_path),
        ],
        cwd=repo_root,
        check=True,
    )
    return lib_path


def main() -> int:
    repo_root = pathlib.Path(__file__).resolve().parents[1]
    lib_path = build_library(repo_root)
    kvdb = ctypes.CDLL(str(lib_path))

    kvdb.kvdb_open.argtypes = [ctypes.c_char_p, ctypes.c_size_t]
    kvdb.kvdb_open.restype = ctypes.c_void_p

    kvdb.kvdb_close.argtypes = [ctypes.c_void_p]
    kvdb.kvdb_close.restype = None

    kvdb.kvdb_get.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_size_t)]
    kvdb.kvdb_get.restype = ctypes.POINTER(ctypes.c_ubyte)

    kvdb.kvdb_free.argtypes = [ctypes.POINTER(ctypes.c_ubyte), ctypes.c_size_t]
    kvdb.kvdb_free.restype = None

    kvdb.kvdb_put.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t, ctypes.c_char_p, ctypes.c_size_t]
    kvdb.kvdb_put.restype = ctypes.c_int

    kvdb.kvdb_delete.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_size_t]
    kvdb.kvdb_delete.restype = ctypes.c_int

    db_path = b"/tmp/kvdb_python_example.db"
    handle = kvdb.kvdb_open(db_path, len(db_path))
    if not handle:
        print("failed to open database", file=sys.stderr)
        return 1

    try:
        key = b"language"
        value = b"Python"
        status = kvdb.kvdb_put(handle, key, len(key), value, len(value))
        if status != KVDB_STATUS_OK:
            print(f"put failed with status {status}", file=sys.stderr)
            return 1

        value_len = ctypes.c_size_t(0)
        value_ptr = kvdb.kvdb_get(handle, key, len(key), ctypes.byref(value_len))
        if not value_ptr:
            print("get failed", file=sys.stderr)
            return 1

        try:
            result = bytes(ctypes.string_at(value_ptr, value_len.value))
            print(f"language={result.decode('utf-8')}")
        finally:
            kvdb.kvdb_free(value_ptr, value_len.value)

        status = kvdb.kvdb_delete(handle, key, len(key))
        if status != KVDB_STATUS_OK:
            print(f"delete failed with status {status}", file=sys.stderr)
            return 1

        status = kvdb.kvdb_delete(handle, key, len(key))
        if status != KVDB_STATUS_NOT_FOUND:
            print(f"expected not-found status, got {status}", file=sys.stderr)
            return 1
    finally:
        kvdb.kvdb_close(handle)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
