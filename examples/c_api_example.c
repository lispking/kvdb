#include <stdint.h>
#include <stdio.h>
#include <string.h>

struct Database;
typedef struct Database *KVDB_Handle;

enum KVDB_Status {
    KVDB_STATUS_OK = 0,
    KVDB_STATUS_INVALID_ARGUMENT = 1,
    KVDB_STATUS_NOT_FOUND = 2,
    KVDB_STATUS_TRANSACTION_CONFLICT = 3,
    KVDB_STATUS_STORAGE_ERROR = 4,
    KVDB_STATUS_WAL_ERROR = 5,
    KVDB_STATUS_INTERNAL_ERROR = 255,
};

KVDB_Handle kvdb_open(const unsigned char *path, size_t path_len);
void kvdb_close(KVDB_Handle handle);
unsigned char *kvdb_get(KVDB_Handle handle, const unsigned char *key, size_t key_len, size_t *value_len);
void kvdb_free(unsigned char *value, size_t value_len);
int kvdb_put(KVDB_Handle handle, const unsigned char *key, size_t key_len, const unsigned char *value, size_t value_len);
int kvdb_delete(KVDB_Handle handle, const unsigned char *key, size_t key_len);
int kvdb_status_code(enum KVDB_Status status);

int main(void) {
    const char *db_path = "/tmp/kvdb_ffi_example.db";
    KVDB_Handle db = kvdb_open((const unsigned char *)db_path, strlen(db_path));
    if (db == NULL) {
        fprintf(stderr, "failed to open database\n");
        return 1;
    }

    if (kvdb_put(db, (const unsigned char *)"language", strlen("language"), (const unsigned char *)"C", strlen("C")) != kvdb_status_code(KVDB_STATUS_OK)) {
        fprintf(stderr, "failed to put key\n");
        kvdb_close(db);
        return 1;
    }

    size_t value_len = 0;
    unsigned char *value = kvdb_get(db, (const unsigned char *)"language", strlen("language"), &value_len);
    if (value == NULL) {
        fprintf(stderr, "failed to get key\n");
        kvdb_close(db);
        return 1;
    }

    printf("language=%.*s\n", (int)value_len, value);
    kvdb_free(value, value_len);

    if (kvdb_delete(db, (const unsigned char *)"language", strlen("language")) != kvdb_status_code(KVDB_STATUS_OK)) {
        fprintf(stderr, "failed to delete key\n");
        kvdb_close(db);
        return 1;
    }

    if (kvdb_delete(db, (const unsigned char *)"language", strlen("language")) != kvdb_status_code(KVDB_STATUS_NOT_FOUND)) {
        fprintf(stderr, "expected a stable not-found status\n");
        kvdb_close(db);
        return 1;
    }

    kvdb_close(db);
    return 0;
}
