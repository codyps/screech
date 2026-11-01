/* Linux test-only interposer. Never linked into screech-server. */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static atomic_int fired;
static int matches(int fd, int sync_call) {
    const char *mode = getenv("SCREECH_IO_MODE"), *arm = getenv("SCREECH_IO_ARM");
    if (!mode || !arm || (sync_call != (strncmp(mode, "sync-", 5) == 0)) || access(arm, F_OK)) return 0;
    char link[64], path[4096];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t size = readlink(link, path, sizeof(path)-1);
    if (size < 0) return 0;
    path[size] = 0;
    const char *target = getenv("SCREECH_IO_TARGET");
    const char *suffix = target && !strcmp(target, "db") ? "data.db" : "data.db-wal";
    size_t length = strlen(suffix);
    if ((size_t)size < length || strcmp(path+size-length, suffix)) return 0;
    int expected = 0;
    return atomic_compare_exchange_strong(&fired, &expected, 1);
}
static void mark(const char *operation) {
    const char *path = getenv("SCREECH_IO_HIT");
    if (!path) _exit(97);
    int fd = open(path, O_WRONLY|O_CREAT|O_TRUNC, 0600);
    if (fd < 0 || write(fd, operation, strlen(operation)) < 0) _exit(98);
    close(fd);
}
int fsync(int fd) {
    int (*real_call)(int) = dlsym(RTLD_NEXT, "fsync");
    if (!matches(fd, 1)) return real_call(fd);
    const char *mode = getenv("SCREECH_IO_MODE");
    if ((!strcmp(mode, "sync-after-eio") || !strcmp(mode, "sync-hang-after")) && real_call(fd)) return -1;
    mark("fsync");
    if (!strncmp(mode, "sync-hang-", 10)) for (;;) pause();
    errno = EIO; return -1;
}
int fdatasync(int fd) {
    int (*real_call)(int) = dlsym(RTLD_NEXT, "fdatasync");
    if (!matches(fd, 1)) return real_call(fd);
    const char *mode = getenv("SCREECH_IO_MODE");
    if ((!strcmp(mode, "sync-after-eio") || !strcmp(mode, "sync-hang-after")) && real_call(fd)) return -1;
    mark("fdatasync");
    if (!strncmp(mode, "sync-hang-", 10)) for (;;) pause();
    errno = EIO; return -1;
}
ssize_t pwrite64(int fd, const void *data, size_t count, off64_t offset) {
    ssize_t (*real_call)(int, const void*, size_t, off64_t) = dlsym(RTLD_NEXT, "pwrite64");
    if (!matches(fd, 0)) return real_call(fd, data, count, offset);
    ssize_t n = real_call(fd, data, count > 1 ? count/2 : count, offset);
    mark("pwrite64");
    if (!strcmp(getenv("SCREECH_IO_MODE"), "short-write")) return n;
    errno = ENOSPC; return -1;
}
ssize_t pwrite(int fd, const void *data, size_t count, off_t offset) {
    ssize_t (*real_call)(int, const void*, size_t, off_t) = dlsym(RTLD_NEXT, "pwrite");
    if (!matches(fd, 0)) return real_call(fd, data, count, offset);
    ssize_t n = real_call(fd, data, count > 1 ? count/2 : count, offset);
    mark("pwrite");
    if (!strcmp(getenv("SCREECH_IO_MODE"), "short-write")) return n;
    errno = ENOSPC; return -1;
}
