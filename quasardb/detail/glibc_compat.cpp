// TEMPORARY(QDB-20060): remove once quasardb-build runs on RHEL8 (glibc 2.28).
// When the archive is compiled on the same glibc the wheel is linked on, no
// symbol version can move between the two and this file has no effect.
//
// Linux wheels link libqdb_api.a, which quasardb-build compiles on glibc 2.26,
// inside a manylinux_2_28 image whose glibc is 2.28. glibc 2.27 gave expf,
// exp2f, logf, log2f and powf a new default symbol version, so a reference the
// archive leaves unresolved binds to GLIBC_2.27 at wheel link time. auditwheel
// then lifts the platform tag from manylinux_2_26 to manylinux_2_27 and the
// wheel no longer installs on glibc 2.26 systems such as Amazon Linux 2.
//
// This file defines those five functions for the extension itself, hidden so
// they are never exported, and forwards each to the version every supported
// glibc exports. Being an object that precedes the archive on the link line,
// it satisfies the archive's references before libm is consulted.
//
// Compiled without LTO (see CMakeLists.txt): .symver renames references in the
// object it appears in, and LTO partitioning could move the definitions below
// into a different object than the directive. Compiled with -fno-builtin so the
// definitions below do not clash with gcc's built-in declarations of the same
// functions, which would discard the hidden visibility.

#if defined(__x86_64__)
#    define QDB_GLIBC_BASE "GLIBC_2.2.5"
#elif defined(__aarch64__)
#    define QDB_GLIBC_BASE "GLIBC_2.17"
#else
#    error "glibc_compat.cpp: unsupported Linux architecture"
#endif

extern "C" {
float qdb_base_expf(float);
float qdb_base_exp2f(float);
float qdb_base_logf(float);
float qdb_base_log2f(float);
float qdb_base_powf(float, float);
}

__asm__(".symver qdb_base_expf, expf@" QDB_GLIBC_BASE);
__asm__(".symver qdb_base_exp2f, exp2f@" QDB_GLIBC_BASE);
__asm__(".symver qdb_base_logf, logf@" QDB_GLIBC_BASE);
__asm__(".symver qdb_base_log2f, log2f@" QDB_GLIBC_BASE);
__asm__(".symver qdb_base_powf, powf@" QDB_GLIBC_BASE);

#define QDB_HIDDEN __attribute__((visibility("hidden")))

extern "C" {

QDB_HIDDEN float expf(float x)
{
    return qdb_base_expf(x);
}

QDB_HIDDEN float exp2f(float x)
{
    return qdb_base_exp2f(x);
}

QDB_HIDDEN float logf(float x)
{
    return qdb_base_logf(x);
}

QDB_HIDDEN float log2f(float x)
{
    return qdb_base_log2f(x);
}

QDB_HIDDEN float powf(float x, float y)
{
    return qdb_base_powf(x, y);
}

} // extern "C"
