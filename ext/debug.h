#include <stdio.h>

#ifdef HAVE_DEBUG
#define ZKLOCK_DEBUG(fmt, ...) fprintf(stderr, "[DEBUG] "); fprintf(stderr, fmt, ##__VA_ARGS__)
#else
#define ZKLOCK_DEBUG(fmt, ...)
#endif
