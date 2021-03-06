#ifndef __ZKLOCK_H__
#define __ZKLOCK_H__

#include "ruby.h"
#include "zookeeper/zookeeper.h"

#include <time.h>

#define ZKL_CALLOC(ptr, type) ptr = (type *) malloc(sizeof(type)); \
  if (ptr == NULL) rb_raise(rb_eNoMemError, "out of memory"); memset(ptr, 0, sizeof(type));
#define ZKL_CALLOCB(ptr, num_bytes, type) ptr = (type) malloc(num_bytes); \
  if (ptr == NULL) rb_raise(rb_eNoMemError, "out of memory"); memset(ptr, 0, num_bytes);
#define ZKL_FREE(ptr) free(ptr); ptr = NULL;

enum zklock_thread_status {
  ZKLTHREAD_STOPPED = 0,
  ZKLTHREAD_STARTING,
  ZKLTHREAD_RUNNING,
  ZKLTHREAD_STOPPING
};

enum zklock_command_type {
  ZKLCMD_TERMINATE = 0,
  ZKLCMD_LOCK,
  ZKLCMD_UNLOCK
};

struct zklock_command {
  enum zklock_command_type cmd;
  union {
    void *lock;
  } x;
};

extern VALUE zklock_connection_class_;
extern VALUE zklock_exception_;
extern VALUE zklock_timeout_error_;

extern int exiting_;

int64_t get_timeout_from_hash(VALUE hash, int allow_zero, struct timespec *ts);

void zkl_zookeeper_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx);
int zkl_wait_for_notification(pthread_mutex_t *mutex, pthread_cond_t *cond, struct timespec *ts);

/* Taken from https://gist.github.com/BinaryPrison/1112092/ */
static inline uint32_t __iter_div_u64_rem(uint64_t dividend, uint32_t divisor, uint64_t *remainder)
{
  uint32_t ret = 0;
  while (dividend >= divisor) {
    /* The following asm() prevents the compiler from
       optimising this loop into a modulo operation.  */
    asm("" : "+rm"(dividend));

    dividend -= divisor;
    ret++;
  }
  *remainder = dividend;
  return ret;
}

#define NSEC_PER_SEC  1000000000L
static inline void timespec_add_ns(struct timespec *a, uint64_t ns)
{
  a->tv_sec += __iter_div_u64_rem(a->tv_nsec + ns, NSEC_PER_SEC, &ns);
  a->tv_nsec = ns;
}

#ifdef __MACH__ /* no clock_gettime() on OSX */
#include <mach/clock.h>
#include <mach/mach.h>

#define CLOCK_REALTIME 1
int clock_gettime(int clk_id, struct timespec *ts);
#endif

#endif /* __ZKLOCK_H__ */
