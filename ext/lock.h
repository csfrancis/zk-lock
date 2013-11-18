#ifndef __LOCK_H__
#define __LOCK_H__

#include <pthread.h>

enum zklock_type {
  ZKLOCK_SHARED = 0,
  ZKLOCK_EXCLUSIVE
};

struct lock_data {
  enum zklock_type type;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
};

void define_lock_methods(VALUE klass);

#endif /* __LOCK_H__ */
