#ifndef __LOCK_H__
#define __LOCK_H__

#include <pthread.h>

#include "zklock.h"
#include "connection.h"

enum zklock_type {
  ZKLOCK_SHARED = 0,
  ZKLOCK_EXCLUSIVE
};

enum zklock_state {
  ZKLOCK_STATE_UNLOCKED = 0,
  ZKLOCK_STATE_STARTED_LOCK,
  ZKLOCK_STATE_CREATE_PATH,
  ZKLOCK_STATE_CREATE_NODE,
  ZKLOCK_STATE_GET_CHILDREN,
  ZKLOCK_STATE_WATCHING,
  ZKLOCK_STATE_LOCKED
};

struct lock_data {
  enum zklock_type type;
  char *path;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  struct connection_data *conn;
  enum zklock_state state;
  enum zklock_state desired_state;
};

void zkl_lock_process_lock(struct zklock_command *cmd);
void define_lock_methods(VALUE klass);

#endif /* __LOCK_H__ */
