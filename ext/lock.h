#ifndef __LOCK_H__
#define __LOCK_H__

#include <pthread.h>

#include "zklock.h"
#include "connection.h"

enum zklock_state {
  ZKLOCK_STATE_ERROR = -1,
  ZKLOCK_STATE_UNLOCKED,
  ZKLOCK_STATE_CREATE_PATH,
  ZKLOCK_STATE_CREATE_NODE,
  ZKLOCK_STATE_GET_CHILDREN,
  ZKLOCK_STATE_WATCHING,
  ZKLOCK_STATE_UNLOCKING,
  ZKLOCK_STATE_LOCK_WOULD_BLOCK,
  ZKLOCK_STATE_LOCKED
};

struct lock_data {
  int type;
  int ref_count;
  enum zklock_state state;
  char *path;
  char *create_path;
  int64_t seq;
  int should_block;
  struct connection_data *conn;
  int err;
  union {
    struct {
      long num_slaves;
      struct lock_data *slaves;
      pthread_mutex_t mutex;
      pthread_cond_t cond;
    } master;
    struct {
      struct lock_data *master;
      pthread_mutex_t *mutex;
      pthread_cond_t *cond;
    } slave;
  };
};

void zkl_lock_process_lock_command(struct zklock_command *cmd);
void zkl_lock_process_unlock_command(struct zklock_command *cmd);
void define_lock_methods(VALUE klass);

#endif /* __LOCK_H__ */
