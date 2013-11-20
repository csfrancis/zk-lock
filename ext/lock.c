#include "ruby.h"

#include "zklock.h"
#include "connection.h"
#include "lock.h"
#include "logging.h"

#include <errno.h>
#include <sys/select.h>
#include <unistd.h>

static const char * kLockIvarData = "@_lock_data";
static const char * kLockIvarConnection = "@_connection";

#define ZKL_GETLOCK() struct lock_data *lock; \
  Data_Get_Struct(rb_iv_get(self, kLockIvarData), struct lock_data, lock);
#define ZKL_GETLOCKCONNECTION() struct connection_data *conn; \
  Data_Get_Struct(rb_iv_get(self, kLockIvarConnection), struct connection_data, conn);
#define ZKL_LOCKERROR(rc) handle_lock_error(rc, lock); break;

static void cb_zk_get_children(int rc, const struct String_vector *strings, const void *data);

static inline int64_t get_lock_sequence(const char *path) {
  return strtoll(strrchr(path, '-') + 1, NULL, 10);
}

static inline const char * get_path_name(const char *path) {
  return strrchr(path, '/') + 1;
}

static void zkl_lock_send_command(enum zklock_command_type cmd_type, struct lock_data *lock) {
  struct zklock_command cmd;
  cmd.cmd = cmd_type;
  cmd.x.lock = lock;
  zkl_connection_send_command(lock->conn, &cmd);
}

static int zkl_lock_get_children(struct lock_data *lock) {
  char *p;
  int ret;

  p = strrchr(lock->path, '/');
  if (!p) {
    ZKL_DEBUG("could not find trailing '/' in lock path %s", lock->path);
    return ZSYSTEMERROR;
  }
  *p = '\0';
  lock->state = ZKLOCK_STATE_GET_CHILDREN;
  ret = zoo_aget_children(lock->conn->zk, lock->path, 0, cb_zk_get_children, lock);
  *p = '/';
  return ret;
}

static void set_lock_state_and_signal(enum zklock_state state, struct lock_data *lock) {
  if (exiting_) return;
  pthread_mutex_lock(&lock->mutex);
  ZKL_DEBUG("setting lock state %d on lock %p", state, lock);
  lock->state = state;
  pthread_cond_broadcast(&lock->cond);
  pthread_mutex_unlock(&lock->mutex);
}

static void handle_lock_error(int rc, struct lock_data *lock) {
  lock->err = rc;
  if (lock->state >= ZKLOCK_STATE_CREATE_PATH) {
    zkl_connection_decr_locks(lock->conn);
  }
  set_lock_state_and_signal(ZKLOCK_STATE_ERROR, lock);
}

static void cb_zk_exists_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
  int ret;
  struct lock_data *lock = (struct lock_data *) watcherCtx;

  if (type == ZOO_CHANGED_EVENT || type == ZOO_DELETED_EVENT || type == ZOO_NOTWATCHING_EVENT) {
    ZKL_DEBUG("received watcher event %d for %s", type, path);
    if (lock->state == ZKLOCK_STATE_WATCHING) {
      ret = zkl_lock_get_children(lock);
      if (ret != ZOK) {
        handle_lock_error(ret, lock);
      }
    } else {
      ZKL_DEBUG("lock %p, state=%d is no longer interested in events", lock, lock->state);
    }
  } else if (type == ZOO_SESSION_EVENT) {
    ZKL_DEBUG("session has been lost");
    handle_lock_error(ZSYSTEMERROR, lock);
  }
}

static void cb_zk_exists(int rc, const struct Stat *stat, const void *data) {
  struct lock_data *lock = (struct lock_data *) data;

  ZKL_DEBUG("%d, data=%p", rc, data);

  if (lock->state != ZKLOCK_STATE_WATCHING) {
    handle_lock_error(ZSYSTEMERROR, lock);
    return;
  }

  if (rc == ZNONODE) {
    rc = zkl_lock_get_children(lock);
    if (rc != ZOK) {
      handle_lock_error(rc, lock);
    }
  }
}

static void cb_zk_get_children(int rc, const struct String_vector *strings, const void *data) {
  int32_t i;
  int ret, owns_lock = 0;
  int64_t seq, watch_seq = 0;
  char *path = NULL;
  size_t path_len;
  const char *path_name, *watch_node;
  struct lock_data *lock = (struct lock_data *) data;

  ZKL_DEBUG("%d, num_strings=%d, data=%p", rc, strings->count, data);

  switch (lock->state) {
  case ZKLOCK_STATE_GET_CHILDREN:
    owns_lock = 1;
    for (i = 0; i < strings->count; i++) {
      path_name = strings->data[i];
      seq = get_lock_sequence(path_name);
      if (seq == lock->seq) continue;
      if (lock->type == ZKLOCK_SHARED) {
        if (strstr(path_name, "write-") == path_name && seq < lock->seq) {
          owns_lock = 0;
        }
      } else {
        if (seq < lock->seq) {
          owns_lock = 0;
        }
      }

      if (!owns_lock && seq > watch_seq) {
        watch_seq = seq;
        watch_node = path_name;
      }
    }

    if (owns_lock) {
      set_lock_state_and_signal(ZKLOCK_STATE_LOCKED, lock);
      break;
    }

    if (!lock->should_block) {
      set_lock_state_and_signal(ZKLOCK_STATE_LOCK_WOULD_BLOCK, lock);
      break;
    }

    path_len = strlen(lock->path) + strlen(watch_node);
    path = (char *) malloc(path_len);
    if (!path) {
      ZKL_LOCKERROR(ZSYSTEMERROR);
    }
    strncpy(path, lock->path, path_len);
    *(strrchr(path, '/') + 1) = '\0';
    strcat(path, watch_node);

    lock->state = ZKLOCK_STATE_WATCHING;
    ret = zoo_awexists(lock->conn->zk, path, cb_zk_exists_watcher, lock, cb_zk_exists, lock);
    if (ret != ZOK) {
      ZKL_LOCKERROR(ret);
    }
    break;
  default:
    ZKL_LOCKERROR(rc);
  }
}

static void cb_zk_create(int rc, const char *value, const void *data) {
  char *p;
  int ret;
  struct lock_data *lock = (struct lock_data *) data;

  ZKL_DEBUG("%d, value=%s, data=%p", rc, value, data);
  switch (lock->state) {
  case ZKLOCK_STATE_CREATE_NODE:
    switch (rc) {
    case ZOK:
      ZKL_DEBUG("created lock node %s", value);
      free(lock->path);
      lock->path = strdup(value);

      lock->seq = get_lock_sequence(lock->path);
      ZKL_DEBUG("initialized lock with sequence: %" PRId64, lock->seq);

      ret = zkl_lock_get_children(lock);
      if (ret != ZOK) {
        ZKL_LOCKERROR(ret);
      }
      break;
    case ZNONODE:
      lock->state = ZKLOCK_STATE_CREATE_PATH;
      lock->create_path = strdup(lock->path);

      p = strchr(lock->create_path + 1, '/'); /* skip the first / in the path */
      if (!p) {
        ZKL_LOCKERROR(ZSYSTEMERROR);
      }
      *p = '\0';
      ZKL_DEBUG("ZNONODE when creating lock - creating parent %s", lock->path);
      ret = zoo_acreate(lock->conn->zk, lock->create_path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, cb_zk_create, lock);
      if (ret != ZOK) {
        ZKL_LOCKERROR(ret);
      }
      break;
    default:
      ZKL_LOCKERROR(rc);
    }
    break;
  case ZKLOCK_STATE_CREATE_PATH:
    switch (rc) {
    case ZOK:
    case ZNODEEXISTS:
      if (rc == ZNODEEXISTS) {
        value = lock->create_path;
      }

      if (strstr(lock->path, value) != lock->path) {
        ZKL_DEBUG("could not find %s in lock path %s", value, lock->path);
        ZKL_LOCKERROR(ZSYSTEMERROR);
      }

      p = lock->create_path + strlen(lock->create_path);
      *p = '/';
      p = strchr(p + 1, '/');
      if (p == NULL) {
        lock->state = ZKLOCK_STATE_CREATE_NODE;
        ZKL_DEBUG("creating lock node %s", lock->path);
        ret = zoo_acreate(lock->conn->zk, lock->path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL | ZOO_SEQUENCE,
                    cb_zk_create, lock);
      } else {
        *p = '\0';
        ZKL_DEBUG("creating path segment %s", lock->create_path);
        ret = zoo_acreate(lock->conn->zk, lock->create_path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE, 0, cb_zk_create, lock);
      }

      if (ret != ZOK) {
        ZKL_LOCKERROR(ret);
      }
      break;
    default:
      ZKL_LOCKERROR(rc);
    }
    break;
  default:
    break;
  }
}

void cb_zk_delete(int rc, const void *data) {
  struct lock_data *lock = (struct lock_data *) data;
  ZKL_DEBUG("%d, data=%p", rc, data);
  set_lock_state_and_signal(ZKLOCK_STATE_UNLOCKED, lock);
}

static void zkl_cleanup_lock_node(struct lock_data *lock) {
  int ret;

  if (lock->state > ZKLOCK_STATE_CREATE_NODE) {
    ZKL_DEBUG("deleting lock node %s", lock->path);
    ret = zoo_adelete(lock->conn->zk, lock->path, -1, cb_zk_delete, lock);
    if (ret != ZOK) {
      handle_lock_error(ret, lock);
      return;
    }
  }

  zkl_connection_decr_locks(lock->conn);
}

void zkl_lock_process_lock_command(struct zklock_command *cmd) {
  struct lock_data *lock = cmd->x.lock;
  struct connection_data *conn = lock->conn;

  if (lock->state == ZKLOCK_STATE_CREATE_NODE) {
    ZKL_DEBUG("creating lock node %s", lock->path);
    int ret = zoo_acreate(conn->zk, lock->path, NULL, 0, &ZOO_OPEN_ACL_UNSAFE,
      ZOO_EPHEMERAL | ZOO_SEQUENCE, cb_zk_create, lock);
    if (ret != ZOK) {
      handle_lock_error(ret, lock);
    }
    return;
  }
}

void zkl_lock_process_unlock_command(struct zklock_command *cmd) {
  struct lock_data *lock = cmd->x.lock;
  zkl_cleanup_lock_node(lock);
}

static void lock_data_free(void *p) {
  struct lock_data *lock = (struct lock_data *) p;
  ZKL_DEBUG("freeing lock: %p", p);
  free(lock->path);
  if (lock->create_path) free(lock->create_path);
  if (lock->state > ZKLOCK_STATE_UNLOCKED && lock->state != ZKLOCK_STATE_UNLOCKING) {
    ZKL_LOG("lock %p (%s) in state %d is being gc'd before being unlocked!", lock,
      lock->path, lock->state);
    zkl_connection_decr_locks(lock->conn);
  }
  pthread_cond_destroy(&lock->cond);
  pthread_mutex_destroy(&lock->mutex);
  free(lock);
}

static int zkl_lock_unlocked(struct lock_data *lock) {
  return lock->state == ZKLOCK_STATE_UNLOCKED;
}

static int zkl_lock_locked(struct lock_data *lock) {
  return lock->state == ZKLOCK_STATE_LOCKED;
}

static VALUE lock_initialize(int argc, VALUE *argv, VALUE self) {
  size_t path_len;
  struct lock_data *lock = NULL;
  enum zklock_type lock_type;
  VALUE class_name;

  if (argc < 2 || TYPE(argv[0]) != RUBY_T_STRING || RSTRING_LEN(argv[0]) == 0 || RSTRING_PTR(argv[0])[0] != '/') {
    rb_raise(rb_eArgError, "lock must be initialized with a valid zookeeper path and connection");
  }

  if (TYPE(argv[1]) != RUBY_T_DATA
    || TYPE(rb_equal(rb_class_of(argv[1]), zklock_connection_class_)) != RUBY_T_TRUE) {
    ZKL_DEBUG("arg[0]: 0x%x %s", TYPE(argv[1]), RSTRING_PTR(rb_class_name(rb_class_of(argv[1]))));
    rb_raise(rb_eArgError, "lock must be initialized with a connection");
  }

  class_name = rb_class_name(rb_class_of(self));
  if (strcmp(RSTRING_PTR(class_name), "ZKLock::SharedLock") == 0)
    lock_type = ZKLOCK_SHARED;
  else if (strcmp(RSTRING_PTR(class_name), "ZKLock::ExclusiveLock") == 0)
    lock_type = ZKLOCK_EXCLUSIVE;
  else
    rb_raise(rb_eRuntimeError, "lock_initialize() called with class: %s", RSTRING_PTR(class_name));

  ZKL_CALLOC(lock, struct lock_data);
  lock->type = lock_type;
  path_len = RSTRING_LEN(argv[0]) + 16;
  ZKL_CALLOCB(lock->path, path_len, char *);
  snprintf(lock->path, path_len, "%s/%s-", RSTRING_PTR(argv[0]), lock_type == ZKLOCK_SHARED ? "read" : "write");
  pthread_mutex_init(&lock->mutex, NULL);
  pthread_cond_init(&lock->cond, NULL);
  Data_Get_Struct(argv[1], struct connection_data, lock->conn);
  lock->state = ZKLOCK_STATE_UNLOCKED;
  rb_iv_set(self, kLockIvarData, Data_Wrap_Struct(rb_cObject, NULL, lock_data_free, lock));
  rb_iv_set(self, kLockIvarConnection, argv[1]);

  return self;
}

static VALUE lock_lock(int argc, VALUE *argv, VALUE self) {
  struct timespec ts = { 0 };
  enum zklock_state state;
  int blocking = 0;
  ZKL_GETLOCK();
  ZKL_GETLOCKCONNECTION();

  if (!zkl_lock_unlocked(lock)) {
    rb_raise(zklock_exception_, "lock is already locked or pending");
  }

  if (argc == 1 && TYPE(argv[0]) == T_HASH) {
    VALUE blocking_ref;
    get_timeout_from_hash(argv[0], 0, &ts);

    blocking_ref = rb_hash_aref(argv[0], ID2SYM(rb_intern("blocking")));
    if (TYPE(blocking_ref) != T_NIL) {
      if (TYPE(blocking_ref) != T_TRUE && TYPE(blocking_ref) != T_FALSE) {
        rb_raise(rb_eArgError, "blocking value must be a boolean");
      }
      blocking = TYPE(blocking_ref) == T_TRUE ? 1 : 0;
    }
  }

  if (!zkl_connection_valid(conn)) {
    zkl_connection_connect(conn);
  }

  if (!zkl_connection_connected(conn)) {
    pthread_mutex_lock(&conn->mutex);
    if (!zkl_connection_connected(conn)) {
      do {
        int ret = zkl_wait_for_connection(conn, &ts);
        if (ret == ETIMEDOUT && !zkl_connection_connected(conn)) {
          pthread_mutex_unlock(&conn->mutex);
          rb_raise(zklock_timeout_error_, "connect timed out");
        }
      } while (zkl_connection_valid(conn) && !zkl_connection_connected(conn));

      if (!zkl_connection_valid(conn)) {
        pthread_mutex_unlock(&conn->mutex);
        rb_raise(zklock_exception_, "unable to connect to server");
      }
    }
    pthread_mutex_unlock(&conn->mutex);
  }

  lock->state = ZKLOCK_STATE_CREATE_NODE;
  lock->should_block = blocking;
  zkl_connection_incr_locks(conn);
  zkl_lock_send_command(ZKLCMD_LOCK, lock);

  pthread_mutex_lock(&lock->mutex);
  while (lock->state != ZKLOCK_STATE_LOCKED && lock->state != ZKLOCK_STATE_LOCK_WOULD_BLOCK) {
    int ret = zkl_wait_for_notification(&lock->mutex, &lock->cond, &ts);
    state = lock->state;
    if (ret == ETIMEDOUT && state != ZKLOCK_STATE_LOCKED && state != ZKLOCK_STATE_LOCK_WOULD_BLOCK) {
      pthread_mutex_unlock(&lock->mutex);

      zkl_cleanup_lock_node(lock);

      if (state == ZKLOCK_STATE_ERROR)
        rb_raise(zklock_exception_, "error acquiring lock");
      else
        rb_raise(zklock_timeout_error_, "lock acquire timed out");
    }
  }
  pthread_mutex_unlock(&lock->mutex);

  if (state != ZKLOCK_STATE_LOCKED) {
    zkl_lock_send_command(ZKLCMD_UNLOCK, lock);
  }

  return state == ZKLOCK_STATE_LOCKED ? Qtrue : Qfalse;
}

static VALUE lock_locked(VALUE self) {
  ZKL_GETLOCK();
  return zkl_lock_locked(lock) ? Qtrue : Qfalse;
}

static VALUE lock_unlock(int argc, VALUE *argv, VALUE self) {
  int64_t timeout = 0;
  struct timespec ts = { 0 };
  ZKL_GETLOCK();

  if (lock->state != ZKLOCK_STATE_LOCKED) {
    return Qfalse;
  }

  if (argc == 1 && TYPE(argv[0]) == T_HASH) {
    timeout = get_timeout_from_hash(argv[0], 1, &ts);
  }

  lock->state = ZKLOCK_STATE_UNLOCKING;
  zkl_lock_send_command(ZKLCMD_UNLOCK, lock);

  if (timeout != 0) {
    pthread_mutex_lock(&lock->mutex);
    while (lock->state != ZKLOCK_STATE_UNLOCKED) {
      int ret = zkl_wait_for_notification(&lock->mutex, &lock->cond, &ts);
      if (ret == ETIMEDOUT && lock->state != ZKLOCK_STATE_UNLOCKED) {
        pthread_mutex_unlock(&lock->mutex);
        if (lock->state == ZKLOCK_STATE_ERROR)
          rb_raise(zklock_exception_, "error unlocking lock");
        else
          rb_raise(zklock_timeout_error_, "lock unlock timed out");
      }
    }
    pthread_mutex_unlock(&lock->mutex);
  }

  return Qtrue;
}

void define_lock_methods(VALUE klass) {
  rb_define_method(klass, "initialize", lock_initialize, -1);
  rb_define_method(klass, "lock", lock_lock, -1);
  rb_define_method(klass, "locked?", lock_locked, 0);
  rb_define_method(klass, "unlock", lock_unlock, -1);
}
