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

void zkl_lock_process_lock(struct zklock_command *cmd) {
  struct lock_data *lock = cmd->x.lock;
  usleep(100000);
  pthread_mutex_lock(&lock->mutex);
  ZKL_DEBUG("setting lock flag on lock %p", lock);
  lock->state = ZKLOCK_STATE_LOCKED;
  pthread_cond_broadcast(&lock->cond);
  pthread_mutex_unlock(&lock->mutex);
}

static void lock_data_free(void *p) {
  struct lock_data *lock = (struct lock_data *) p;
  ZKL_DEBUG("freeing lock: %p", p);
  pthread_cond_destroy(&lock->cond);
  pthread_mutex_destroy(&lock->mutex);
  free(lock);
}

static VALUE lock_initialize(int argc, VALUE *argv, VALUE self) {
  struct lock_data *lock = NULL;
  enum zklock_type lock_type;
  VALUE class_name;

  if (argc == 0 || TYPE(argv[0]) != RUBY_T_DATA
    || TYPE(rb_equal(rb_class_of(argv[0]), zklock_connection_class_)) != RUBY_T_TRUE) {
    ZKL_DEBUG("lock_initialize() arg[0]: 0x%x %s", TYPE(argv[0]), RSTRING_PTR(rb_class_name(rb_class_of(argv[0]))));
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
  pthread_mutex_init(&lock->mutex, NULL);
  pthread_cond_init(&lock->cond, NULL);
  Data_Get_Struct(argv[0], struct connection_data, lock->conn);
  lock->state = lock->desired_state = ZKLOCK_STATE_UNLOCKED;
  rb_iv_set(self, kLockIvarData, Data_Wrap_Struct(rb_cObject, NULL, lock_data_free, lock));
  rb_iv_set(self, kLockIvarConnection, argv[0]);

  return self;
}

static VALUE lock_lock(int argc, VALUE *argv, VALUE self) {
  struct zklock_command cmd;
  int64_t timeout = -1;
  struct timespec ts = { 0 };
  ZKL_GETLOCK();
  ZKL_GETLOCKCONNECTION();

  if (argc == 1 && TYPE(argv[0]) == T_HASH) {
    VALUE timeout_ref = rb_hash_aref(argv[0], ID2SYM(rb_intern("timeout")));
    if (TYPE(timeout_ref) != T_NIL) {
      if ((TYPE(timeout_ref) != T_FLOAT && TYPE(timeout_ref) != RUBY_T_FIXNUM)
          || NUM2DBL(timeout_ref) == 0) {
        rb_raise(rb_eArgError, "timeout value must be numeric and cannot be zero");
      }

      timeout = (int64_t) (NUM2DBL(timeout_ref) * NSEC_PER_SEC);
      if (timeout > 0) {
        clock_gettime(CLOCK_REALTIME, &ts);
        ZKL_DEBUG("lock timeout: %lldns", timeout);
        ZKL_DEBUG("current time: %lld.%09ld", ts.tv_sec, ts.tv_nsec);
        timespec_add_ns(&ts, (uint64_t) timeout);
        ZKL_DEBUG("will block until time: %lld.%09ld", ts.tv_sec, ts.tv_nsec);
      }
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
          rb_raise(zklock_timeout_exception_, "connect timed out");
        }
      } while (zkl_connection_valid(conn) && !zkl_connection_connected(conn));

      if (!zkl_connection_valid(conn)) {
        pthread_mutex_unlock(&conn->mutex);
        rb_raise(zklock_exception_, "unable to connect to server");
      }
    }
    pthread_mutex_unlock(&conn->mutex);
  }

  lock->desired_state = ZKLOCK_STATE_LOCKED;
  cmd.cmd = ZKLCMD_LOCK;
  cmd.x.lock = lock;
  zkl_connection_send_command(conn, &cmd);

  pthread_mutex_lock(&lock->mutex);
  while (lock->state != ZKLOCK_STATE_LOCKED) {
    int ret = zkl_wait_for_notification(&lock->mutex, &lock->cond, &ts);
    if (ret == ETIMEDOUT && lock->state != ZKLOCK_STATE_LOCKED) {
      pthread_mutex_unlock(&conn->mutex);
      /* todo: if we were blocking here, we need to unwatch the node */
      rb_raise(zklock_timeout_exception_, "lock acquire timed out");
    }
  }
  pthread_mutex_unlock(&conn->mutex);

  return lock->state == ZKLOCK_STATE_LOCKED ? Qtrue : Qfalse;
}

static VALUE lock_locked(VALUE self) {
  ZKL_GETLOCK();
  return lock->state == ZKLOCK_STATE_LOCKED ? Qtrue : Qfalse;
}

void define_lock_methods(VALUE klass) {
  rb_define_method(klass, "initialize", lock_initialize, -1);
  rb_define_method(klass, "lock", lock_lock, -1);
  rb_define_method(klass, "locked?", lock_locked, 0);
}
