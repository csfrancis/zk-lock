#include "ruby.h"

#include "zklock.h"
#include "connection.h"
#include "lock.h"
#include "logging.h"

static VALUE zklock_module_ = Qnil;
static VALUE zklock_lock_class_ = Qnil;
static VALUE zklock_shared_lock_class_ = Qnil;
static VALUE zklock_exclusive_lock_class_ = Qnil;

VALUE zklock_connection_class_ = Qnil;
VALUE zklock_exception_ = Qnil;
VALUE zklock_timeout_error_ = Qnil;

struct notification_data {
  pthread_mutex_t *mutex;
  pthread_cond_t *cond;
  struct timespec ts;
};

void zkl_zookeeper_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
  struct connection_data *conn = (struct connection_data *) zoo_get_context(zh);

  ZKL_DEBUG("zkl_zookeeper_watcher %p, type=%d, state=%d, path=%s", zh, type, state, path != NULL ? path : "n/a");

  pthread_mutex_lock(&conn->mutex);
  pthread_cond_broadcast(&conn->cond);
  pthread_mutex_unlock(&conn->mutex);
}

static VALUE wait_for_notification(void *p) {
  int ret;
  struct notification_data *data = (struct notification_data *) p;

  if (data->ts.tv_sec == 0 && data->ts.tv_nsec == 0) {
    ret = pthread_cond_wait(data->cond, data->mutex);
  } else {
    ret = pthread_cond_timedwait(data->cond, data->mutex, &data->ts);
  }

  return INT2NUM(ret);
}

static void unblock_wait_notification(void *p) {
  struct notification_data *data = (struct notification_data *) p;
  pthread_mutex_lock(data->mutex);
  pthread_cond_broadcast(data->cond);
  pthread_mutex_unlock(data->mutex);
}

int zkl_wait_for_notification(pthread_mutex_t *mutex, pthread_cond_t *cond, struct timespec *ts) {
  int ret;
  struct notification_data data = { mutex, cond };

  if (ts) {
    data.ts = *ts;
  } else {
    memset(&data.ts, 0, sizeof(struct timespec));
  }

  ret = NUM2INT(rb_thread_blocking_region(wait_for_notification, &data, unblock_wait_notification, &data));
  if (rb_thread_interrupted(rb_thread_current())) {
    pthread_mutex_unlock(mutex);
    rb_raise(rb_eInterrupt, "interrupted");
  }

  return ret;
}

static void define_methods(void) {
  define_connection_methods(zklock_connection_class_);
  define_lock_methods(zklock_lock_class_);
}

void Init_zklock(void) {
  zoo_set_debug_level(ZOO_LOG_LEVEL_DEBUG);

  zklock_module_ = rb_define_module("ZKLock");
  zklock_connection_class_ = rb_define_class_under(zklock_module_, "Connection", rb_cObject);
  zklock_lock_class_ = rb_define_class_under(zklock_module_, "Lock", rb_cObject);
  zklock_shared_lock_class_ = rb_define_class_under(zklock_module_, "SharedLock", zklock_lock_class_);
  zklock_exclusive_lock_class_ = rb_define_class_under(zklock_module_, "ExclusiveLock", zklock_lock_class_);
  zklock_exception_ = rb_define_class_under(zklock_module_, "Exception", rb_eStandardError);
  zklock_timeout_error_ = rb_define_class_under(zklock_module_, "TimeoutError", zklock_exception_);

  define_methods();
}
