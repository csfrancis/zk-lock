#include "ruby.h"

#include "zklock.h"
#include "connection.h"
#include "logging.h"

static VALUE zklock_module_ = Qnil;
static VALUE zklock_connection_class_ = Qnil;
static VALUE zklock_lock_class_ = Qnil;
static VALUE zklock_shared_lock_class_ = Qnil;
static VALUE zklock_exclusive_lock_class_ = Qnil;

VALUE zklock_exception_ = Qnil;
VALUE zklock_timeout_exception_ = Qnil;

__thread struct timespec thread_ts_;

void zkl_zookeeper_watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx) {
  struct connection_data *conn = (struct connection_data *) zoo_get_context(zh);

  ZKL_DEBUG("zkl_zookeeper_watcher %p, type=%d, state=%d, path=%s", zh, type, state, path != NULL ? path : "n/a");

  pthread_mutex_lock(&conn->mutex);
  pthread_cond_broadcast(&conn->cond);
  pthread_mutex_unlock(&conn->mutex);
}

static void define_methods(void) {
  define_connection_methods(zklock_connection_class_);
}

void Init_zklock(void) {
  zklock_module_ = rb_define_module("ZKLock");
  zklock_connection_class_ = rb_define_class_under(zklock_module_, "Connection", rb_cObject);
  zklock_lock_class_ = rb_define_class_under(zklock_module_, "Lock", rb_cObject);
  zklock_shared_lock_class_ = rb_define_class_under(zklock_module_, "SharedLock", zklock_lock_class_);
  zklock_exclusive_lock_class_ = rb_define_class_under(zklock_module_, "ExclusiveLock", zklock_lock_class_);
  zklock_exception_ = rb_define_class_under(zklock_module_, "Exception", rb_eStandardError);
  zklock_timeout_exception_ = rb_define_class_under(zklock_module_, "TimeoutException", zklock_exception_);

  define_methods();
}
