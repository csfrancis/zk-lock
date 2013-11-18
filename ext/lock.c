#include "ruby.h"

#include "zklock.h"
#include "lock.h"
#include "logging.h"

static const char * kLockIvarName = "@_lock_name";

#define ZKL_GETLOCK() struct lock_data * lock; Data_Get_Struct(rb_iv_get(self, kLockIvarName), struct lock_data, lock);

static void lock_data_free(void *p) {
  struct lock_data *lock = (struct lock_data *) p;
  ZKL_DEBUG("freeing lock: %p", p);
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
  rb_iv_set(self, kLockIvarName, Data_Wrap_Struct(rb_cObject, NULL, lock_data_free, lock));

  return self;
}

void define_lock_methods(VALUE klass) {
  rb_define_method(klass, "initialize", lock_initialize, -1);
}
