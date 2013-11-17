#include "ruby.h"
#include "zookeeper/zookeeper.h"
#include "debug.h"

static VALUE zklock_module_ = Qnil;
static VALUE zklock_connection_class_ = Qnil;
static VALUE zklock_lock_class_ = Qnil;
static VALUE zklock_shared_lock_class_ = Qnil;
static VALUE zklock_exclusive_lock_class_ = Qnil;

static VALUE connection_connected(VALUE self) {
	return Qfalse;
}

static VALUE connection_closed(VALUE self) {
	return Qtrue;
}

static VALUE connection_initialize(int argc, VALUE *argv, VALUE self) {
	if (argc == 0) rb_raise(rb_eArgError, "zookeeper server must be specified");

	return self;
}

static void define_methods(void) {
	rb_define_method(zklock_connection_class_, "connected?", connection_connected, 0);
	rb_define_method(zklock_connection_class_, "closed?", connection_closed, 0);
	rb_define_method(zklock_connection_class_, "initialize", connection_initialize, -1);
}

void Init_zklock(void) {
  zklock_module_ = rb_define_module("ZKLock");
  zklock_connection_class_ = rb_define_class_under(zklock_module_, "Connection", rb_cObject);
  zklock_lock_class_ = rb_define_class_under(zklock_module_, "Lock", rb_cObject);
  zklock_shared_lock_class_ = rb_define_class_under(zklock_module_, "SharedLock", zklock_lock_class_);
  zklock_exclusive_lock_class_ = rb_define_class_under(zklock_module_, "ExclusiveLock", zklock_lock_class_);

  define_methods();
}
