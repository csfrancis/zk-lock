#ifndef __LOCK_H__
#define __LOCK_H__

enum zklock_type {
	ZKLOCK_SHARED = 0,
	ZKLOCK_EXCLUSIVE
};

struct lock_data {
	enum zklock_type type;
};

void define_lock_methods(VALUE klass);

#endif /* __LOCK_H__ */
