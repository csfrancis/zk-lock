# zk-lock #

A no-frills [ZooKeeper](http://zookeeper.apache.org/) distributed locking library for Ruby. For a full-featured Zookeeper library, see [ZK](https://github.com/zk-ruby/zk).

## Motivation ##
zk-lock was born out of three requirements:

- The need for a ZooKeeper locking library where lock timeouts could be accurately controlled.
- The need to acquire multiple locks asynchronously.
- #webscale performance.

We are using zk-lock as part of a resource announcement strategy, where a resource is represented by a shared-exclusive lock. Any process that uses a resource must acquire a shared lock in order to control exclusive access by other processes. It is also sometimes desirable for multiple resource locks to be acquired by a process. Rather than acquire each lock sequentially, zk-lock allows a set of locks to be acquired asynchronously.

zk-lock using the locking recipe as described in http://zookeeper.apache.org/doc/r3.1.2/recipes.html#Shared+Locks

## Usage ##

First, create a connection with the ZooKeeper server path:
```ruby
c = ZKLock::Connection.new("localhost:2181")
```

Then, create either an `ExclusiveLock` or `SharedLock` instance from the connection:
```ruby
lock = ZKLock::SharedLock("/shopify/shops/1/lock", c)
lock.lock
# do stuff with lock acquired
lock.unlock
```

You can also use the `with_lock` and a block:
```ruby
lock.with_lock do
  # do stuff with lock acquired
end
```

## Reference ##
All classes and exceptions exist in the `ZKLock` namespace.

#### ZKLock::Connection ####
***new(servers) → new_connection*** 

Creates a new connection object.

- `servers`- ZooKeeper server list.

***connect(opts) → true or false***

Connects to the ZooKeeper server.  Returns true if the connection was successfully established, otherwise false.

- `opts` - Options hash that can contain the following keys:
  - `:timeout`- A positive numeric value specifies the amount of time to wait for a connection before raising `TimeoutError`. Negative values will block indefinitely and 0 will perform a non-blocking connect. Default value is 0.

***close → connection*** - 

Closes the connection. Returns self.

***connected? → true or false***

Indicates if the connection instance is connected.

#### ZKLock::(Shared|Exclusive)Lock ####
***new(path, connection) → new_lock*** 

Creates a new lock object.

- `path` - String or array that specifies the lock path.  All paths must start with a forward slash (/) character.

- `connection` - Connection instance.

***lock(opts) → true or false or hash***

Locks a lock instance. If the connection instance associated with the lock will be automatically connected if it's not already. If the lock has a single path, true or false will be returned to indicate if the lock has been acquired. If the lock instance has multiple paths, a hash will be returned with a key for each path and a boolean value indicating if the lock has been acquired.

- `opts` - Options hash that can contain the following keys:
  - `:timeout` - A positive numeric value specifies the amount of time to wait for the lock to be acquired before raising `TimeoutError`. A negative value will block indefinitely. Default value is -1.
  - `:blocking` - Boolean value that indicates whether the lock should block to wait for acquisition (set up a watch on the currently locked node).  Default value is false.

***unlock(opts) → true or false***

Unlocks a lock instance.  Returns true if the lock was unlocked or false if the lock was not locked.

- `opts` - Options hash that can contain the following keys:
  - `:timeout` - A positive numeric value specifies the amount of time to wait for the lock to be unlocked before raising `TimeoutError`. Negative values will block indefinitely and 0 will perform a non-blocking unlock.  Default value is 0.

***with_lock(opts) { block } → lock***

Wraps a lock/unlock pair in a block. The `opts` parameter accepts the same keys as in `lock`.  This method will block for lock acquisition by setting the `:blocking` option to true.

***locked? → true or false or hash***

Indicates if the lock is currently locked. Returns true or false for a single lock path, or a hash if the lock has multiple locks.

#### Exceptions ####

`ZKLock::TimeoutError` - Raised when an operation times out.

`ZKLock::Exception` - General purpose exception class.

## Todo ##

- [ ] Multiplex connections across a single worker thread.

- [ ] Better exceptions.
