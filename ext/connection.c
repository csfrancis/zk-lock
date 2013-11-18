#include "ruby.h"

#include "zklock.h"
#include "connection.h"
#include "lock.h"
#include "logging.h"

#include <sys/select.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#define ZKL_GETCONNECTION() struct connection_data * conn; Data_Get_Struct(self, struct connection_data, conn);

static const int kZookeeperRecvTimeout = 10000;
static const long kSelectTimeout = 100000; /* 100ms */
static const size_t kBufSize = 1024;

static void zkl_connection_process_command(struct connection_data *conn, struct zklock_command *cmd) {
  ZKL_DEBUG("received zkl command: %d", cmd->cmd);
  switch(cmd->cmd) {
  case ZKLCMD_TERMINATE:
    conn->thread_state = ZKLTHREAD_STOPPING;
    break;
  case ZKLCMD_LOCK:
    zkl_lock_process_lock(cmd);
    break;
  default:
    break;
  }
}

void zkl_connection_send_command(struct connection_data *conn, struct zklock_command *cmd) {
  int ret;
  ret = write(conn->pipefd[1], (void *) cmd, sizeof(struct zklock_command));
  if (ret == -1) {
    char buf[128];
    snprintf(buf, 128, "error calling write(): %d", errno);
    ZKL_LOG("%s", buf);
    rb_raise(zklock_exception_, "%s", buf);
  }
}

static void zkl_send_terminate(struct connection_data *conn) {
  struct zklock_command cmd;
  cmd.cmd = ZKLCMD_TERMINATE;
  zkl_connection_send_command(conn, &cmd);
}

static void connection_data_free(void *p) {
  struct connection_data *conn = (struct connection_data *) p;
  ZKL_DEBUG("freeing connection: %p", p);

  if (conn->initialized) {
    if (conn->thread_state != ZKLTHREAD_STOPPED && conn->thread_state != ZKLTHREAD_STOPPING) {
      ZKL_DEBUG("zookeeper worker thread for %p still alive in gc(!)", conn);
      zkl_send_terminate(conn);
    }
    if (conn->thread_state != ZKLTHREAD_STOPPED) {
      /* if the connection wasn't explicitly closed, we have to wait. tough. */
      pthread_join(conn->tid, NULL);
    }
    close(conn->pipefd[0]);
    close(conn->pipefd[1]);
    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
    free(conn->server);
    conn->initialized = 0;
  }

  free(conn);
}

static void * connection_data_worker(void *p) {
  unsigned char buf[kBufSize];
  size_t buf_pos = 0;
  int ret;
  struct connection_data *conn = (struct connection_data *) p;

  conn->zk = zookeeper_init(conn->server, zkl_zookeeper_watcher, kZookeeperRecvTimeout,
    NULL, (void *) conn, 0);
  if (conn->zk == NULL) {
    ZKL_LOG("zookeeper_init failed: %d", errno);
    goto exit;
  }
  ZKL_DEBUG("zookeeper_init initialized with handle %p", conn->zk);

  conn->thread_state = ZKLTHREAD_RUNNING;

  while (conn->thread_state != ZKLTHREAD_STOPPING) {
    struct timeval tv = { 0 }, zk_tv = { 0 };
    int numfds, zk_fd = -1, zk_interest = 0, zk_events = 0;
    fd_set rd, wr, er;

    FD_ZERO(&rd);
    FD_ZERO(&wr);
    FD_ZERO(&er);
    FD_SET(conn->pipefd[0], &rd);

    ret = zookeeper_interest(conn->zk, &zk_fd, &zk_interest, &zk_tv);
    if (ret != ZOK) {
      ZKL_LOG("zookeeper_interest() failed: %d, errno: %d", ret, errno);
      break;
    }

    if (zk_interest & (ZOOKEEPER_READ | ZOOKEEPER_WRITE)) {
      numfds = (zk_fd > conn->pipefd[0]) ? zk_fd : conn->pipefd[0];
      if (zk_interest & ZOOKEEPER_READ) FD_SET(zk_fd, &rd);
      if (zk_interest & ZOOKEEPER_WRITE) FD_SET(zk_fd, &wr);
    } else {
      numfds = conn->pipefd[0];
    }

    tv.tv_sec = 0;
    tv.tv_usec = zk_tv.tv_usec < kSelectTimeout ? zk_tv.tv_usec : kSelectTimeout;

    ret = select(numfds + 1, &rd, &wr, &er, &tv);

    if (ret == -1) {
      if (errno == EINTR) continue;
      else {
        ZKL_LOG("error calling select(): %d", errno);
        break;
      }
    }

    if (FD_ISSET(conn->pipefd[0], &rd)) {
      int bytes_read = read(conn->pipefd[0], buf + buf_pos, kBufSize - buf_pos);
      if (bytes_read == 0) {
        ZKL_DEBUG("EOF from read pipe - breaking out of connection_data_worker");
        break;
      }
      if (bytes_read == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
      }
      buf_pos += bytes_read;
      if (buf_pos == sizeof(struct zklock_command)) {
        zkl_connection_process_command(conn, (struct zklock_command *) buf);
        buf_pos = 0;
      }
    }

    if (zk_interest & ZOOKEEPER_READ && FD_ISSET(zk_fd, &rd)) {
      zk_events |= ZOOKEEPER_READ;
    }

    if (zk_interest & ZOOKEEPER_WRITE && FD_ISSET(zk_fd, &wr)) {
      zk_events |= ZOOKEEPER_WRITE;
    }

    if (zk_events) {
      ret = zookeeper_process(conn->zk, zk_events);
      if (ret == ZOK || ret == ZNOTHING)
        continue;
      ZKL_DEBUG("zookeeper_process() returned: %d, handle=%p", ret, conn->zk);
      break;
    }
  }

exit:
  if (conn->zk) {
    ret = zookeeper_close(conn->zk);
    if (ret != 0) {
      ZKL_DEBUG("zookeeper_close() returned: %d, handle=%p", ret, conn->zk);
    }
    conn->zk = NULL;
  }
  conn->thread_state = ZKLTHREAD_STOPPED;
  pthread_mutex_lock(&conn->mutex);
  pthread_cond_broadcast(&conn->cond);
  pthread_mutex_unlock(&conn->mutex);

  ZKL_DEBUG("exiting connection_data_worker");
  return NULL;
}

static VALUE connection_connected(VALUE self) {
  ZKL_GETCONNECTION();
  return zkl_connection_connected(conn) ? Qtrue : Qfalse;
}

int zkl_connection_connected(struct connection_data *conn) {
  return conn->zk != NULL && zoo_state(conn->zk) == ZOO_CONNECTED_STATE;
}

int zkl_connection_valid(struct connection_data *conn) {
  return conn->thread_state == ZKLTHREAD_RUNNING || conn->thread_state == ZKLTHREAD_STARTING;
}

static VALUE connection_closed(VALUE self) {
  ZKL_GETCONNECTION();
  return conn->thread_state == ZKLTHREAD_STOPPED ? Qtrue : Qfalse;
}

static VALUE connection_alloc(VALUE klass) {
  struct connection_data *conn = NULL;
  ZKL_CALLOC(conn, struct connection_data);
  return Data_Wrap_Struct(klass, NULL, connection_data_free, conn);
}

static VALUE connection_initialize(int argc, VALUE *argv, VALUE self) {
  pthread_mutexattr_t attr;
  ZKL_GETCONNECTION();

  if (argc == 0 || TYPE(argv[0]) != T_STRING) rb_raise(rb_eArgError, "zookeeper server must be specified as a string");
  if (RSTRING_LEN(argv[0]) == 0) rb_raise(rb_eArgError, "server string cannot be empty");

  conn->server = strdup(RSTRING_PTR(argv[0]));
  pthread_mutexattr_init(&attr);
  pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
  pthread_mutex_init(&conn->mutex, &attr);
  pthread_cond_init(&conn->cond, NULL);
  if (pipe(conn->pipefd) == -1) rb_raise(rb_eFatal, "pipe() failed: %d", errno);
  fcntl(conn->pipefd[0], F_SETFL, fcntl(conn->pipefd[0], F_GETFL) | O_NONBLOCK);

  conn->initialized = 1;

  return self;
}

int zkl_wait_for_connection(struct connection_data *conn, struct timespec *ts) {
  return zkl_wait_for_notification(&conn->mutex, &conn->cond, ts);
}

void zkl_connection_connect(struct connection_data *conn) {
  int err;
  if (conn->thread_state != ZKLTHREAD_STOPPED) {
    rb_raise(zklock_exception_, "connection is in progress or connected");
  }

  conn->thread_state = ZKLTHREAD_STARTING;
  err = pthread_create(&conn->tid, NULL, connection_data_worker, (void *) conn);
  if (err != 0) {
    rb_raise(rb_eFatal, "pthread_create failed: %d", err);
  }
}

static VALUE connection_connect(int argc, VALUE *argv, VALUE self) {
  uint64_t timeout = 0;
  struct timespec ts;
  ZKL_GETCONNECTION();

  if (conn->thread_state != ZKLTHREAD_STOPPED) {
    rb_raise(zklock_exception_, "connection is in progress or connected");
  }

  if (argc == 1 && TYPE(argv[0]) == T_HASH) {
    VALUE timeout_ref = rb_hash_aref(argv[0], ID2SYM(rb_intern("timeout")));
    if (TYPE(timeout_ref) != T_NIL) {
      if ((TYPE(timeout_ref) != T_FLOAT && TYPE(timeout_ref) != RUBY_T_FIXNUM)
          || NUM2DBL(timeout_ref) <= 0) {
        rb_raise(rb_eArgError, "timeout must be a positive numeric value");
      }

      timeout = (uint64_t) (NUM2DBL(timeout_ref) * NSEC_PER_SEC);
      clock_gettime(CLOCK_REALTIME, &ts);
      ZKL_DEBUG("connect timeout: %lldns", timeout);
      ZKL_DEBUG("current time: %lld.%09ld", ts.tv_sec, ts.tv_nsec);
      timespec_add_ns(&ts, (uint64_t) timeout);
      ZKL_DEBUG("will block until time: %lld.%09ld", ts.tv_sec, ts.tv_nsec);
    }
  }

  zkl_connection_connect(conn);
  if (timeout) {
    pthread_mutex_lock(&conn->mutex);
    if (!zkl_connection_connected(conn)) {
      while(conn->thread_state != ZKLTHREAD_STOPPED && connection_connected(self) != Qtrue) {
        int ret = zkl_wait_for_connection(conn, &ts);
        if (ret == ETIMEDOUT && !zkl_connection_connected(conn)) {
          pthread_mutex_unlock(&conn->mutex);
          rb_raise(zklock_timeout_exception_, "connect timed out");
        }
      }
    }
    pthread_mutex_unlock(&conn->mutex);
  }

  return connection_connected(self);
}

static VALUE connection_close(VALUE self) {
  ZKL_GETCONNECTION();

  if (conn->thread_state != ZKLTHREAD_RUNNING)
    rb_raise(zklock_exception_, "connection is not connected");

  zkl_send_terminate(conn);

  return self;
}

void define_connection_methods(VALUE klass) {
  rb_define_alloc_func(klass, connection_alloc);
  rb_define_method(klass, "initialize", connection_initialize, -1);
  rb_define_method(klass, "connected?", connection_connected, 0);
  rb_define_method(klass, "closed?", connection_closed, 0);
  rb_define_method(klass, "connect", connection_connect, -1);
  rb_define_method(klass, "close", connection_close, 0);
}
