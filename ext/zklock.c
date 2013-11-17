#include "ruby.h"
#include "zookeeper/zookeeper.h"
#include "logging.h"

#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <sys/select.h>

static const int kZookeeperRecvTimeout = 10000;
static const long kSelectTimeout = 100000; /* 100ms */
static const size_t kBufSize = 1024;

static VALUE zklock_module_ = Qnil;
static VALUE zklock_connection_class_ = Qnil;
static VALUE zklock_lock_class_ = Qnil;
static VALUE zklock_shared_lock_class_ = Qnil;
static VALUE zklock_exclusive_lock_class_ = Qnil;
static VALUE zklock_exception_ = Qnil;

#define ZKL_CALLOC(ptr, type) ptr = malloc(sizeof(type)); if (ptr == NULL) rb_raise(rb_eNoMemError, "out of memory"); memset(ptr, 0, sizeof(type));
#define ZKL_GETCONNECTION() struct connection_info * conn; Data_Get_Struct(self, struct connection_info, conn);

enum zklock_thread_status {
  ZKLTHREAD_STOPPED = 0,
  ZKLTHREAD_RUNNING,
  ZKLTHREAD_STOPPING
};

struct connection_info {
  pthread_t tid;
  enum zklock_thread_status thread_state;
  int initialized;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int pipefd[2];
  zhandle_t *zk;
  char *server;
};

enum zklock_command_type {
  ZKLCMD_DISCONNECT = 0,
  ZKLCMD_LOCK,
  ZKLCMD_UNLOCK
};

struct zklock_command {
  enum zklock_command_type cmd;
};

static void connection_info_free(void *p) {
  struct connection_info *conn = (struct connection_info *) p;

  if (conn->initialized) {
    close(conn->pipefd[0]);
    close(conn->pipefd[1]);
    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
    free(conn->server);
    conn->initialized = 0;
  }

  free(conn);
}

static void process_zkl_command(struct connection_info *conn, struct zklock_command *cmd) {
  ZKL_DEBUG("received zkl command: %d", cmd->cmd);
  switch(cmd->cmd) {
  case ZKLCMD_DISCONNECT:
    conn->thread_state = ZKLTHREAD_STOPPING;
    break;
  }
}

static void send_zkl_command(struct connection_info *conn, struct zklock_command *cmd) {
  int ret;
  ret = write(conn->pipefd[1], (void *) cmd, sizeof(struct zklock_command));
  if (ret == -1) {
    char buf[128];
    snprintf(buf, 128, "error calling write(): %d", errno);
    ZKL_LOG("%s", buf);
    rb_raise(zklock_exception_, "%s", buf);
  }
}

static void * connection_info_thread(void *p) {
  unsigned char buf[kBufSize];
  size_t buf_pos = 0;
  struct connection_info *conn = (struct connection_info *) p;

  conn->thread_state = ZKLTHREAD_RUNNING;

  while (conn->thread_state != ZKLTHREAD_STOPPING) {
    struct timeval timeout = { 0 };
    int numfds, ret;
    fd_set rd, wr, er;

    FD_ZERO(&rd);
    FD_ZERO(&wr);
    FD_ZERO(&er);
    FD_SET(conn->pipefd[0], &rd);

    timeout.tv_usec = kSelectTimeout;
    numfds = conn->pipefd[0];

    ret = select(numfds + 1, &rd, &wr, &er, &timeout);

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
        ZKL_DEBUG("EOF from read pipe - breaking out of connection_info_thread");
        break;
      }
      if (bytes_read == -1) {
        if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
      }
      buf_pos += bytes_read;
      if (buf_pos == sizeof(struct zklock_command)) {
        process_zkl_command(conn, (struct zklock_command *) buf);
        buf_pos = 0;
      }
    }
  }

  conn->thread_state = ZKLTHREAD_STOPPED;

  ZKL_DEBUG("exiting connection_info_thread");
}

static VALUE connection_connected(VALUE self) {
  ZKL_GETCONNECTION();
  return conn->tid != 0 ? Qtrue : Qfalse;
}

static VALUE connection_closed(VALUE self) {
  ZKL_GETCONNECTION();
  return conn->thread_state == ZKLTHREAD_STOPPED ? Qtrue : Qfalse;
}

static VALUE connection_alloc(VALUE klass) {
  struct connection_info *conn = NULL;
  ZKL_CALLOC(conn, struct connection_info);
  return Data_Wrap_Struct(klass, NULL, connection_info_free, conn);
}

static VALUE connection_initialize(int argc, VALUE *argv, VALUE self) {
  ZKL_GETCONNECTION();

  if (argc == 0 || TYPE(argv[0]) != T_STRING) rb_raise(rb_eArgError, "zookeeper server must be specified as a string");
  if (RSTRING_LEN(argv[0]) == 0) rb_raise(rb_eArgError, "server string cannot be empty");

  conn->server = strdup(RSTRING_PTR(argv[0]));
  pthread_mutex_init(&conn->mutex, NULL);
  pthread_cond_init(&conn->cond, NULL);
  if (pipe(conn->pipefd) == -1) rb_raise(rb_eFatal, "pipe() failed: %d", errno);
  fcntl(conn->pipefd[0], F_SETFL, fcntl(conn->pipefd[0], F_GETFL) | O_NONBLOCK);

  conn->initialized = 1;

  return self;
}

static VALUE connection_connect(VALUE self) {
  int err;
  ZKL_GETCONNECTION();

  err = pthread_create(&conn->tid, NULL, connection_info_thread, (void *) conn);
  if (err != 0) rb_raise(rb_eFatal, "pthread_create failed: %d", err);

  return self;
}

static VALUE connection_close(VALUE self) {
  struct zklock_command cmd;
  ZKL_GETCONNECTION();

  if (conn->tid == 0) rb_raise(zklock_exception_, "connection is not connected");

  cmd.cmd = ZKLCMD_DISCONNECT;
  send_zkl_command(conn, &cmd);

  return self;
}

static void define_methods(void) {
  rb_define_alloc_func(zklock_connection_class_, connection_alloc);
  rb_define_method(zklock_connection_class_, "initialize", connection_initialize, -1);
  rb_define_method(zklock_connection_class_, "connected?", connection_connected, 0);
  rb_define_method(zklock_connection_class_, "closed?", connection_closed, 0);
  rb_define_method(zklock_connection_class_, "connect", connection_connect, 0);
  rb_define_method(zklock_connection_class_, "close", connection_close, 0);
}

void Init_zklock(void) {
  zklock_module_ = rb_define_module("ZKLock");
  zklock_connection_class_ = rb_define_class_under(zklock_module_, "Connection", rb_cObject);
  zklock_lock_class_ = rb_define_class_under(zklock_module_, "Lock", rb_cObject);
  zklock_shared_lock_class_ = rb_define_class_under(zklock_module_, "SharedLock", zklock_lock_class_);
  zklock_exclusive_lock_class_ = rb_define_class_under(zklock_module_, "ExclusiveLock", zklock_lock_class_);
  zklock_exception_ = rb_define_class_under(zklock_module_, "Exception", rb_eStandardError);

  define_methods();
}
