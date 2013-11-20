#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include <pthread.h>

struct connection_data {
  pthread_t tid;
  int ref_count;
  enum zklock_thread_status thread_state;
  int initialized;
  int pipefd[2];
  zhandle_t *zk;
  char *server;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
};

void zkl_connection_send_command(struct connection_data *conn, struct zklock_command *cmd);
int zkl_wait_for_connection(struct connection_data *conn, struct timespec *ts);
void zkl_connection_connect(struct connection_data *conn);
int zkl_connection_connected(struct connection_data *conn);
int zkl_connection_valid(struct connection_data *conn);
void zkl_connection_incr_locks(struct connection_data *conn);
void zkl_connection_decr_locks(struct connection_data *conn);

void define_connection_methods(VALUE klass);

#endif /* __CONNECTION_H__ */
