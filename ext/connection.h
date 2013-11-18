#ifndef __CONNECTION_H__
#define __CONNECTION_H__

#include <pthread.h>

struct connection_data {
  pthread_t tid;
  enum zklock_thread_status thread_state;
  int initialized;
  pthread_mutex_t mutex;
  pthread_cond_t cond;
  int pipefd[2];
  zhandle_t *zk;
  char *server;
};

void define_connection_methods(VALUE klass);

#endif /* __CONNECTION_H__ */
