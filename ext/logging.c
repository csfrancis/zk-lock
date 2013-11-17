#include <stdio.h>
#include <stdarg.h>

static const int kBufSize = 1024;

static void zkl_print_log(FILE *f, const char *tag, const char *fmt, va_list ap) {
  char buf[kBufSize];
  vsnprintf(buf, kBufSize, fmt, ap);
  fprintf(f, "%s %s\n", tag, buf);
}

void zkl_log(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  zkl_print_log(stderr, "[ZKLock]", fmt, ap);
  va_end(ap);
}

#ifdef HAVE_DEBUG

void zkl_debug(const char *fmt, ...) {
  va_list ap;
  va_start(ap, fmt);
  zkl_print_log(stderr, "[ZKLock::DEBUG]", fmt, ap);
  va_end(ap);
}

#endif /* HAVE_DEBUG */
