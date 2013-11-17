#ifndef __LOGGING_H__
#define __LOGGING_H__

void zkl_log(const char *fmt, ...);
#define ZKL_LOG(fmt, ...) zkl_log(fmt, ##__VA_ARGS__)

#ifdef HAVE_DEBUG
void zkl_debug(const char *fmt, ...);
#define ZKL_DEBUG(fmt, ...) zkl_debug(fmt, ##__VA_ARGS__)
#else
#define ZKL_DEBUG(fmt, ...)
#endif

#endif /* __LOGGING_H__ */