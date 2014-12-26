#include <pthread.h>

extern pthread_mutex_t mutex;

void dbThread();
int validMsg (char* msg);
