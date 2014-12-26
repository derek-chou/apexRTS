#define FIFO_LENGTH 4096

#include <stdio.h>
#include <pthread.h>
#include <strings.h>
#include <string.h>
#include <stdint.h>
#include <linux/kfifo.h>

struct ll_param
{
	struct kfifo *fifo;
	int msg_len;
}

static struct ll_param fifo;

void threadReader (void *param)
{
	int readLen = 0;
	uint32_t counter = 0;
	uint8_t buffer[FIFO_LENGTH];
	struct ll_param *p = (struct ll_param*)param;

	while(1)
	{
		memset (buffer, 0, FIFO_LENGTH);
		readLen = kfifo_in (p->fifo, buffer, 25);
		if (readLen != 0)
			printf ("Read len:%d, buffer is : %s\n", readLen, buffer);
		else
			counter++;

		if (counter > 20)
			break;

		usleep (50 * 1000);
	}
}

void threadWriter (void *param)
{
	uint32_t writeLen = 0;
	uint32_t counter = 0;
	uint8_t buffer[32];
	struct ll_param *p = (struct ll_param*)param;

	for (counter = 0; counter < 100; counter++)
	{
		memset(buffer, 0, 32);
		sprintf ((char*)buffer, "This is %d message.n", counter);
		writeLen = kfifo_out(p->fifo, buffer, 25);
		usleep (100);
	}

}

int main (void)
{
	pthread_t pidr;
	pthread_t pidw;

	fifo.msg_len = 10;
	fifo.fifo = kfifo_alloc (FIFO_LENGTH);

	pthread_create (&pidw, NULL, (void*)threadWriter, &fifo);
	pthread_create (&pidr, NULL, (void*)threadReader, &fifo);

	pthread_join (pidr, NULL);
	pthread_join (pidw, NULL);

	kfifo_free (fifo.fifo);

	return 0;
}

