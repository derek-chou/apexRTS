#include "zmq.h"
#include <assert.h>
#include <string.h>
#include <stdint.h>

int main (void)
{
	void *context = zmq_ctx_new ();
	void *publisher = zmq_socket (context, ZMQ_PUB);
	int rc = zmq_bind (publisher, "tcp://*:2266");
	assert (rc == 0);

	int cnt;
	uint32_t index = 0;
	while (1)
	{
		char update [20];
		sprintf (update, "%.10d", index);
		zmq_send (publisher, update, strlen(update), 0);
		//if (index % 5 == 0)
			printf ("send : %s\n", update);
		index++;
		usleep (1000 * 1000);
	}
	zmq_close (publisher);
	zmq_ctx_destroy (context);
	return 0;
}
