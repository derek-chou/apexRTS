#include "apexRTS.h"
#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include "log.h"

int pubSock;

int publishInit () 
{
	pubSock = nn_socket( AF_SP, NN_PUB );
	if (pubSock < 0)
		return -1;
	LOG_INFO (gLog, "PUB URL=%s", NANOMSG_PUB_URL);
	if (nn_bind (pubSock, NANOMSG_PUB_URL) < 0)
		return -1;


	return 0;
}

int publish (const char *msg, int msgLen) 
{
	//int msgLen = strlen (msg);

	int bytes = nn_send (pubSock, msg, msgLen, 0);
	assert (bytes == msgLen);
	return 0;
}

int publishDestroy ()
{
	return nn_shutdown (pubSock, 0);
}
