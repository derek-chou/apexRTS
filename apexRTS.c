#include "zmq.h"
#include "publish.h"
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
#include <pthread.h>
#include <sys/types.h>
#include <unistd.h>
#include <errno.h>
#include "apexRTS.h"
#include "reply.h"
#include "dbHandle.h"
#include "config.h"
#include "common.h"
#include "log.h"

char ZMQ_SUB_URL[32] = {0x00};
char NANOMSG_PUB_URL[32] = {0x00};
char NANOMSG_REP_URL[32] = {0x00};
log4c_category_t *gLog;

void subscribeThread ();
int processRunning (const char *prg);
int main (int argc, char *argv [])
{
	int rc;
	pthread_t repThreadID, subThreadID, dbThreadID;

	if (processRunning ("apexRTS") == 0)
	{
		printf ("apexRTS process is running!!\n");	
		return -1;
	}

	//setvbuf (stdout, NULL, _IONBF, 8192);
	getConfString ("IS", ZMQ_SUB_URL);
	getConfString ("PUB", NANOMSG_PUB_URL);
	getConfString ("REP", NANOMSG_REP_URL);

	log4c_init ();
	gLog = log4c_category_get ("log.std");

	LOG_INFO (gLog, "system start !!");
	LOG_INFO (gLog, "info src : %s", ZMQ_SUB_URL);
	rc = publishInit();
	if (rc != 0)
	{
		LOG_ERROR (gLog, "publishInit fail!! %s", strerror (errno));
		return -1;
	}

	pthread_create (&repThreadID, NULL, (void *)&replyThread, (void *)NULL);
	pthread_create (&subThreadID, NULL, (void *)&subscribeThread, (void *)NULL);
	pthread_create (&dbThreadID, NULL, (void *)&dbThread, (void *)NULL);
	
	pthread_join (repThreadID, NULL);
	pthread_join (subThreadID, NULL);
	pthread_join (dbThreadID, NULL);

	publishDestroy();
	return 0;
}

void subscribeThread ()
{
	LOG_INFO (gLog, "sub thread init...");
	int rc, recvBytes;
	void *context = zmq_ctx_new ();
	void *subscriber = zmq_socket (context, ZMQ_SUB);
	rc = zmq_connect (subscriber, ZMQ_SUB_URL);
	assert (rc == 0);

	rc = zmq_setsockopt (subscriber, ZMQ_SUBSCRIBE, ZMQ_SUB_FILTER, strlen (ZMQ_SUB_FILTER));
	assert (rc == 0);
	while(1)
	{
		char msg[1024] = {0x00};
		recvBytes = zmq_recv (subscriber, msg, 1024, 0);
		if (recvBytes <= 0)
			LOG_ERROR (gLog, "zmq_recv fail!! err=%s", strerror (errno));

		if (validMsg (msg) == 0)
			publish (msg, recvBytes);

		LOG_TRACE (gLog, "msg : %s", msg);
	}
	zmq_close (subscriber);
	zmq_ctx_destroy (context);


}

int processRunning (const char* prg)
{
	const char *pidFile = ".tmp_pid";
	const char *p = strrchr (prg, '/');
	if (p)
		p++;
	else
		p = prg;

	char cmd[128] = {0x00};
	snprintf (cmd, 128, "pgrep %s > %s", p, pidFile);
	system (cmd);
	FILE *fp = fopen (pidFile, "r");
	if (fp == NULL)
	{
		LOG_ERROR (gLog, "can't open pid file: %s!", pidFile);
		return -1;
	}
	int pid;
	ssize_t readBytes;
	size_t len = 0;
	char *lineBuf = NULL;
	while ((readBytes = getline (&lineBuf, &len, fp)) != -1)
	{
		pid = atoi (lineBuf);
		if ( (pid_t)pid != getpid())
			break;
		else
			pid = 0;
	}
	fclose (fp);
	sprintf (cmd, "rm -f %s", pidFile);
	system (cmd);
	return (pid > 0) ? 0 : -1;
}

