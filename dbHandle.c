#include "dbHandle.h"
#include <nanomsg/nn.h>
#include <nanomsg/pubsub.h>
#include "apexRTS.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sqlite3.h>
#include <sys/time.h>
#include <signal.h>
#include <pthread.h>
#include "common.h"
#include "log.h"

#define DB_TIMER_INTERVAL 30
#define MAX_QUEUE 40
#define SQL_STR_SIZE 1024

char gSqlStr[SQL_STR_SIZE * MAX_QUEUE] = {0x00};
int curQueueSize = 0;
pthread_mutex_t mutex;

void insertMsg (char *msg, int msgLen);

void execSql()
{
	sqlite3 *db;
	int rc;
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc != 0)
		LOG_ERROR (gLog, "execSql sqlite3_open_v2 fail!! msg=%s", sqlite3_errmsg (db));
	char *errMsg;

	sqlite3_exec (db, "begin;", 0, 0, &errMsg);
	rc = sqlite3_exec (db, gSqlStr, 0, 0, &errMsg);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_exec insert into msg fail!! msg=%s, sql=%s", errMsg, gSqlStr);
	}

	/*
	char clearSql[256] = {0x00};
	char hhmm[5] = {0x00};
	getNowHHMM (hhmm);
	snprintf (clearSql, 256, "delete from msg where mid not in "
		"(select mid from market where (('%s'>=stime1 and '%s'<=etime1) or "
		"('%s'>=stime2 and '%s'<=etime2)));", hhmm, hhmm, hhmm, hhmm);
	rc = sqlite3_exec (db, clearSql, 0, 0, &errMsg);
	printf("%s\n", clearSql);
	if (rc != SQLITE_OK)
	{
		perror ("sqlite3_exec clear sql fail!!");
	}
	*/

	sqlite3_exec (db, "commit;", 0, 0, &errMsg);

	sqlite3_close (db);
	curQueueSize = 0;
	memset (gSqlStr, 0, sizeof (gSqlStr));
}

void dbFlushTimer (int a)
{
	LOG_INFO (gLog, "db flush!!");
	pthread_mutex_lock (&mutex);
	execSql();
	pthread_mutex_unlock (&mutex);
	LOG_INFO (gLog, "db flush finish!!");
}
void dbThread()
{
	LOG_INFO (gLog, "db thread init ...");
	int rc, sub;

	sub = nn_socket (AF_SP, NN_SUB);
	rc = nn_setsockopt (sub, NN_SUB, NN_SUB_SUBSCRIBE, "", 0);
	if (rc != 0)
		LOG_ERROR (gLog, "dbThread nn_setsockopt fail!! msg=%s", strerror (errno));

	int timeout = 1000;
	rc = nn_setsockopt (sub, 0, NN_RCVTIMEO, &timeout, sizeof (timeout));
	if (rc != 0)
		LOG_ERROR (gLog, "dbThread nn_setsockopt rcv timeout fail!! msg=%s", strerror (errno));
	rc = nn_connect (sub, NANOMSG_PUB_URL);
	if (rc < 0)
		LOG_ERROR (gLog, "dbThread nn_connect fail!! msg=%s", strerror (errno));

	pthread_mutex_init (&mutex, NULL);

	/*
	struct itimerval t;
	t.it_interval.tv_usec = 0;
	t.it_interval.tv_sec = DB_TIMER_INTERVAL;
	t.it_value.tv_usec = 0;
	t.it_value.tv_sec = DB_TIMER_INTERVAL;
	if (setitimer (ITIMER_REAL, &t, NULL) < 0)
	{
		perror ("setitimer fail!!");
		return;
	}
	signal (SIGALRM, dbFlushTimer);
	*/

	int flushCount = 0;
	while(1)
	{
		int bytes;
		//char *buf = NULL;
		char buf[1024] = {0x00};
		//bytes = nn_recv (sub, &buf, NN_MSG, 0);
		bytes = nn_recv (sub, buf, 1024, 0);
		if (bytes <= 0)
		{
			LOG_INFO (gLog, "dbThread nn_recv fail!! %d msg=%s", bytes, nn_strerror (errno));
		}
		else
		{
			//buf[bytes] = 0x00;
			//printf("db thread : %s\n", buf);
			insertMsg (buf, bytes);
		}
		//nn_freemsg (buf);

		if(++flushCount >= 50)
		{
			dbFlushTimer (0);
			flushCount = 0;
		}
	}
}

void clearMarketData (char *market)
{
	sqlite3 *db;
	int rc;
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc != 0)
		LOG_ERROR (gLog, "clearMarketData sqlite3_open_v2 fail!! msg=%s", sqlite3_errmsg (db));
	char *errMsg;

	sqlite3_exec (db, "begin;", 0, 0, &errMsg);

	char clearSql[1024] = {0x00};
	//snprintf (clearSql, 1024, "delete from %c_msg where mid = '%s';"
	//		"delete from quote where mid = '%s';", market[0], market, market);
	snprintf (clearSql, 1024, "delete from msg where mid = '%s';"
			"delete from quote where mid = '%s';", market, market);
	rc = sqlite3_exec (db, clearSql, 0, 0, &errMsg);
	LOG_DEBUG (gLog, "%s", clearSql);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_exec clear sql fail!! msg=%s", errMsg);
	}

	sqlite3_exec (db, "commit;", 0, 0, &errMsg);

	sqlite3_close (db);
	LOG_DEBUG (gLog, "clearMarketData finish!!");
}

void updateQuoteR (char *mid, char *pid, char *lastPrice, char *lastQty, char *stockName)
{
	sqlite3 *db;
	int rc;
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc != 0)
		LOG_ERROR (gLog, "updateQuoteR sqlite3_open_v2 fail!! msg=%s", sqlite3_errmsg (db));
	char *errMsg;

	sqlite3_exec (db, "begin;", 0, 0, &errMsg);
	long double dPrice =strtold (lastPrice, NULL);
	int qty = atoi (lastQty);

	char sqlStr[2048] = {0x00};
	snprintf (sqlStr, 2048, "insert or replace into quote(mid, pid, last_price, last_qty, stock_name) "
			"values('%s', '%s', %LF, %d, '%s');", mid, pid, dPrice, qty, stockName);
	rc = sqlite3_exec (db, sqlStr, 0, 0, &errMsg);
	LOG_TRACE (gLog, "%s", sqlStr);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_exec updateQuoteR fail!! msg=%s, sql=%s", errMsg, sqlStr);
	}

	sqlite3_exec (db, "commit;", 0, 0, &errMsg);

	sqlite3_close (db);
}

void updateQuoteT_R (char *mid, char *pid, char *lastPrice)
{
	sqlite3 *db;
	int rc;
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc != 0)
		LOG_ERROR (gLog, "updateQuoteT_R sqlite3_open_v2 fail!! msg=%s", sqlite3_errmsg (db));
	char *errMsg;

	sqlite3_exec (db, "begin;", 0, 0, &errMsg);
	long double dPrice = strtold (lastPrice, NULL);

	char sqlStr[2048] = {0x00};
	snprintf (sqlStr, 2048, "update quote set last_price=%LF where mid='%s' and pid='%s';", 
			dPrice, mid, pid);
	rc = sqlite3_exec (db, sqlStr, 0, 0, &errMsg);
	LOG_TRACE (gLog, "%s", sqlStr);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_exec updateQuoteT_R fail!! msg=%s, sql=%s", errMsg, sqlStr);
	}

	sqlite3_exec (db, "commit;", 0, 0, &errMsg);

	sqlite3_close (db);
}

void updateQuoteT (char *mid, char *pid, char *price, char *qty, char *seq)
{
	sqlite3 *db;
	int rc;
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc !=0)
		LOG_ERROR (gLog, "updateQuoteT sqlite3_open_v2 fail!! msg=%s", 
				sqlite3_errmsg (db));
	char *errMsg;

	sqlite3_exec (db, "begin;", 0, 0, &errMsg);
	long double dPrice =strtod (price, NULL);
	//float fPrice = atof (price);
	int iQty = atoi (qty);

	char sqlStr[4096] = {0x00};
	snprintf (sqlStr, 4096, "insert or replace into quote(mid, pid, close_price, total_qty, last_seq, "
			"last_price, last_qty, open_price, max_price, min_price, stock_name) "
			"values('%s', '%s', %LF, %d, '%s', "
				"(select last_price from quote where mid='%s' and pid ='%s'),"
				"(select last_qty from quote where mid='%s' and pid ='%s'),"
				"(select open_price from quote where mid='%s' and pid ='%s'),"
				"(select max_price from quote where mid='%s' and pid ='%s'),"
				"(select min_price from quote where mid='%s' and pid ='%s'),"
				"(select stock_name from quote where mid='%s' and pid ='%s')"
			");", mid, pid, dPrice, iQty, seq, 
			mid, pid, mid, pid, mid, pid, mid, pid, mid, pid, mid, pid);
	snprintf (&sqlStr[strlen (sqlStr)], 256, "update quote set open_price=%LF where mid='%s' and pid='%s' "
			"and open_price is null;", dPrice, mid, pid);
	snprintf (&sqlStr[strlen (sqlStr)], 256, "update quote set max_price=%LF where mid='%s' and pid='%s' "
			"and (max_price is null or max_price<%LF);", dPrice, mid, pid, dPrice);
	snprintf (&sqlStr[strlen (sqlStr)], 256, "update quote set min_price=%LF where mid='%s' and pid='%s' "
			"and (min_price is null or min_price>%LF);", dPrice, mid, pid, dPrice);
	rc = sqlite3_exec (db, sqlStr, 0, 0, &errMsg);
	LOG_TRACE (gLog, "%s", sqlStr);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_exec updateQuoteT fail!! msg=%s, sql=%s", errMsg, sqlStr);
	}

	sqlite3_exec (db, "commit;", 0, 0, &errMsg);

	sqlite3_close (db);
}

void insertMsg (char *msg, int msgLen)
{
	if (!msg) return;
	//int msgLen = strlen (msg);
	if (msgLen <= 0) return;
	//printf ("msg len = %d\n", msgLen);

	char market[20] = {0x00};
	char seq[20] = {0x00};
	char type[20] = {0x00};
	char symbol[20] = {0x00};
	char status[20] = {0x00};
	findTagValue (msg, "3=", market, 20);
	findTagValue (msg, "34=", seq, 20);
	findTagValue (msg, "35=", type, 20);
	findTagValue (msg, "5=", symbol, 20);
	findTagValue (msg, "15005=", status, 20);

	LOG_TRACE (gLog, "market : %s, symbol : %s, seq : %s, type : %s, status : %s", 
			market, symbol, seq, type, status);
	
	pthread_mutex_lock (&mutex);
	if ((strcmp (type, "S") == 0) && (strcmp (status, "1") == 0))
		clearMarketData (market);
	pthread_mutex_unlock (&mutex);

	pthread_mutex_lock (&mutex);
	if (strcmp (type, "R") == 0)
	{
		char lastPrice[20] = {0x00};
		char lastQty[20] = {0x00};
		char stockName[40] = {0x00};
		findTagValue (msg,  "407=", lastPrice, 20);
		findTagValue (msg,  "15007=", lastQty, 20);
		findTagValue (msg,  "15006=", stockName, 40);

		updateQuoteR (market, symbol, lastPrice, lastQty, stockName);
	}

	if (strcmp (type, "T") == 0)
	{
		char price[20] = {0x00};
		char qty[20] = {0x00};
		char lastPrice[20] = {0x00};
		findTagValue (msg,  "21=", price, 20);
		findTagValue (msg,  "22=", qty, 20);
		
		findTagValue (msg,  "407=", lastPrice, 20);

		updateQuoteT (market, symbol, price, qty, seq);
		//2015/01/05 Tick中修改昨收
		if (strlen(lastPrice) > 0)
			updateQuoteT_R (market, symbol, lastPrice);
	}

	//snprintf (&gSqlStr[strlen(gSqlStr)], SQL_STR_SIZE, 
	//		"insert into %c_msg values('%s', '%s', '%s', %s, '%s');", 
	//		market[0], market, symbol, type, seq, msg);
	snprintf (&gSqlStr[strlen(gSqlStr)], SQL_STR_SIZE, 
			"insert into msg values('%s', '%s', '%s', %s, '%s');", 
			market, symbol, type, seq, msg);
	curQueueSize ++;
	if (curQueueSize == MAX_QUEUE)
	{
		execSql();
	}
	pthread_mutex_unlock (&mutex);
}

int validMsg (char *msg)
{
	sqlite3 *db;
	int rc;

	char sqlStr[512] = {0x00};
	char mid[20] = {0x00};
	char pid[20] = {0x00};
	char type[20] = {0x00};
	char **result;
	int rows, cols;
	findTagValue (msg, "3=", mid, 20);
	findTagValue (msg, "5=", pid, 20);
	findTagValue (msg, "35=", type, 20);
	if (strcmp (type, "H") == 0)
		return -1;
	if (strcmp (type, "R") != 0)
		return 0;

	pthread_mutex_lock (&mutex);
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READONLY, NULL);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "sqlite3_open_v2 fail!! msg=%s, sqlite msg=%s", 
				strerror (errno), sqlite3_errstr (rc));
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);
		return -1;
	}

	//snprintf (sqlStr, 512, "select exists(select 1 from %c_msg where mid='%s' "
	//		"and pid='%s' and type='R') as ret;", mid[0], mid, pid);
	snprintf (sqlStr, 512, "select exists(select 1 from msg where mid='%s' "
			"and pid='%s' and type='R') as ret;", mid, pid);
	rc = sqlite3_get_table (db, sqlStr, &result, &rows, &cols, 0);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "validMsg sqlite3_get_table fail!! msg=%s, sqlite msg=%s, sql=%s", 
				strerror (errno), sqlite3_errstr (rc), sqlStr);
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);
		return -1;
	}
	LOG_TRACE (gLog, "validMsg sql=%s", sqlStr);
	int i, j;
	for (i = 0; i < rows + 1; i++)
	{
		for (j = 0; j < cols; j++)
		{
			LOG_TRACE (gLog, "validMsg reslut = %s", result[i*cols+j]);
		}
	}
	if (rows >= 1 && result[1] != NULL && result[1][0] == '1')
	{
	
		LOG_TRACE (gLog, "validMsg return -1");
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);
		return -1;
	}
	LOG_TRACE (gLog, "validMsg return 0");


	sqlite3_free_table (result);
	sqlite3_close (db);
	pthread_mutex_unlock (&mutex);

	return 0;
}


