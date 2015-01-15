#include "apexRTS.h"
#include <assert.h> 
#include <stdio.h> 
#include <stdlib.h> 
#include <string.h> 
#include <sqlite3.h> 
#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"
#include "log.h"
#include "dbHandle.h"
#include "publish.h"

void rescueTick (int sock, char *mid, char *pid, char *price, char *qty);
void rescueQuote (int sock, char *mid, char *pid, char *price, char *qty);

int splitRequest (char *req, char *mid, char *pid, char *type, char *startSeq, char *endSeq)
{
	char *pos = NULL;
	int beginPos = 0, endPos = 0, count = 0;

	while(1)
	{
		pos = strchr (&req[beginPos], '#');
		if (pos != NULL)
			endPos = pos - req;
		else
			break;

		if (endPos >= beginPos)
		{
			switch (count)
			{
				case 0:
					memcpy (mid, &req[beginPos], endPos - beginPos); break;
				case 1:
					memcpy (pid, &req[beginPos], endPos - beginPos); break;
				case 2:
					memcpy (type, &req[beginPos], endPos - beginPos); break;
				case 3:
					memcpy (startSeq, &req[beginPos], endPos - beginPos); break;
				case 4:
					memcpy (endSeq, &req[beginPos], endPos - beginPos); break;
			}
			beginPos = endPos + 1;
		}
		else
			break;
		count ++;
	}
	if (count != 5)
		return -1;

	return 0;
}

void replyThread ()
{
	LOG_INFO (gLog, "reply thread init...");
	int rep = nn_socket (AF_SP, NN_REP);
	if (rep < 0)
		LOG_ERROR (gLog, "replyThread nn_socket fail!! err=%s", strerror (errno));
	int rc = nn_bind (rep, NANOMSG_REP_URL);
	LOG_INFO (gLog, "REP URL=%s", NANOMSG_REP_URL);
	if (rc < 0)
		LOG_ERROR (gLog, "replyThread nn_bind fail!! err=%s", strerror (errno));

	while(1)
	{
		char buf[128] = {0x00};
		int bufSize = sizeof (buf);

		int rc = nn_recv (rep, buf, bufSize, 0);
		if (rc < 0)
			LOG_ERROR (gLog, "reply nn_recv fail!! err=%s", strerror (errno));

		LOG_DEBUG (gLog, "recv request : %s", buf);
		//1. parse req
		char mid[20] = {0x00};
		char pid[20] = {0x00};
		char startSeq[20] = {0x00};
		char endSeq[20] = {0x00};
		char type[20] = {0x00};
		rc = splitRequest (buf, mid, pid, type, startSeq, endSeq);
		if (rc == -1)
		{
			nn_send (rep, "parameter error", 15, 0);
			continue;
		}
		LOG_INFO (gLog, "mid : %s, pid : %s, type : %s, start : %s, end : %s", mid, pid, type, startSeq, endSeq);

		if (strcmp (type, "MT") == 0)
		{
			rescueTick (rep, mid, pid, startSeq, endSeq);
			char retmsg[32] = {0x00};
			snprintf (retmsg, sizeof (retmsg), "[success]");
			rc = nn_send (rep, retmsg, strlen (retmsg), 0); 
			if (rc < 0)
			{
				LOG_ERROR (gLog, "replyThread[rescue] nn_send fail!! %s", strerror (errno));
				return;
			}
	
			continue;
		}
		if (strcmp (type, "MQ") == 0)
		{
			rescueQuote (rep, mid, pid, startSeq, endSeq);
			char retmsg[32] = {0x00};
			snprintf (retmsg, sizeof (retmsg), "[success]");
			rc = nn_send (rep, retmsg, strlen (retmsg), 0); 
			if (rc < 0)
			{
				LOG_ERROR (gLog, "replyThread[rescue] nn_send fail!! %s", strerror (errno));
				return;
			}
	
			continue;
		}

		//2. query db
		sqlite3 *db;
		char **result;
		int rows, cols, i;
		char msg[20480] = {0x00};
		char startCondition[30] = {0x00};
		char endCondition[30] = {0x00};
		char limitCondition[30] = {0x00};

		if (strlen (endSeq) > 0)
			snprintf (endCondition, 30, "and seq <= %s", endSeq);
		if ((strlen (mid) == 0) && (strlen (pid) == 0) && (strlen (startSeq) > 0))
			snprintf (limitCondition, 30, "%s,", startSeq);
		else if (strlen (startSeq) > 0)
			snprintf (startCondition, 30, "and seq >= %s", startSeq);

		char sqlStr[512] = {0x00};
		if (strlen (pid) == 0)
			sprintf (pid, "%%");
		if (strcmp (type, "Q") == 0)
		{
			if (strlen (startSeq) == 0)
				startSeq[0] = '0';
			snprintf (sqlStr, 512, "select * from quote where mid like '%%%s%%' and pid like '%s' "
					"order by mid, pid limit %s, 100;", mid, pid, startSeq);
		}
		else
		{
			snprintf (sqlStr, 512, "select * from %c_msg where mid like '%%%s%%' and pid like '%s' "
					"and type like '%%%s%%' %s %s order by seq limit %s100;", 
					mid[0], mid, pid, type, startCondition, endCondition, limitCondition);
		}
		LOG_INFO (gLog, "request sql : %s", sqlStr);

		pthread_mutex_lock (&mutex);
		rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READONLY, NULL);
		if (rc != SQLITE_OK)
		{
			LOG_ERROR (gLog, "sqlite3_open_v2 fail!! msg=%s, sqlite msg=%s", 
					strerror (errno), sqlite3_errstr (rc));
			sqlite3_close (db);
			pthread_mutex_unlock (&mutex);
			continue;
		}
		rc = sqlite3_get_table (db, sqlStr, &result, &rows, &cols, 0);
		if (rc != SQLITE_OK)
		{
			LOG_ERROR (gLog, "replyThread rqlite3_get_table fail!! msg=%s, sqlite msg=%s, sql=%s", 
					strerror (errno), sqlite3_errstr (rc), sqlStr);
			sqlite3_close (db);
			rc = nn_send (rep, "parameter error", 15, 0); 
			pthread_mutex_unlock (&mutex);
			continue;
		}
		if (strcmp (type, "Q") == 0)
		{
			int j = 0;
			for (i = 1; i < rows+1; i++)
			{
				sprintf (&msg[strlen (msg)], "%s%c", "8=FIX.4.2", 0x01);
				sprintf (&msg[strlen (msg)], "%s%c", "35=Q", 0x01);
				for (j = 0; j < cols; j++)
				{
					switch (j)
					{
						case 0:
							sprintf (&msg[strlen (msg)], "%s", "3="); break;
						case 1:
							sprintf (&msg[strlen (msg)], "%s", "5="); break;
						case 2:
							sprintf (&msg[strlen (msg)], "%s", "407="); break;
						case 3:
							sprintf (&msg[strlen (msg)], "%s", "15007="); break;
						case 4:
							sprintf (&msg[strlen (msg)], "%s", "400="); break;
						case 5:
							sprintf (&msg[strlen (msg)], "%s", "388="); break;
						case 6:
							sprintf (&msg[strlen (msg)], "%s", "394="); break;
						case 7:
							sprintf (&msg[strlen (msg)], "%s", "385="); break;
						case 8:
							sprintf (&msg[strlen (msg)], "%s", "22="); break;
						case 9:
							sprintf (&msg[strlen (msg)], "%s", "15008="); break;
						case 10:
							sprintf (&msg[strlen (msg)], "%s", "15006="); break;
						default: break;
					}
					if (result[i*cols+j])
						sprintf (&msg[strlen (msg)], "%s", result[i*cols+j]);

					sprintf (&msg[strlen (msg)], "%c", 0x01);
				}
				sprintf (&msg[strlen (msg)], "%s%c", "10=000", 0x01);
				//LOG_TRACE (gLog, "%s", result[i*11+4]);
			}
		}
		else
		{
			for (i = 1; i < rows+1; i++)
			{
				//msg column in 4 position
				LOG_TRACE (gLog, "%s", result[i*5+4]);
				sprintf (&msg[strlen (msg)], "%s", result[i*5+4]);
			}
		}
		sprintf (&msg[strlen (msg)], "[end]");
		LOG_TRACE (gLog, "msg = %s", msg);
		sqlite3_free_table (result);
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);

		rc = nn_send (rep, msg, strlen (msg), 0); 
		if (rc < 0)
		{
			LOG_ERROR (gLog, "replyThread nn_send fail!! %s", strerror (errno));
			continue;
		}
	}
}

void getFixNowString (char *buf)
{
	time_t now;
	struct tm *timeinfo;
	struct timeval tv;
	
	gettimeofday (&tv, NULL);
	time (&now);
	timeinfo = localtime (&now);
	sprintf (buf, "%04d%02d%02d %02d:%02d:%02d", 
			timeinfo->tm_year+1900, timeinfo->tm_mon+1, timeinfo->tm_mday, 
			timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}


void rescueTick (int sock, char *mid, char *pid, char *price, char *qty)
{
	sqlite3 *db;
	int rc, rows, cols, i, j; 
	char **result;
	char tickmsg[512] = {0x00};
	char sqlStr[512] = {0x00};
		
	pthread_mutex_lock (&mutex);
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READONLY, NULL);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "rescue sqlite3_open_v2 fail!! msg=%s, sqlite msg=%s", 
				strerror (errno), sqlite3_errstr (rc));
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);
		return;
	}
	//Tick -- get seq, mid
	snprintf (sqlStr, sizeof (sqlStr), "select seq, mid from %c_msg where pid = '%s' "
					"order by seq desc limit 1;", mid[0], pid);
	LOG_INFO (gLog, "rescue sql : %s", sqlStr);
	rc = sqlite3_get_table (db, sqlStr, &result, &rows, &cols, 0);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "rescue rqlite3_get_table fail!! tickmsg=%s, sqlite tickmsg=%s, sql=%s", 
				strerror (errno), sqlite3_errstr (rc), sqlStr);
		sqlite3_close (db);
		rc = nn_send (sock, "parameter error", 15, 0); 
		pthread_mutex_unlock (&mutex);
		return;
	}

	LOG_INFO (gLog, "Tick rows=%d, cols=%d", rows, cols);
	for (i = 1; i < rows+1; i++)
	{
		sprintf (&tickmsg[strlen (tickmsg)], "%s%c", "8=FIX.4.2", 0x01);
		sprintf (&tickmsg[strlen (tickmsg)], "%s%c", "35=T", 0x01);
		for (j = 0; j < cols; j++)
		{
			switch (j)
			{
				case 0:
					sprintf (&tickmsg[strlen (tickmsg)], "%s", "34="); break;
				case 1:
					sprintf (&tickmsg[strlen (tickmsg)], "%s", "3="); break;
				default: break;
			}
			if (result[i*cols+j])
				sprintf (&tickmsg[strlen (tickmsg)], "%s", result[i*cols+j]);

			sprintf (&tickmsg[strlen (tickmsg)], "%c", 0x01);
		}
	}

	if (rows >= 1)
	{
		sprintf (&tickmsg[strlen (tickmsg)], "5=%s%c", pid, 0x01);
		sprintf (&tickmsg[strlen (tickmsg)], "21=%s%c", price, 0x01);
		sprintf (&tickmsg[strlen (tickmsg)], "22=%s%c", qty, 0x01);
		char localtime[32] = {0x00};
		getFixNowString (localtime);
		sprintf (&tickmsg[strlen (tickmsg)], "18=%s%c", localtime, 0x01);
		sprintf (&tickmsg[strlen (tickmsg)], "10=%c", 0x01);
	
		publish (tickmsg, strlen (tickmsg));
		LOG_INFO (gLog, "rescue publish msg : %s", tickmsg);
	}
	sqlite3_free_table (result);
	sqlite3_close (db);

	pthread_mutex_unlock (&mutex);

	return;
}

void rescueQuote (int sock, char *mid, char *pid, char *price, char *qty)
{
	sqlite3 *db;
	int rc, rows, cols, i, j; 
	char sqlStr[512] = {0x00};
		
	pthread_mutex_lock (&mutex);
	//Quote
	char **quoteresult;
	char quotemsg[512] = {0x00};
	rc = sqlite3_open_v2 ("rts.db", &db, SQLITE_OPEN_READWRITE, NULL);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "rescue sqlite3_open_v2 fail!! msg=%s, sqlite msg=%s", 
				strerror (errno), sqlite3_errstr (rc));
		sqlite3_close (db);
		pthread_mutex_unlock (&mutex);
		return;
	}
	rows = 0; cols = 0;
	memset (sqlStr, 0, sizeof (sqlStr));
	snprintf (sqlStr, sizeof (sqlStr), "update quote set last_price='%s', last_qty='%s' where pid='%s';"
			"select * from quote where pid= '%s' limit 1;", price, qty, pid, pid);
	//snprintf (sqlStr, sizeof (sqlStr), "select * from quote limit 2;");
	LOG_INFO (gLog, "rescue sql : %s", sqlStr);
	rc = sqlite3_get_table (db, sqlStr, &quoteresult, &rows, &cols, 0);
	if (rc != SQLITE_OK)
	{
		LOG_ERROR (gLog, "rescue rqlite3_get_table fail!! quotemsg=%s, sqlite quotemsg=%s, sql=%s", 
				strerror (errno), sqlite3_errstr (rc), sqlStr);
		sqlite3_close (db);
		rc = nn_send (sock, "parameter error", 15, 0); 
		pthread_mutex_unlock (&mutex);
		return;
	}

	LOG_INFO (gLog, "Quote rows=%d, cols=%d", rows, cols);
	for (i = 1; i < rows+1; i++)
	{
		sprintf (&quotemsg[strlen (quotemsg)], "%s%c", "8=FIX.4.2", 0x01);
		sprintf (&quotemsg[strlen (quotemsg)], "%s%c", "35=Q", 0x01);
		//LOG_INFO (gLog, "quote msg : %s", quotemsg);
		for (j = 0; j < cols; j++)
		{
			switch (j)
			{
				case 0:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "3="); break;
				case 1:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "5="); break;
				case 2: // last_price
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "407="); break;
				case 3: // last_qty
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "15007="); break;
				case 4:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "400="); break;
				case 5: // high
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "388="); break;
				case 6: // low
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "394="); break;
				case 7: // price
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "385="); break;
				case 8:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "22="); break;
				case 9:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "15008="); break;
				case 10:
					sprintf (&quotemsg[strlen (quotemsg)], "%s", "15006="); break;
				default: break;
			}
			if (quoteresult[i*cols+j])
			{
				if (j == 2)
				{
					sprintf (&quotemsg[strlen (quotemsg)], "%s", price);
				}
				else if (j == 3)
				{
					sprintf (&quotemsg[strlen (quotemsg)], "%s", qty);
				}
				else
					sprintf (&quotemsg[strlen (quotemsg)], "%s", quoteresult[i*cols+j]);

				//LOG_INFO (gLog, "11 quote msg : %s, %s", quotemsg, quoteresult[i*cols+j]);
			}

			sprintf (&quotemsg[strlen (quotemsg)], "%c", 0x01);
		}
	}

	if (rows >= 1)
	{
		sprintf (&quotemsg[strlen (quotemsg)], "34=1%c", 0x01);
		sprintf (&quotemsg[strlen (quotemsg)], "10=000%c", 0x01);
	
		publish (quotemsg, strlen (quotemsg));
		LOG_INFO (gLog, "rescue publish msg : %s", quotemsg);
	}
	sqlite3_free_table (quoteresult);
	sqlite3_close (db);
	pthread_mutex_unlock (&mutex);

	return;
}
