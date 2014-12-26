#include "apexRTS.h"
#include <assert.h> 
#include <stdio.h> 
#include <string.h> 
#include <sqlite3.h> 
#include "nanomsg/nn.h"
#include "nanomsg/reqrep.h"
#include "log.h"
#include "dbHandle.h"

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

		//2. query db
		sqlite3 *db;
		char **result;
		int rows, cols, i;
		char msg[2048] = {0x00};
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
		if (strcmp (type, "Q") == 0)
		{
			if (strlen (startSeq) == 0)
				startSeq[0] = '0';
			snprintf (sqlStr, 512, "select * from quote where mid like '%%%s%%' and pid like '%%%s%%' "
					"limit %s, 10;", mid, pid, startSeq);
		}
		else
		{
			snprintf (sqlStr, 512, "select * from msg where mid like '%%%s%%'	and pid like '%%%s%%' "
					"and type like '%%%s%%' %s %s order by seq limit %s10;", 
					mid, pid, type, startCondition, endCondition, limitCondition);
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
			for (i = 1; i < rows + 1; i++)
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
			for (i = 1; i < rows + 1; i++)
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
	}
	
}
