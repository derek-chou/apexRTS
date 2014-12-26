#include <time.h>
#include <stdio.h>
#include <sys/time.h>
#include <string.h>

void getNowString (char *buf)
{
	time_t now;
	struct tm *timeinfo;
	struct timeval tv;
	
	gettimeofday (&tv, NULL);
	time (&now);
	timeinfo = localtime (&now);
	sprintf (buf, "%04d/%02d/%02d %02d:%02d:%02d %06ld", 
			timeinfo->tm_year+1900, timeinfo->tm_mon+1, timeinfo->tm_mday, 
			timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec, tv.tv_usec);
}

void getNowHHMM (char *buf)
{
	time_t now;
	struct tm *timeinfo;
	
	time (&now);
	timeinfo = localtime (&now);
	sprintf (buf, "%02d%02d", timeinfo->tm_hour, timeinfo->tm_min);
}

void findTagValue (char *msg, char *tag, char *result, int resultLen)
{
	int msgLen = strlen (msg);
	if (msgLen < 0) 
		return;
	int tagLen = strlen (tag);
	if (tagLen > 31) 
		return;
	if (msgLen < tagLen) 
		return;

	char findStr[32] = {0x00};
	findStr[0] = 0x01;
	memcpy (&findStr[1], tag, tagLen);
	char *p = strstr (msg, findStr);
	int pos = (p) ? (p - msg) : -1;
	if(pos > 0)
	{
		int i, end = 0;
		for(i = pos+1; i < msgLen; i++)
		{
			if (msg[i] == 0x01)
			{
				end = i;
				break;
			}
		}
		int valueLen = end - (pos + tagLen + 1);
		int copyLen = (valueLen > resultLen) ? resultLen : valueLen;
		if (end > pos+tagLen+1)
			memcpy (result, &msg[pos+tagLen+1], copyLen);
	}
}
