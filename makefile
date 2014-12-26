CC = gcc
INC = -I .
CFLAGS = -Wall
LDFLAGS = -lpthread -lzmq -lnanomsg -lconfig -lsqlite3 -llog4c

apexRTS: apexRTS.o dbHandle.o publish.o reply.o config.o common.o
	$(CC) -o apexRTS apexRTS.o dbHandle.o publish.o reply.o config.o common.o $(INC) $(CFLAGS) $(LDFLAGS)
apexRTS.o: apexRTS.c publish.h reply.h dbHandle.h common.h
	$(CC) apexRTS.c $(CFLAGS) -c
dbHandle.o: dbHandle.c
	$(CC) dbHandle.c $(CFLAGS) -c
publish.o: publish.c
	$(CC) publish.c $(CFLAGS) -c
reply.o: reply.c
	$(CC) reply.c $(CFLAGS) -c
config.o: config.c
	$(CC) config.c $(CFLAGS) -c
common.o: common.c
	$(CC) common.c $(CFLAGS) -c
clean:
	@rm -rf *.o
