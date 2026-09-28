CC		= gcc
CFLAGS	= -Wall -Wextra -g

all: manager peer

manager: manager.c protocol.h
	$(CC) $(CFLAGS) -o manager manager.c

peer: peer.c protocol.h
	$(CC) $(CFLAGS) -o peer peer.c

clean:
	rm -f manager peer

.PHONY: all clean