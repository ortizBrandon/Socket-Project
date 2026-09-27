CC		= gcc
CFLAGS	= -Wall -Wextra -g

all: manager

manager: manager.c protocol.h
	$(CC) $(CFLAGS) -o manager manager.c

clean:
	rm -f manager

.PHONY: all clean