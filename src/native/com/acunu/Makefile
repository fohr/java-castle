CC= gcc
LD= ld
PROGNAME= libCastleImpl.so 
SRC = CastleImpl

INCLUDES= -I/usr/lib/jvm/java/include -I/usr/lib/jvm/java/include/linux -I$(JNI_HEADERS)

CFLAGS= -Wall -Werror -g -fPIC -c $(INCLUDES)
CFLAGS+= $(INCLUDES)
LDFLAGS = -G $(SRC).o -lm -lc -lpthread -lcastle -o $(PROGNAME)

all:
	$(CC) $(CFLAGS) $(SRC).c
	$(LD) $(LDFLAGS)
