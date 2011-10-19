CC= gcc
LD= ld

CFLAGS= -Wall -Werror -g -D_FORTIFY_SOURCE=2 -O2

all:
	rm -f err_enum_class_gen CastleError.java
	$(CC) $(CFLAGS) err_enum_class_gen.c -o err_enum_class_gen
	./err_enum_class_gen CastleError.java
