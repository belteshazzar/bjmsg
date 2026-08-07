CC       ?= cc
CFLAGS   ?= -std=c11 -Wall -Wextra -O2 -g

# BJIO_REQUIRE_SYNC makes binjson-structures reject, at open time, any io
# that is writable but cannot fsync. A shipping server wants that: without
# it a misconfigured adapter is silently not durable.
CPPFLAGS += -DBJIO_REQUIRE_SYNC \
            -Iinclude \
            -Ithird_party/binjson/include \
            -Ithird_party/binjson-structures/include \
            -Ithird_party/http11c/include

CURL_CFLAGS := $(shell curl-config --cflags 2>/dev/null)
CURL_LIBS   := $(shell curl-config --libs 2>/dev/null || echo -lcurl)

SRC := src/main.c src/server.c src/store.c src/push.c src/client.c src/bjtext.c

# One copy of binjson, shared by our code and by binjson-structures —
# binjson-structures' own nested third_party/binjson stays uninitialised
# so two copies can never end up linked into the same binary.
THIRD_PARTY := third_party/binjson/src/binjson.c \
               third_party/binjson-structures/src/bjfile.c \
               third_party/binjson-structures/src/entrylog.c \
               third_party/binjson-structures/src/bplustree.c \
               third_party/binjson-structures/src/bjio_posix.c \
               third_party/http11c/src/http11c.c

OBJ := $(patsubst %.c,build/%.o,$(SRC) $(THIRD_PARTY))

all: bin/bjmsg

bin/bjmsg: $(OBJ)
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $^ $(CURL_LIBS) -o $@

# Our own sources are warning-free and stay that way.
build/src/%.o: src/%.c include/bjmsg.h
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) -Werror $(CPPFLAGS) $(CURL_CFLAGS) -c $< -o $@

# Vendored sources build without -Werror: a warning introduced by someone
# else's compiler upgrade should not break our build.
build/third_party/%.o: third_party/%.c
	@mkdir -p $(@D)
	$(CC) $(CFLAGS) $(CPPFLAGS) -c $< -o $@

clean:
	rm -rf build bin

.PHONY: all clean
