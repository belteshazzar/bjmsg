/*
 * main.c — subcommand dispatch for the single bjmsg executable.
 *
 * The broker and the clients live in one binary on purpose: a node that
 * both serves subjects and subscribes to another node's is then a matter
 * of running both halves in one process, since http11c_poll embeds in a
 * host's own loop rather than owning it.
 */
#include "bjmsg.h"

#include <curl/curl.h>

#include <stdlib.h>
#include <string.h>

#define DEFAULT_HOST "127.0.0.1"
#define DEFAULT_PORT 8080
#define DEFAULT_DIR  "./bjmsg-data"

static void usage(void) {
    fprintf(stderr,
        "bjmsg — publish/subscribe over HTTP/1.1 with binjson payloads\n"
        "\n"
        "usage:\n"
        "  bjmsg serve       [--host H] [--port P] [--dir D]\n"
        "                    [--dedup-window SECONDS]\n"
        "  bjmsg pub         [--url URL] <subject> (<text> | --int N | --file PATH)\n"
        "  bjmsg sub         [--url URL] <subject> [--consumer NAME] [--tail]\n"
        "                    [--port N] [--callback URL] [--exec CMD] [--keep]\n"
        "\n"
        "job queues (each message goes to one group member):\n"
        "  bjmsg work        [--url URL] <subject> --group G --exec CMD [--max N]\n"
        "  bjmsg take        [--url URL] <subject> --group G [--max N]\n"
        "  bjmsg done        [--url URL] <subject> --group G --index N\n"
        "  bjmsg fail        [--url URL] <subject> --group G --index N\n"
        "  bjmsg queue       [--url URL] <subject> [--group G [--lease D]]\n"
        "  bjmsg dead        [--url URL] <subject>\n"
        "  bjmsg requeue     [--url URL] <subject> --index N\n"
        "  bjmsg pipe        [--url URL] <in> --consumer N --to <out> --exec CMD\n"
        "  bjmsg request     [--url URL] <subject> <text> [--timeout D]\n"
        "  bjmsg reply       [--url URL] <subject> --exec CMD [--group G]\n"
        "\n"
        "query a running broker and exit:\n"
        "  bjmsg health      [--url URL]\n"
        "  bjmsg push        [--url URL] [--consumer NAME --delete]\n"
        "  bjmsg subjects    [--url URL]\n"
        "  bjmsg info        [--url URL] <subject>\n"
        "  bjmsg consumers   [--url URL] <subject>\n"
        "  bjmsg unsubscribe [--url URL] <subject> --consumer NAME\n"
        "  bjmsg trim        [--url URL] <subject> (--before N | --keep N)\n"
        "  bjmsg seek        [--url URL] <subject> --consumer NAME --index N\n"
        "  bjmsg policy      [--url URL] [<subject> [--max-age D]\n"
        "                    [--max-messages N] [--max-bytes S] [--clear]]\n"
        "\n"
        "`sub` receives rather than polls: it starts a small HTTP server and\n"
        "registers it as the callback the broker POSTs messages to.\n"
        "\n"
        "A --consumer subscription is durable: the broker persists a read\n"
        "receipt, so rejoining resumes after the last acknowledged message.\n"
        "\n"
        "Every client waits and retries when the broker cannot be reached;\n"
        "--retry MS sets the wait, and --retry 0 turns it off.\n"
        "\n"
        "defaults: --host " DEFAULT_HOST ", --port 8080, --dir " DEFAULT_DIR ",\n"
        "          --url http://127.0.0.1:8080, --retry 5000\n");
}

static int cmd_serve(int argc, char **argv) {
    const char *host = DEFAULT_HOST, *dir = DEFAULT_DIR;
    int port = DEFAULT_PORT;
    long dedup_window_s = 0;

    for (int i = 0; i < argc; i++) {
        const char *a = argv[i];
        int last = (i + 1 >= argc);
        if (strcmp(a, "--host") == 0 && !last)      host = argv[++i];
        else if (strcmp(a, "--port") == 0 && !last) port = atoi(argv[++i]);
        else if (strcmp(a, "--dir") == 0 && !last)  dir = argv[++i];
        else if (strcmp(a, "--dedup-window") == 0 && !last)
            dedup_window_s = atol(argv[++i]);
        else if (strcmp(a, "-h") == 0 || strcmp(a, "--help") == 0) { usage(); return 0; }
        else { fprintf(stderr, "bjmsg: bad argument %s\n", a); return 2; }
    }
    return bjm_serve(host, port, dir, (uint64_t)dedup_window_s * 1000);
}

int main(int argc, char **argv) {
    if (argc < 2) { usage(); return 2; }

    const char *cmd = argv[1];
    int rest_argc = argc - 2;
    char **rest_argv = argv + 2;

    if (strcmp(cmd, "-h") == 0 || strcmp(cmd, "--help") == 0 ||
        strcmp(cmd, "help") == 0) {
        usage();
        return 0;
    }
    /* Both halves need libcurl's global state now: the broker is an HTTP
     * client too, because that is how it delivers to a callback. */
    if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
        fprintf(stderr, "bjmsg: curl_global_init failed\n");
        return 1;
    }

    if (strcmp(cmd, "serve") == 0) {
        int rc = cmd_serve(rest_argc, rest_argv);
        curl_global_cleanup();
        return rc;
    }

    static const struct {
        const char *name;
        int (*run)(int, char **);
    } clients[] = {
        { "pub",         bjm_cmd_pub },
        { "sub",         bjm_cmd_sub },
        { "push",        bjm_cmd_push },
        { "health",      bjm_cmd_health },
        { "subjects",    bjm_cmd_subjects },
        { "info",        bjm_cmd_info },
        { "consumers",   bjm_cmd_consumers },
        { "unsubscribe", bjm_cmd_unsubscribe },
        { "trim",        bjm_cmd_trim },
        { "seek",        bjm_cmd_seek },
        { "policy",      bjm_cmd_policy },
        { "queue",       bjm_cmd_queue },
        { "take",        bjm_cmd_take },
        { "done",        bjm_cmd_done },
        { "fail",        bjm_cmd_fail },
        { "work",        bjm_cmd_work },
        { "dead",        bjm_cmd_dead },
        { "requeue",     bjm_cmd_requeue },
        { "pipe",        bjm_cmd_pipe },
        { "request",     bjm_cmd_request },
        { "reply",       bjm_cmd_reply },
    };

    for (size_t i = 0; i < sizeof clients / sizeof *clients; i++) {
        if (strcmp(cmd, clients[i].name) != 0) continue;
        int rc = clients[i].run(rest_argc, rest_argv);
        curl_global_cleanup();
        return rc;
    }

    curl_global_cleanup();
    fprintf(stderr, "bjmsg: unknown command '%s'\n\n", cmd);
    usage();
    return 2;
}
