#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/select.h>
#include <time.h>
#include <glob.h>

#include "protocol.h"

// p2p message types
#define P2P_SET_ID 1
#define P2P_STORE 2
#define P2P_DONE 3
#define P2P_FIND_EVENT 4
#define P2P_FIND_RESULT 5
#define P2P_CLEAR 6
#define P2P_REBUILD_REQUEST 7
#define P2P_RECONFIG 8
#define P2P_REBUILD_READY 9
#define P2P_STORE_ACK 10
#define CLEAR_TEARDOWN 1
#define CLEAR_REBUILD 2

// local hash table
#define MAX_TABLE 200000
static char *myTable[MAX_TABLE]; // each slot holds a separately allocated record
static int myTableSize = 0;
static int myCount = 0;
static int expectedRecords = 0;
static int populateFailures = 0;
static int myTableEventIDs[MAX_TABLE];

// global state
static char peerName[MAX_NAME_LEN] = "";
static char peerIP[INET_ADDRSTRLEN] = "";
static int mPort = 0, pPort = 0;

static char managerIP[INET_ADDRSTRLEN] = "";
static int managerPort = 0;

static int managerSocket = -1;
static int peerSocket = -1;

static int peerID = -1;
static int dhtSize = 0;
static int dhtBuilt = 0;

static Tuple dhtPeers[MAX_PEERS];
static int numDhtPeers = 0;
static Tuple rightNeighbor;
static int haveRightNeighbor = 0;
static int registered = 0;
static Tuple queryStart;
static int haveQueryStart = 0;
static int pendingChurn = 0;
static char expectedLeader[MAX_NAME_LEN] = "";
static Tuple rebuiltRequester;
static int rebuildIsRunning = 0;

// p2p message
typedef struct
{
    int type;
    int peerID;
    int dhtSize;
    int tableSize;      // for set_id so peers know s
    int pos;            // for store
    EventRecord record; // complete record for store
    int tupleCount;
    Tuple tuples[MAX_PEERS]; // for set_id
    int counts[MAX_PEERS];   // for done
    int doneCount;

    int eventID;            // event id being searched for
    int visited[MAX_PEERS]; // ids already checked during this query
    int visitedCount;       // num of peers already visited
    int found;              // 1 if event was found, else 0
    char requesterIP[INET_ADDRSTRLEN];
    int requesterPort;
    int hops;
    int mode;
    int year;
    int status;
    int expectedCount;
    Tuple requesterTuple;
    char newLeader[MAX_NAME_LEN];
} PeerMessage;

static PeerMessage rebuildPlan;

// prototypes
static int build_manager_addr(struct sockaddr_in *addr);
static int send_to_manager(Message *m); // send and wait for manager reply
static void cmd_register(const char *line);
static void cmd_setup_dht(const char *line);
static void handle_set_id(PeerMessage *m);
static void handle_store(PeerMessage *m);
static void handle_done(PeerMessage *m);
static void send_to_peer(const Tuple *t, PeerMessage *m);
static int is_prime(int x);
static int next_prime_above(int x);
static int count_csv_lines(int year);
static int csv_input_path(int year, char *path, size_t size);
static int csv_project_record(const char *record, int raw, char *output, size_t size, int *eventID);
static int csv_read_record(FILE *stream, char **line, size_t *size);
static void populate_dht(int year, int n);
static void send_dht_complete(void);
static void print_config(void);
static int store_with_linear_probing(int startPos, int eventID, const char *csvLine);
static void cmd_query_dht(const char *line);
static void handle_find_event(PeerMessage *pm);
static int find_event_local(int eventID, char *result, size_t resultSize);
static int get_event_id_from_csv(const char *line);
static void cmd_find_local_event(int eventID);
static void send_find_result(PeerMessage *pm);
static void handle_find_result(PeerMessage *pm);
static void cmd_find_event(int eventID);
static void clear_table(void);
static void reset_ring(void);
static void cmd_leave_or_join(const char *line, int leaving);
static void cmd_teardown(const char *line);
static int cmd_deregister(const char *line);
static void handle_clear(PeerMessage *pm);
static void handle_rebuild_request(PeerMessage *pm);
static void handle_reconfig(PeerMessage *pm);
static void handle_rebuild_ready(PeerMessage *pm);
static void print_labeled_event(const char *csv);
static void notify_manager(int type);
static int wait_for_store_ack(int eventID, const Tuple *destination, PeerMessage *message);

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <manager-ip> <manager-port>\n", argv[0]);
        return 1;
    }

    snprintf(managerIP, sizeof(managerIP), "%s", argv[1]);
    managerPort = atoi(argv[2]);
    srand((unsigned)time(NULL) ^ (unsigned)getpid());

    managerSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (managerSocket < 0)
    {
        perror("socket manager");
        return 1;
    }

    peerSocket = socket(AF_INET, SOCK_DGRAM, 0);
    if (peerSocket < 0)
    {
        perror("socket peer");
        close(managerSocket);
        return 1;
    }

    // limit blocking waits on the peer socket
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(peerSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct timeval managerTimeout = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(managerSocket, SOL_SOCKET, SO_RCVTIMEO, &managerTimeout, sizeof(managerTimeout));
    setvbuf(stdout, NULL, _IOLBF, 0);
    printf("Peer started. Manager: %s:%d\n", managerIP, managerPort);

    char command[256];

    while (1)
    {

        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(STDIN_FILENO, &rfds);
        FD_SET(peerSocket, &rfds);
        int maxfd = peerSocket > STDIN_FILENO ? peerSocket : STDIN_FILENO;

        if (select(maxfd + 1, &rfds, NULL, NULL, NULL) < 0)
        {
            perror("select");
            break;
        }

        // stdin ready
        if (FD_ISSET(STDIN_FILENO, &rfds))
        {
            printf("peer> ");

            if (fgets(command, sizeof(command), stdin) == NULL)
                break;
            command[strcspn(command, "\n")] = '\0';
            if (strlen(command) == 0)
                continue;

            if (strncmp(command, "register", 8) == 0)
            {
                cmd_register(command);
            }
            else if (strncmp(command, "setup-dht", 9) == 0)
            {
                cmd_setup_dht(command);
            }
            else if (strncmp(command, "query-dht", 9) == 0)
            {
                cmd_query_dht(command);
            }
            else if (strncmp(command, "find-local ", 11) == 0)
            {
                int eventID;

                if (sscanf(command + 11, "%d", &eventID) == 1)
                {
                    cmd_find_local_event(eventID);
                }
                else
                {
                    printf("Usage: find-local <EVENT_ID>\n");
                }
            }
            else if (strncmp(command, "find-event ", 11) == 0)
            {
                int eventID;
                char extra;

                if (sscanf(command + 11, "%d %c", &eventID, &extra) == 1)
                {
                    cmd_find_event(eventID);
                }
                else
                {
                    printf("Usage: find-event <EVENT_ID>\n");
                }
            }
            else if (strncmp(command, "leave-dht", 9) == 0)
            {
                cmd_leave_or_join(command, 1);
            }
            else if (strncmp(command, "join-dht", 8) == 0)
            {
                cmd_leave_or_join(command, 0);
            }
            else if (strncmp(command, "teardown-dht", 12) == 0)
            {
                cmd_teardown(command);
            }
            else if (strncmp(command, "deregister", 10) == 0)
            {
                if (cmd_deregister(command)) break;
            }
            else if (strcmp(command, "config") == 0)
            {
                print_config();
            }
            else if (strcmp(command, "quit") == 0)
            {
                break;
            }
            else
            {
                printf("Unknown command.\n");
            }
        }

        // peer socket ready
        if (FD_ISSET(peerSocket, &rfds))
        {
            PeerMessage pm;
            struct sockaddr_in from;
            socklen_t fromlen = sizeof(from);

            ssize_t r = recvfrom(peerSocket, &pm, sizeof(pm), 0,
                                 (struct sockaddr *)&from, &fromlen);
            if (r < 0)
            {
                perror("recvfrom peer");
                continue;
            }

            switch (pm.type)
            {
            case P2P_SET_ID:
                handle_set_id(&pm);
                break;
            case P2P_STORE:
                handle_store(&pm);
                break;
            case P2P_DONE:
                handle_done(&pm);
                break;
            case P2P_FIND_EVENT:
                handle_find_event(&pm);
                break;
            case P2P_FIND_RESULT:
                handle_find_result(&pm);
                break;
            case P2P_CLEAR:
                handle_clear(&pm);
                break;
            case P2P_REBUILD_REQUEST:
                handle_rebuild_request(&pm);
                break;
            case P2P_RECONFIG:
                handle_reconfig(&pm);
                break;
            case P2P_REBUILD_READY:
                handle_rebuild_ready(&pm);
                break;
            case P2P_STORE_ACK:
                break;
            default:
                printf("Unknown P2P type %d\n", pm.type);
            }
        }
    }

    close(managerSocket);
    close(peerSocket);
    return 0;
}

static int build_manager_addr(struct sockaddr_in *addr)
{
    memset(addr, 0, sizeof(*addr));
    addr->sin_family = AF_INET;
    addr->sin_port = htons(managerPort);
    return inet_pton(AF_INET, managerIP, &addr->sin_addr);
}

static int send_to_manager(Message *m)
{
    struct sockaddr_in addr;
    if (build_manager_addr(&addr) <= 0)
        return -1;

    if (sendto(managerSocket, m, sizeof(*m), 0,
               (struct sockaddr *)&addr, sizeof(addr)) < 0)
    {
        perror("sendto manager");
        return -1;
    }

    Message reply;
    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);
    if (recvfrom(managerSocket, &reply, sizeof(reply), 0,
                 (struct sockaddr *)&from, &fromlen) < 0)
    {
        perror("recvfrom manager");
        return -1;
    }

    *m = reply;
    return 0;
}

static void cmd_register(const char *line)
{
    char name[MAX_NAME_LEN], ip[INET_ADDRSTRLEN];
    int m, p;

    if (sscanf(line, "register %15s %15s %d %d", name, ip, &m, &p) != 4)
    {
        printf("Usage: register <name> <ip> <m-port> <p-port>\n");
        return;
    }

    if (registered)
    {
        printf("Peer already registered as '%s'\n", peerName);
        return;
    }
    snprintf(peerName, sizeof(peerName), "%s", name);
    snprintf(peerIP, sizeof(peerIP), "%s", ip);
    mPort = m;
    pPort = p;

    struct sockaddr_in a;

    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = INADDR_ANY;
    a.sin_port = htons(mPort);
    if (bind(managerSocket, (struct sockaddr *)&a, sizeof(a)) < 0)
    {
        perror("bind manager socket");
        return;
    }

    memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = INADDR_ANY;
    a.sin_port = htons(pPort);
    if (bind(peerSocket, (struct sockaddr *)&a, sizeof(a)) < 0)
    {
        perror("bind peer socket");
        return;
    }

    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_REGISTER;
    snprintf(msg.peer_name, sizeof(msg.peer_name), "%s", peerName);
    snprintf(msg.ip, sizeof(msg.ip), "%s", peerIP);
    msg.m_port = mPort;
    msg.p_port = pPort;

    printf("Peer: registering '%s'...\n", peerName);
    if (send_to_manager(&msg) < 0)
        return;

    if (msg.return_code == RC_SUCCESS) {
        registered = 1;
        printf("Peer: registration SUCCESS\n");
    } else
        printf("Peer: registration FAILURE\n");
}

static void cmd_setup_dht(const char *line)
{
    char name[MAX_NAME_LEN];
    int n, year;

    if (sscanf(line, "setup-dht %15s %d %d", name, &n, &year) != 3)
    {
        printf("Usage: setup-dht <peer-name> <n> <YYYY>\n");
        return;
    }
    if (strcmp(name, peerName) != 0)
    {
        printf("This peer is registered as '%s'\n", peerName);
        return;
    }

    if (!registered || peerID >= 0 || n < 3 || n > MAX_PEERS) {
        printf("setup-dht: invalid state or ring size\n");
        return;
    }
    // compute table size from csv before sending set-id
    if (count_csv_lines(year) < 0) return;

    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_SETUP_DHT;
    snprintf(msg.peer_name, sizeof(msg.peer_name), "%s", peerName);
    msg.n = n;
    msg.year = year;

    printf("Peer: sending setup-dht to manager\n");
    if (send_to_manager(&msg) < 0)
        return;

    if (msg.return_code != RC_SUCCESS)
    {
        printf("Peer: setup-dht FAILURE\n");
        return;
    }
    dhtBuilt = 0;
    haveQueryStart = 0;

    numDhtPeers = msg.tuple_count;
    for (int i = 0; i < numDhtPeers; i++)
        dhtPeers[i] = msg.tuples[i];

    peerID = 0; // leader is always 0
    dhtSize = numDhtPeers;
    rightNeighbor = dhtPeers[1 % dhtSize];
    haveRightNeighbor = 1;


    // send set-id to every non-leader peer
    for (int i = 1; i < numDhtPeers; i++)
    {
        PeerMessage pm;
        memset(&pm, 0, sizeof(pm));
        pm.type = P2P_SET_ID;
        pm.peerID = i;
        pm.dhtSize = numDhtPeers;
        pm.tableSize = myTableSize;
        pm.tupleCount = numDhtPeers;
        for (int j = 0; j < numDhtPeers; j++)
            pm.tuples[j] = dhtPeers[j];

        printf("Peer: sending set-id (id=%d) to '%s'\n",
               i, dhtPeers[i].peer_name);
        send_to_peer(&dhtPeers[i], &pm);
    }

    // allow peers to receive their assigned ids
    usleep(500000); // 500 ms

    // read csv and push stores around the ring
    populate_dht(year, n);

    // send done around the ring to collect counts
    PeerMessage done;
    memset(&done, 0, sizeof(done));
    done.type = P2P_DONE;
    done.doneCount = 1;
    done.counts[0] = myCount; // slot 0 is the leader's own count

    printf("Peer: sending done-request around ring\n");
    send_to_peer(&rightNeighbor, &done);

    // block until done returns
    int doneCompleted = 0;
    while (1)
    {
        PeerMessage back;
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);

        ssize_t r = recvfrom(peerSocket, &back, sizeof(back), 0,
                             (struct sockaddr *)&from, &fromlen);
        if (r < 0)
        {
            perror("recvfrom done");
            break;
        }

        if (back.type == P2P_DONE)
        {
            int total = 0;
            for (int j = 0; j < back.doneCount; ++j) total += back.counts[j];
            doneCompleted = back.doneCount == dhtSize &&
                            total == expectedRecords && !populateFailures;
            printf("\nPeer: ===== records stored per peer =====\n");
            for (int i = 0; i < back.doneCount; i++)
            {
                printf("  peer id %d (%s): %d records\n",
                       i, dhtPeers[i].peer_name, back.counts[i]);
            }
            printf("Peer: =====================================\n");
            printf("Peer: total %d / expected %d records\n", total, expectedRecords);
            break;
        }
        if (back.type == P2P_STORE)
            handle_store(&back);
    }

    // signal completion to the manager
    if (doneCompleted) send_dht_complete();
    else fprintf(stderr, "Peer: setup incomplete; NOT sending dht-complete\n");
}

static void handle_set_id(PeerMessage *m)
{
    clear_table();
    peerID = m->peerID;
    dhtSize = m->dhtSize;
    myTableSize = m->tableSize;

    numDhtPeers = m->tupleCount;
    for (int i = 0; i < numDhtPeers; i++)
        dhtPeers[i] = m->tuples[i];

    int rightIdx = (peerID + 1) % dhtSize;
    rightNeighbor = dhtPeers[rightIdx];
    haveRightNeighbor = 1;

    printf("\n*** received set-id: id=%d  ring=%d  rightNeighbor='%s'\n",
           peerID, dhtSize, rightNeighbor.peer_name);
}

static int store_with_linear_probing(int startPos, int eventID, const char *csvLine)
{
    if (myTableSize <= 0)
        return -1;

    // start at the original hash position.
    for (int i = 0; i < myTableSize; i++)
    {
        int pos = (startPos + i) % myTableSize;

        if (myTable[pos] == NULL)
        {
            myTable[pos] = strdup(csvLine);

            if (myTable[pos] == NULL)
            {
                perror("strdup");
                return -1;
            }

            myTableEventIDs[pos] = eventID;
            myCount++;

            return pos; // return the actual slot used.
        }
    }

    fprintf(stderr, "Peer %s: hash table is full\n", peerName);
    return -1;
}

static int find_event_local(int eventID, char *result, size_t resultSize)
{
    if (myTableSize <= 0 || result == NULL || resultSize == 0)
        return 0;

    int startPos = eventID % myTableSize;

    for (int i = 0; i < myTableSize; i++)
    {
        int pos = (startPos + i) % myTableSize;

        // an empty slot ends linear probing
        if (myTable[pos] == NULL)
            return 0;

        if (myTableEventIDs[pos] == eventID)
        {
            snprintf(result, resultSize, "%s", myTable[pos]);
            return 1;
        }
    }

    return 0;
}

// begin a search from the peer selected by query-dht
static void cmd_find_event(int eventID)
{
    if (eventID <= 0) { printf("Invalid event ID\n"); return; }
    if (!registered || !haveQueryStart)
    {
        printf("First run query-dht %s from a Free peer.\n", peerName);
        return;
    }

    PeerMessage pm;
    memset(&pm, 0, sizeof(pm));

    pm.type = P2P_FIND_EVENT;
    pm.eventID = eventID;

    // this peer as the requester
    snprintf(pm.requesterIP, sizeof(pm.requesterIP), "%s", peerIP);
    pm.requesterPort = pPort;

    pm.visitedCount = 0;

    printf("Searching for event %d starting at peer '%s'...\n",
           eventID, queryStart.peer_name);
    send_to_peer(&queryStart, &pm);
}

static void handle_store(PeerMessage *m)
{
    if (dhtSize <= 0 || myTableSize <= 0)
    {
        return;
    }

    // determine which peer is responsible for this record
    int id = m->pos % dhtSize;

    if (id == peerID)
    {
        char existing[MAX_RECORD_LEN];
        int already = find_event_local(m->record.eventID, existing, sizeof(existing));
        // this peer is responsible for storing the record.
        // use linear probing if the original position is occupied
        int storedPos = already ? m->pos : store_with_linear_probing(
            m->pos, m->record.eventID, m->record.csvLine);
        if (storedPos < 0)
            fprintf(stderr, "Peer %s: failed to store event %d\n", peerName, m->record.eventID);
        if (m->requesterPort > 0)
        {
            PeerMessage ack;
            memset(&ack, 0, sizeof(ack));
            ack.type = P2P_STORE_ACK;
            ack.eventID = m->record.eventID;
            ack.status = storedPos >= 0;
            struct sockaddr_in dest;
            memset(&dest, 0, sizeof(dest));
            dest.sin_family = AF_INET;
            dest.sin_port = htons(m->requesterPort);
            if (inet_pton(AF_INET, m->requesterIP, &dest.sin_addr) == 1)
                sendto(peerSocket, &ack, sizeof(ack), 0, (struct sockaddr *)&dest, sizeof(dest));
        }
    }
    else if (haveRightNeighbor)
    {
        // forward until reaches right peer
        send_to_peer(&rightNeighbor, m);
    }
}

static void handle_done(PeerMessage *m)
{
    if (peerID == 0 && m->doneCount >= dhtSize)
    {
        int total = 0;
        for (int k = 0; k < m->doneCount; ++k) total += m->counts[k];
        if (total != expectedRecords || populateFailures) {
            fprintf(stderr, "Peer: rebuild INCOMPLETE total=%d expected=%d failed=%d\n",
                    total, expectedRecords, populateFailures);
            return;
        }
        dhtBuilt = 1;
        printf("Peer: ===== rebuilt records per peer =====\n");
        for (int i = 0; i < m->doneCount && i < dhtSize; ++i)
            printf("  [%d] %s: %d records\n", i, dhtPeers[i].peer_name, m->counts[i]);
        printf("Peer: ===================================\n");
        if (rebuildIsRunning)
        {
            PeerMessage ready;
            memset(&ready, 0, sizeof(ready));
            ready.type = P2P_REBUILD_READY;
            ready.status = 1;
            snprintf(ready.newLeader, sizeof(ready.newLeader), "%s", peerName);
            send_to_peer(&rebuiltRequester, &ready);
            rebuildIsRunning = 0;
        }
        return;
    }
    dhtBuilt = 1;

    if (m->doneCount < MAX_PEERS)
    {
        m->counts[m->doneCount] = myCount;
        m->doneCount++;
    }

    if (haveRightNeighbor)
    {
        send_to_peer(&rightNeighbor, m);
    }
}

static void send_to_peer(const Tuple *t, PeerMessage *m)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(t->p_port);
    if (inet_pton(AF_INET, t->ip, &addr.sin_addr) <= 0)
        return;

    sendto(peerSocket, m, sizeof(*m), 0,
           (struct sockaddr *)&addr, sizeof(addr));
}

static void send_find_result(PeerMessage *pm)
{
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));

    addr.sin_family = AF_INET;
    addr.sin_port = htons(pm->requesterPort);

    if (inet_pton(AF_INET, pm->requesterIP,
                  &addr.sin_addr) <= 0)
    {
        perror("Invalid requester IP");
        return;
    }

    pm->type = P2P_FIND_RESULT;

    sendto(peerSocket, pm, sizeof(*pm), 0,
           (struct sockaddr *)&addr, sizeof(addr));
}

static int is_prime(int x)
{
    if (x < 2)
        return 0;
    if (x < 4)
        return 1;
    if (x % 2 == 0)
        return 0;
    for (int i = 3; (long)i * i <= x; i += 2)
        if (x % i == 0)
            return 0;
    return 1;
}

static int next_prime_above(int x)
{
    int n = x + 1;
    while (!is_prime(n))
        n++;
    return n;
}

// use the original noaa file when it is available
static int csv_input_path(int year, char *path, size_t size)
{
    char pattern[128];
    snprintf(pattern, sizeof(pattern), "StormEvents_details-ftp_v1.0_d%d_c*", year);

    glob_t results;
    memset(&results, 0, sizeof(results));
    int status = glob(pattern, 0, NULL, &results);
    if (status == 0 && results.gl_pathc > 0)
    {
        snprintf(path, size, "%s", results.gl_pathv[0]);
        globfree(&results);
        return 1;
    }
    globfree(&results);

    snprintf(path, size, "details-%d.csv", year);
    FILE *file = fopen(path, "r");
    if (!file)
    {
        fprintf(stderr, "Peer: CSV missing: %s (or StormEvents_details-ftp_v1.0_d%d_c*)\n", path, year);
        return 0;
    }
    fclose(file);
    return 1;
}

// handle quoted csv fields that span more than one line
static int csv_read_record(FILE *stream, char **line, size_t *size)
{
    ssize_t length = getline(line, size, stream);
    if (length < 0) return -1;

    int quotes = 0;
    for (ssize_t i = 0; i < length; ++i)
        if ((*line)[i] == '"') quotes ^= 1;

    while (quotes)
    {
        char *additional = NULL;
        size_t capacity = 0;
        ssize_t extra = getline(&additional, &capacity, stream);
        if (extra < 0)
        {
            free(additional);
            return -1;
        }
        if ((size_t)(length + extra + 1) > *size)
        {
            size_t required = (size_t)(length + extra + 1);
            char *resized = realloc(*line, required);
            if (!resized)
            {
                free(additional);
                return -1;
            }
            *line = resized;
            *size = required;
        }
        memcpy(*line + length, additional, (size_t)extra + 1);
        for (ssize_t i = 0; i < extra; ++i)
            if (additional[i] == '"') quotes ^= 1;
        length += extra;
        free(additional);
    }
    return 1;
}

// select the 14 fields used by the dht without changing the input file
static int csv_project_record(const char *record, int raw, char *output, size_t size, int *eventID)
{
    static const int originalFields[14] = {7, 8, 10, 11, 12, 13, 15, 20, 21, 22, 23, 24, 25, 31};
    int fieldIndex = 0;
    int selected = 0;
    int inQuotes = 0;
    size_t position = 0;
    char field[MAX_RECORD_LEN];
    size_t fieldLength = 0;
    const char *cursor = record;

    while (1)
    {
        char ch = *cursor;
        if (ch == '"')
        {
            if (inQuotes && cursor[1] == '"')
            {
                if (fieldLength < sizeof(field) - 1)
                    field[fieldLength++] = '"';
                cursor += 2;
                continue;
            }
            inQuotes = !inQuotes;
            cursor++;
            continue;
        }
        if ((ch == ',' && !inQuotes) || ch == '\0' || (ch == '\n' && !inQuotes) || (ch == '\r' && !inQuotes))
        {
            int wanted = raw ? originalFields[selected] : selected;
            if (fieldIndex == wanted)
            {
                field[fieldLength] = '\0';
                if (selected == 0)
                {
                    char *after = NULL;
                    long identifier = strtol(field, &after, 10);
                    if (after == field || *after != '\0' || identifier <= 0 || identifier > 2147483647L)
                        return 0;
                    *eventID = (int)identifier;
                }
                if (selected > 0 && position + 1 >= size) return 0;
                if (selected > 0) output[position++] = ',';
                int escape = strpbrk(field, ",\"\n\r") != NULL;
                if (escape)
                {
                    if (position + 1 >= size) return 0;
                    output[position++] = '"';
                }
                for (size_t k = 0; k < fieldLength; ++k)
                {
                    if (position + 2 >= size) return 0;
                    if (field[k] == '"') output[position++] = '"';
                    output[position++] = field[k];
                }
                if (escape)
                {
                    if (position + 1 >= size) return 0;
                    output[position++] = '"';
                }
                selected++;
                if (selected == 14)
                {
                    output[position] = '\0';
                    return 1;
                }
            }
            fieldLength = 0;
            fieldIndex++;
            if (ch == '\0' || ch == '\n' || ch == '\r') break;
            cursor++;
            continue;
        }
        if (fieldLength >= sizeof(field) - 1) return 0;
        field[fieldLength++] = ch;
        cursor++;
    }
    return 0;
}

// count records to calculate the shared hash table size
static int count_csv_lines(int year)
{
    char filename[1024];
    if (!csv_input_path(year, filename, sizeof(filename))) return -1;

    FILE *f = fopen(filename, "r");
    if (!f)
    {
        perror("fopen CSV");
        return -1;
    }

    char *line = NULL;
    size_t capacity = 0;
    if (csv_read_record(f, &line, &capacity) < 0)
    {
        free(line);
        fclose(f);
        return -1;
    }

    int rows = 0;
    while (csv_read_record(f, &line, &capacity) > 0) rows++;
    free(line);
    fclose(f);

    if (rows < 1)
    {
        printf("CSV empty\n");
        return -1;
    }

    myTableSize = next_prime_above(2 * rows);
    if (myTableSize > MAX_TABLE)
    {
        fprintf(stderr, "Dataset too large: prime hash table requires %d slots; MAX_TABLE=%d\n",
                myTableSize, MAX_TABLE);
        return -1;
    }
    expectedRecords = rows;
    printf("Peer: reading CSV %s (%d events), table size s = %d\n", filename, rows, myTableSize);
    return rows;
}

// read the csv and send each record to its assigned peer
static void populate_dht(int year, int n)
{
    char filename[1024];
    if (!csv_input_path(year, filename, sizeof(filename)))
    {
        populateFailures = expectedRecords;
        return;
    }

    FILE *f = fopen(filename, "r");
    if (!f)
    {
        perror("fopen CSV");
        populateFailures = expectedRecords;
        return;
    }

    char *fullLine = NULL;
    size_t lineCapacity = 0;

    // skip csv header.
    if (csv_read_record(f, &fullLine, &lineCapacity) < 0)
    {
        free(fullLine);
        fclose(f);
        populateFailures = expectedRecords;
        return;
    }

    int raw = strncmp(fullLine, "EVENT_ID,", 9) != 0;
    int ell = 0, sent = 0, local = 0, collisions = 0;
    populateFailures = 0;

    while (csv_read_record(f, &fullLine, &lineCapacity) > 0)
    {
        char line[MAX_RECORD_LEN];
        int event_id = 0;
        if (!csv_project_record(fullLine, raw, line, sizeof(line), &event_id))
        {
            fprintf(stderr, "Peer: invalid CSV event in row %d\n", ell + 2);
            collisions++;
            ell++;
            continue;
        }

        // hash function
        int pos = event_id % myTableSize;
        int id = pos % n;

        if (id == peerID)
        {
            // store local records with linear probing.
            int storedPos = store_with_linear_probing(pos, event_id, line);

            if (storedPos >= 0)
                local++;
            else
                collisions++;
        }
        else
        {
            // forward stores through the ring.
            PeerMessage pm;
            memset(&pm, 0, sizeof(pm));

            pm.type = P2P_STORE;
            pm.pos = pos; // preserve the original hash position.
            pm.record.eventID = event_id;
            snprintf(pm.record.csvLine, sizeof(pm.record.csvLine), "%s", line);
            snprintf(pm.requesterIP, sizeof(pm.requesterIP), "%s", peerIP);
            pm.requesterPort = pPort;
            if (wait_for_store_ack(event_id, &rightNeighbor, &pm)) sent++;
            else
            {
                fprintf(stderr, "Peer: FAILED to store event %d after retries\n", event_id);
                collisions++;
            }
        }

        ell++;
    }

    free(fullLine);
    fclose(f);
    populateFailures = collisions;

    printf("Peer: leader stored %d locally, sent %d around ring, "
           "failed stores %d (total rows %d)\n",
           local, sent, collisions, ell);
}

static void send_dht_complete(void)
{
    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_DHT_COMPLETE;
    snprintf(msg.peer_name, sizeof(msg.peer_name), "%s", peerName);

    printf("Peer: sending dht-complete to manager\n");
    if (send_to_manager(&msg) < 0)
        return;

    if (msg.return_code == RC_SUCCESS)
    {
        dhtBuilt = 1;
        printf("Peer: DHT construction COMPLETE\n");
    }
    else
    {
        printf("Peer: dht-complete FAILURE\n");
    }
}

static void print_config(void)
{
    printf("\n===== DHT CONFIG =====\n");
    printf("This peer: %s (%s) m=%d p=%d\n", peerName, peerIP, mPort, pPort);
    printf("  ID=%d  ringSize=%d  tableSize=%d\n", peerID, dhtSize, myTableSize);
    if (haveRightNeighbor)
        printf("  rightNeighbor=%s (%s:%d)\n",
               rightNeighbor.peer_name, rightNeighbor.ip, rightNeighbor.p_port);
    else printf("  rightNeighbor=(none)\n");
    printf("  records stored: %d\n", myCount);
    printf("Ring:\n");
    for (int i = 0; i < numDhtPeers; i++)
    {
        printf("  [%d] %s %s:%d\n",
               i, dhtPeers[i].peer_name, dhtPeers[i].ip, dhtPeers[i].p_port);
    }
    printf("======================\n");
}

static void cmd_query_dht(const char *line)
{
    char command[32];
    char requesterName[MAX_NAME_LEN];

    // read the command arguments
    if (sscanf(line, "%31s %15s", command, requesterName) != 2)
    {
        printf("Usage: query-dht <peer-name>\n");
        return;
    }

    // prepare manager request
    Message msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = MSG_QUERY_DHT;
    snprintf(msg.peer_name, sizeof(msg.peer_name), "%s", requesterName);
    msg.peer_name[MAX_NAME_LEN - 1] = '\0';

    if (!registered || strcmp(requesterName, peerName) != 0) {
        printf("query-dht requires this peer's registered name\n");
        return;
    }
    // send request and wait for manager
    if (send_to_manager(&msg) < 0) return;

    if (msg.return_code != RC_SUCCESS)
    {
        printf("query-dht FAILED\n");
        return;
    }

    if (msg.tuple_count < 1)
    {
        printf("query-dht FAILED: manager returned no DHT peer\n");
        return;
    }

    haveQueryStart = 1;
    queryStart = msg.tuples[0];
    printf("query-dht SUCCESS; now run: find-event <EVENT_ID>\n");
    printf("Starting peer: %s\n", msg.tuples[0].peer_name);
    printf("IP address:   %s\n", msg.tuples[0].ip);
    printf("P2P port:     %d\n", msg.tuples[0].p_port);
}

static void handle_find_event(PeerMessage *pm)
{
    // record peer as visited.
    int alreadyVisited = 0;

    for (int i = 0; i < pm->visitedCount; i++)
    {
        if (pm->visited[i] == peerID)
        {
            alreadyVisited = 1;
            break;
        }
    }

    if (alreadyVisited)
    {
        printf("Peer %s: event %d request already visited this peer; ignoring\n",
               peerName, pm->eventID);
        return;
    }

    if (pm->visitedCount >= MAX_PEERS)
    {
        printf("Peer %s: visited list is full; stopping search for event %d\n",
               peerName, pm->eventID);
        return;
    }

    pm->visited[pm->visitedCount++] = peerID;

    printf("Peer %s: searching for event %d (visited %d/%d)\n",
           peerName, pm->eventID, pm->visitedCount, dhtSize);

    // search this peer's local hash table.
    char result[MAX_RECORD_LEN];

    if (find_event_local(pm->eventID, result, sizeof(result)))
    {
        printf("Peer %s: FOUND event %d; returning result to %s:%d\n",
               peerName, pm->eventID,
               pm->requesterIP, pm->requesterPort);

        pm->found = 1;
        pm->record.eventID = pm->eventID;

        snprintf(pm->record.csvLine,
                 sizeof(pm->record.csvLine),
                 "%s", result);

        send_find_result(pm);
        return;
    }

    printf("Peer %s: event %d not in local table\n",
           peerName, pm->eventID);

    // select a random unvisited peer.
    int candidates[MAX_PEERS];
    int candidateCount = 0;

    for (int i = 0; i < numDhtPeers; i++)
    {
        int visited = 0;

        for (int j = 0; j < pm->visitedCount; j++)
        {
            if (pm->visited[j] == i)
            {
                visited = 1;
                break;
            }
        }

        if (!visited)
            candidates[candidateCount++] = i;
    }

    // every peer has been searched.
    if (candidateCount == 0)
    {
        printf("Peer %s: all peers searched; event %d not found\n",
               peerName, pm->eventID);

        pm->found = 0;
        send_find_result(pm);
        return;
    }

    int nextID = candidates[rand() % candidateCount];

    printf("Peer %s: forwarding event %d to peer %s (ID %d)\n",
           peerName, pm->eventID,
           dhtPeers[nextID].peer_name, nextID);

    // forward search to the selected peer.
    send_to_peer(&dhtPeers[nextID], pm);
}

static void handle_find_result(PeerMessage *pm)
{
    printf("Peer %s: received search result (found=%d)\n",
           peerName, pm->found);

    if (pm->found)
    {
        printf("\nEvent %d found (id-seq: ", pm->eventID);
        for (int i = 0; i < pm->visitedCount; ++i)
            printf("%s%d", i ? ", " : "", pm->visited[i]);
        printf("):\n");
        print_labeled_event(pm->record.csvLine);
    }
    else
    {
        printf("\nStorm event %d not found in the DHT.\n",
               pm->eventID);
    }

    printf("peer> ");
    fflush(stdout);
}

// local lookup for debugging

static int get_event_id_from_csv(const char *line)
{
    if (line == NULL) return -1;
    return atoi(line);
}

static void cmd_find_local_event(int eventID)
{
    if (!dhtBuilt)
    {
        printf("DHT has not been marked complete yet.\n");
        return;
    }

    for (int i = 0; i < myTableSize; i++)
    {
        if (myTable[i] == NULL)
            continue;

        int storedID = get_event_id_from_csv(myTable[i]);

        if (storedID == eventID)
        {
            printf("Event %d found on peer '%s' (ID %d)\n",
                   eventID, peerName, peerID);
            printf("Hash table position: %d\n", i);
            printf("CSV record:\n%s\n", myTable[i]);
            return;
        }
    }

    printf("Event %d not found on peer '%s' (ID %d).\n",
           eventID, peerName, peerID);
}



// release locally stored records before a rebuild or teardown
static void clear_table(void)
{
    for (int i = 0; i < myTableSize && i < MAX_TABLE; ++i) {
        free(myTable[i]);
        myTable[i] = NULL;
        myTableEventIDs[i] = 0;
    }
    myCount = 0;
    myTableSize = 0;
    dhtBuilt = 0;
}

static void reset_ring(void)
{
    clear_table();
    peerID = -1;
    dhtSize = 0;
    numDhtPeers = 0;
    haveRightNeighbor = 0;
    memset(&rightNeighbor, 0, sizeof(rightNeighbor));
    haveQueryStart = 0;
}

// retry udp stores if the acknowledgment does not arrive
static int wait_for_store_ack(int eventID, const Tuple *destination, PeerMessage *message)
{
    for (int attempt = 1; attempt <= 4; ++attempt) {
        send_to_peer(destination, message);
        for (;;) {
            PeerMessage received;
            struct sockaddr_in from;
            socklen_t fromlen = sizeof(from);
            ssize_t len = recvfrom(peerSocket, &received, sizeof(received), 0,
                                   (struct sockaddr *)&from, &fromlen);
            if (len < 0) break;
            if (received.type == P2P_STORE_ACK && received.eventID == eventID)
                return received.status == 1;
            if (received.type == P2P_STORE) handle_store(&received);
            else if (received.type == P2P_FIND_EVENT) handle_find_event(&received);
        }
        fprintf(stderr, "Peer: retrying store %d (attempt %d)\n", eventID, attempt + 1);
    }
    return 0;
}

static void notify_manager(int type)
{
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = type;
    snprintf(m.peer_name, sizeof(m.peer_name), "%s", peerName);
    if (type == MSG_DHT_REBUILT)
        snprintf(m.new_leader, sizeof(m.new_leader), "%s", expectedLeader);
    if (send_to_manager(&m) < 0) {
        fprintf(stderr, "Peer: manager did not acknowledge type=%d\n", type);
        return;
    }
    printf("Peer: %s %s\n",
           type == MSG_DHT_REBUILT ? "dht-rebuilt" : "teardown-complete",
           m.return_code == RC_SUCCESS ? "SUCCESS" : "FAILURE");
    if (m.return_code == RC_SUCCESS && type == MSG_DHT_REBUILT) {
        if (pendingChurn == MSG_LEAVE_DHT) reset_ring();
        pendingChurn = 0;
    }
    if (m.return_code == RC_SUCCESS && type == MSG_TEARDOWN_COMPLETE)
        reset_ring();
}

// ask the manager first, then send the new ring plan to its leader
static void cmd_leave_or_join(const char *line, int leaving)
{
    char command[32], name[MAX_NAME_LEN], extra;
    if (sscanf(line, "%31s %15s %c", command, name, &extra) != 2 ||
        strcmp(name, peerName) != 0 || !registered) {
        printf("Usage: %s <your-registered-name>\n", leaving ? "leave-dht" : "join-dht");
        return;
    }
    if (pendingChurn) { printf("Peer: rebuild already in progress\n"); return; }
    Message managerRequest;
    memset(&managerRequest, 0, sizeof(managerRequest));
    managerRequest.type = leaving ? MSG_LEAVE_DHT : MSG_JOIN_DHT;
    snprintf(managerRequest.peer_name, sizeof(managerRequest.peer_name), "%s", peerName);
    if (send_to_manager(&managerRequest) < 0) return;
    if (managerRequest.return_code != RC_SUCCESS) {
        printf("Peer: %s FAILURE\n", command);
        return;
    }
    int oldCount = managerRequest.tuple_count;
    if (oldCount < (leaving ? 3 : 2) || oldCount > MAX_PEERS) { fprintf(stderr, "Invalid ring from manager\n"); return; }
    PeerMessage plan;
    memset(&plan, 0, sizeof(plan));
    plan.type = P2P_REBUILD_REQUEST;
    plan.year = managerRequest.year;
    snprintf(plan.requesterTuple.peer_name, sizeof(plan.requesterTuple.peer_name), "%s", peerName);
    snprintf(plan.requesterTuple.ip, sizeof(plan.requesterTuple.ip), "%s", peerIP);
    plan.requesterTuple.p_port = pPort;
    if (leaving) {
        int at = -1;
        for (int i = 0; i < oldCount; ++i)
            if (strcmp(managerRequest.tuples[i].peer_name, peerName) == 0) at = i;
        if (at < 0) { fprintf(stderr, "Peer not found in manager ring\n"); return; }
        for (int i = 1; i < oldCount; ++i)
            plan.tuples[plan.tupleCount++] = managerRequest.tuples[(at+i)%oldCount];
    } else {
        for (int i = 0; i < oldCount; ++i)
            plan.tuples[plan.tupleCount++] = managerRequest.tuples[i];
        plan.tuples[plan.tupleCount++] = plan.requesterTuple;
    }
    snprintf(plan.newLeader, sizeof(plan.newLeader), "%s", plan.tuples[0].peer_name);
    snprintf(expectedLeader, sizeof(expectedLeader), "%s", plan.newLeader);
    pendingChurn = managerRequest.type;
    printf("Peer: %s SUCCESS, rebuilding n=%d -> n=%d; new leader=%s\n",
           command, oldCount, plan.tupleCount, expectedLeader);
    send_to_peer(&managerRequest.tuples[0], &plan);
}

static void cmd_teardown(const char *line)
{
    char command[32], name[MAX_NAME_LEN], extra;
    if (sscanf(line, "%31s %15s %c", command, name, &extra) != 2 ||
        strcmp(name, peerName) != 0 || !registered) {
        printf("Usage: teardown-dht <your-registered-name>\n");
        return;
    }
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_TEARDOWN_DHT;
    snprintf(m.peer_name, sizeof(m.peer_name), "%s", peerName);
    if (send_to_manager(&m) < 0) return;
    if (m.return_code != RC_SUCCESS) {
        printf("Peer: teardown-dht FAILURE (leader only)\n");
        return;
    }
    printf("Peer: teardown-dht SUCCESS; starting clear traversal\n");
    PeerMessage clear;
    memset(&clear, 0, sizeof(clear));
    clear.type = P2P_CLEAR;
    clear.mode = CLEAR_TEARDOWN;
    clear.hops = 1;
    send_to_peer(&rightNeighbor, &clear);
}

static int cmd_deregister(const char *line)
{
    char cmd[32], name[MAX_NAME_LEN], extra;
    if (sscanf(line, "%31s %15s %c", cmd, name, &extra) != 2 ||
        strcmp(name, peerName) != 0 || !registered) {
        printf("Usage: deregister <your-registered-name>\n");
        return 0;
    }
    Message m;
    memset(&m, 0, sizeof(m));
    m.type = MSG_DEREGISTER;
    snprintf(m.peer_name, sizeof(m.peer_name), "%s", peerName);
    if (send_to_manager(&m) < 0) return 0;
    if (m.return_code != RC_SUCCESS) {
        printf("Peer: deregister FAILURE (still in DHT?)\n");
        return 0;
    }
    registered = 0;
    printf("Peer: deregister SUCCESS; terminating\n");
    return 1;
}

// the current leader starts clearing the old ring
static void handle_rebuild_request(PeerMessage *pm)
{
    if (peerID != 0 || dhtSize < 2 || pm->tupleCount < 2) {
        fprintf(stderr, "Peer: only current leader can coordinate ring rebuild\n");
        return;
    }
    rebuildPlan = *pm;
    printf("Peer: leader received rebuild request (new ring=%d, leader=%s)\n",
           pm->tupleCount, pm->newLeader);
    PeerMessage clear;
    memset(&clear, 0, sizeof(clear));
    clear.type = P2P_CLEAR;
    clear.mode = CLEAR_REBUILD;
    clear.hops = 1;
    send_to_peer(&rightNeighbor, &clear);
}

static void handle_clear(PeerMessage *pm)
{
    if (peerID == 0 && pm->hops >= dhtSize) {
        clear_table();
        if (pm->mode == CLEAR_TEARDOWN) {
            printf("Peer: teardown returned to leader; notifying manager\n");
            notify_manager(MSG_TEARDOWN_COMPLETE);
        } else if (pm->mode == CLEAR_REBUILD) {
            if (count_csv_lines(rebuildPlan.year) < 0) {
                fprintf(stderr, "Peer: cannot rebuild: dataset is missing\n");
                return;
            }
            PeerMessage config = rebuildPlan;
            config.type = P2P_RECONFIG;
            config.hops = 0;
            config.tableSize = myTableSize;
            config.expectedCount = expectedRecords;
            send_to_peer(&config.tuples[0], &config);
        }
        return;
    }
    Tuple oldNext = rightNeighbor;
    clear_table();
    pm->hops++;
    printf("Peer %s: cleared table; forwarding clear (hop %d)\n", peerName, pm->hops);
    send_to_peer(&oldNext, pm);
    if (pm->mode == CLEAR_TEARDOWN) reset_ring();
}

// assign new ids and neighbor addresses around the rebuilt ring
static void handle_reconfig(PeerMessage *pm)
{
    int newCount = pm->tupleCount;
    if (newCount < 2 || newCount > MAX_PEERS) return;
    if (pm->hops >= newCount) {
        if (peerID != 0) return;
        printf("Peer %s: new ring configured; repopulating year %d\n", peerName, pm->year);
        rebuildIsRunning = 1;
        rebuiltRequester = pm->requesterTuple;
        populate_dht(pm->year, newCount);
        PeerMessage done;
        memset(&done, 0, sizeof(done));
        done.type = P2P_DONE;
        done.doneCount = 1;
        done.counts[0] = myCount;
        send_to_peer(&rightNeighbor, &done);
        return;
    }
    clear_table();
    peerID = pm->hops;
    dhtSize = newCount;
    myTableSize = pm->tableSize;
    expectedRecords = pm->expectedCount;
    numDhtPeers = newCount;
    for (int i = 0; i < newCount; ++i) dhtPeers[i] = pm->tuples[i];
    rightNeighbor = dhtPeers[(peerID+1)%dhtSize];
    haveRightNeighbor = 1;
    dhtBuilt = 0;
    printf("Peer %s: reset-id to %d/%d; right=%s\n",
           peerName, peerID, dhtSize, rightNeighbor.peer_name);
    pm->hops++;
    send_to_peer(&rightNeighbor, pm);
}

// tell the manager only after the new ring finishes loading records
static void handle_rebuild_ready(PeerMessage *pm)
{
    if (!pendingChurn) return;
    if (!pm->status || strcmp(pm->newLeader, expectedLeader) != 0) {
        fprintf(stderr, "Peer: invalid rebuild completion\n");
        return;
    }
    printf("Peer %s: all records redistributed; notifying manager\n", peerName);
    notify_manager(MSG_DHT_REBUILT);
}

static void print_labeled_event(const char *csv)
{
    static const int indices[] = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13};
    static const char *labels[] = {
        "event id", "state", "year", "month name", "event type", "cz type", "cz name",
        "injuries direct", "injuries indirect", "deaths direct", "deaths indirect",
        "damage property", "damage crops", "tor f scale"
    };
    int field = 0, inQuotes = 0;
    char value[MAX_RECORD_LEN];
    size_t at = 0;
    for (const char *p = csv;; ++p) {
        char ch = *p;
        if (ch == '"') {
            if (inQuotes && p[1] == '"') { if (at < sizeof(value)-1) value[at++] = '"'; ++p; }
            else inQuotes = !inQuotes;
        } else if ((ch == ',' && !inQuotes) || ch == '\0') {
            value[at] = '\0';
            for (size_t k = 0; k < sizeof(indices)/sizeof(indices[0]); ++k)
                if (field == indices[k]) printf("  %-20s: %s\n", labels[k], value);
            at = 0;
            field++;
            if (ch == '\0') break;
        } else if (at < sizeof(value)-1) value[at++] = ch;
    }
}
