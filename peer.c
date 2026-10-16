#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/select.h>

#include "protocol.h"

// P2P message types
#define P2P_SET_ID 1
#define P2P_STORE 2
#define P2P_DONE 3

// local hash table
#define MAX_TABLE 200000
static char *myTable[MAX_TABLE]; // pointers to allocate what is needed
static int myTableSize = 0;
static int myCount = 0;

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

// P2P message
typedef struct
{
    int type;
    int peerID;
    int dhtSize;
    int tableSize;      // for SET_ID so peers know s
    int pos;            // for STORE
    EventRecord record; // complete record for STORE
    int tupleCount;
    Tuple tuples[MAX_PEERS]; // for SET_ID
    int counts[MAX_PEERS];   // for DONE
    int doneCount;
} PeerMessage;

// Prototypes
static int build_manager_addr(struct sockaddr_in *addr);
static int send_to_manager(Message *m); // send + wait for reply
static void cmd_register(const char *line);
static void cmd_setup_dht(const char *line);
static void handle_set_id(PeerMessage *m);
static void handle_store(PeerMessage *m);
static void handle_done(PeerMessage *m);
static void send_to_peer(const Tuple *t, PeerMessage *m);
static int is_prime(int x);
static int next_prime_above(int x);
static int count_csv_lines(int year);
static void populate_dht(int year, int n);
static void send_dht_complete(void);
static void print_config(void);
static int store_with_linear_probing(int startPos, const char *csvLine);
static void cmd_query_dht(const char *line);

static int get_event_id_from_csv(const char *line);
static void cmd_find_local_event(int eventID);

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        fprintf(stderr, "Usage: %s <manager-ip> <manager-port>\n", argv[0]);
        return 1;
    }

    strncpy(managerIP, argv[1], INET_ADDRSTRLEN - 1);
    managerPort = atoi(argv[2]);

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

    // Set a receive timeout on the peer socket, so blocking waits don't hang forever
    struct timeval tv = {.tv_sec = 5, .tv_usec = 0};
    setsockopt(peerSocket, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

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
            else if (strcmp(command, "config") == 0)
            {
                print_config();
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

    strncpy(peerName, name, MAX_NAME_LEN - 1);
    strncpy(peerIP, ip, INET_ADDRSTRLEN - 1);
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
    strncpy(msg.peer_name, peerName, MAX_NAME_LEN - 1);
    strncpy(msg.ip, peerIP, INET_ADDRSTRLEN - 1);
    msg.m_port = mPort;
    msg.p_port = pPort;

    printf("Peer: registering '%s'...\n", peerName);
    if (send_to_manager(&msg) < 0)
        return;

    if (msg.return_code == RC_SUCCESS)
        printf("Peer: registration SUCCESS\n");
    else
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

    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_SETUP_DHT;
    strncpy(msg.peer_name, peerName, MAX_NAME_LEN - 1);
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

    numDhtPeers = msg.tuple_count;
    for (int i = 0; i < numDhtPeers; i++)
        dhtPeers[i] = msg.tuples[i];

    peerID = 0; // leader is always 0
    dhtSize = numDhtPeers;
    rightNeighbor = dhtPeers[1 % dhtSize];
    haveRightNeighbor = 1;

    // compute table size from CSV BEFORE sending set-id
    if (count_csv_lines(year) < 0)
        return; // sets myTableSize

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

    // give peers a moment to process set ids
    usleep(500000); // 500 ms

    // read CSV and push stores around the ring
    populate_dht(year, n);

    // send DONE around the ring to collect counts
    PeerMessage done;
    memset(&done, 0, sizeof(done));
    done.type = P2P_DONE;
    done.doneCount = 1;
    done.counts[0] = myCount; // slot 0 is the leader's own count

    printf("Peer: sending done-request around ring\n");
    send_to_peer(&rightNeighbor, &done);

    // block until DONE returns
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
            printf("\nPeer: ===== records stored per peer =====\n");
            for (int i = 0; i < back.doneCount; i++)
            {
                printf("  peer id %d (%s): %d records\n",
                       i, dhtPeers[i].peer_name, back.counts[i]);
            }
            printf("Peer: =====================================\n");
            break;
        }
        if (back.type == P2P_STORE)
            handle_store(&back);
    }

    // signal completion to the manager
    send_dht_complete();
}

static void handle_set_id(PeerMessage *m)
{
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

static int store_with_linear_probing(int startPos, const char *csvLine)
{
    if (myTableSize <= 0)
        return -1;

    // Start at the original hash position.
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

            myCount++;
            return pos; // Return the actual slot used.
        }
    }

    fprintf(stderr, "Peer %s: hash table is full\n", peerName);
    return -1;
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
        // This peer is responsible for storing the record.
        // use linear probing if the original position is occupied
        int storedPos = store_with_linear_probing(
            m->pos,
            m->record.csvLine);

        if (storedPos < 0)
        {
            fprintf(stderr,
                    "Peer %s: failed to store event %d\n",
                    peerName,
                    m->record.eventID);
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

static int count_csv_lines(int year)
{
    char filename[64];
    snprintf(filename, sizeof(filename), "details-%d.csv", year);

    FILE *f = fopen(filename, "r");
    if (!f)
    {
        perror("fopen CSV");
        return -1;
    }

    char line[4096];
    int lines = 0;
    while (fgets(line, sizeof(line), f))
        lines++;
    fclose(f);

    int ell = lines - 1;
    if (ell < 1)
    {
        printf("CSV empty\n");
        return -1;
    }

    myTableSize = next_prime_above(2 * ell);
    if (myTableSize > MAX_TABLE)
        myTableSize = MAX_TABLE;

    printf("Peer: CSV has %d events, table size s = %d\n", ell, myTableSize);
    return ell;
}

static void populate_dht(int year, int n)
{
    char filename[64];
    snprintf(filename, sizeof(filename), "details-%d.csv", year);

    FILE *f = fopen(filename, "r");
    if (!f)
    {
        perror("fopen CSV");
        return;
    }

    char line[4096];

    // Skip CSV header.
    if (!fgets(line, sizeof(line), f))
    {
        fclose(f);
        return;
    }

    int ell = 0, sent = 0, local = 0, collisions = 0;

    while (fgets(line, sizeof(line), f))
    {
        // Remove trailing newline.
        line[strcspn(line, "\r\n")] = '\0';

        // Extract EVENT_ID, the eighth CSV field.
        char *field = line;

        for (int i = 0; i < 7; i++)
        {
            field = strchr(field, ',');
            if (field == NULL)
                break;

            field++;
        }

        if (field == NULL)
            continue;

        int event_id = atoi(field);

        // hash function
        int pos = event_id % myTableSize;
        int id = pos % n;

        if (id == peerID)
        {
            // store locally, resolve collision w linear probing.
            int storedPos = store_with_linear_probing(pos, line);

            if (storedPos >= 0)
            {
                local++;
            }
            else
            {
                collisions++;
            }
        }
        else
        {
            // Send to the right neighbor; records travel around the ring.
            PeerMessage pm;
            memset(&pm, 0, sizeof(pm));

            pm.type = P2P_STORE;
            pm.pos = pos; // Preserve the original hash position.
            pm.record.eventID = event_id;

            strncpy(pm.record.csvLine, line,
                    sizeof(pm.record.csvLine) - 1);

            send_to_peer(&rightNeighbor, &pm);
            sent++;
        }

        ell++;
    }

    fclose(f);

    printf("Peer: leader stored %d locally, sent %d around ring, "
           "failed local stores %d (total rows %d)\n",
           local, sent, collisions, ell);
}

static void send_dht_complete(void)
{
    Message msg;
    memset(&msg, 0, sizeof(msg));
    msg.type = MSG_DHT_COMPLETE;
    strncpy(msg.peer_name, peerName, MAX_NAME_LEN - 1);

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
    printf("  rightNeighbor=%s (%s:%d)\n",
           rightNeighbor.peer_name, rightNeighbor.ip, rightNeighbor.p_port);
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

    // parse
    if (sscanf(line, "%31s %15s", command, requesterName) != 2)
    {
        printf("Usage: query-dht <peer-name>\n");
        return;
    }

    // prep manager request
    Message msg;
    memset(&msg, 0, sizeof(msg));

    msg.type = MSG_QUERY_DHT;
    strncpy(msg.peer_name, requesterName, MAX_NAME_LEN - 1);
    msg.peer_name[MAX_NAME_LEN - 1] = '\0';

    // send request and wait for manager
    send_to_manager(&msg);

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

    printf("query-dht SUCCESS\n");
    printf("Starting peer: %s\n", msg.tuples[0].peer_name);
    printf("IP address:   %s\n", msg.tuples[0].ip);
    printf("P2P port:     %d\n", msg.tuples[0].p_port);
}

// function to find events (to test that my dht is working)

static int get_event_id_from_csv(const char *line)
{
    if (line == NULL)
        return -1;

    const char *field = line;

    for (int i = 0; i < 7; i++)
    {
        field = strchr(field, ',');

        if (field == NULL)
            return -1;

        field++;
    }

    return atoi(field);
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
