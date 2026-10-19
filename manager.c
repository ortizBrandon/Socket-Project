#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <arpa/inet.h>
#include "protocol.h"
#include <time.h>

int dht_exists = 0;                 // 0 does not exist, 1 does exist
int dht_built = 0;                  // 1 after it is built
char dht_Leader[MAX_NAME_LEN] = ""; // leader name is empty until setup

// struct for the peer state storage
typedef struct
{
    char name[MAX_NAME_LEN];
    char ip[INET_ADDRSTRLEN];
    int m_port;
    int p_port;
    int state;
} Peer;

Peer peers[MAX_PEERS]; // registered peer list
int numPeers = 0;      // no peers registered at startup

// save the current dht ring
static Tuple dhtRing[MAX_PEERS];
static int ringSize = 0;
static int ringYear = 0;

// manager operations waiting for completion
enum {
    PENDING_NONE,
    PENDING_BUILD,
    PENDING_LEAVE,
    PENDING_JOIN,
    PENDING_TEARDOWN
};

static int pendingOperation = PENDING_NONE;
static char pendingPeerName[MAX_NAME_LEN] = "";


// prototypes
static void registerPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void setupDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void dhtComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void queryDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);

static void leaveDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void joinDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void dhtRebuilt(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void deregisterPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void teardownDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void teardownComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);


// manager takes one command line argument: its udp port
int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "Only pass ./%s <port>\n", argv[0]); // wrong number of arguments
        return 1;
    }

    int managerPort = atoi(argv[1]);                    // read the manager port
    int managerSocket = socket(AF_INET, SOCK_DGRAM, 0); // udp socket for manager requests

    if (managerSocket < 0)
    {
        perror("socket");
        return 1;
    }

    // setting up the sockets information
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;          // use ipv4
    serverAddr.sin_port = htons(managerPort); // use the given managerport
    serverAddr.sin_addr.s_addr = INADDR_ANY;  // listen on all local interfaces

    // bind the socket to the actual address/port
    int bindResult = bind(managerSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr));
    if (bindResult < 0)
    {
        perror("bind");
        return 1;
    }

    printf("Manager: Listening on UDP port %i\n", managerPort);

    // incoming request
    Message msg; // request buffer

    // source address for udp replies
    struct sockaddr_in peerAddr;
    socklen_t peerlen;

    srand((unsigned int)time(NULL)); // random num generator

    while (1) {
        peerlen = sizeof(peerAddr);
        int bytesReceived = recvfrom(managerSocket, &msg, sizeof(msg), 0, (struct sockaddr *)&peerAddr, &peerlen);
        if (bytesReceived < 0)
        {
            perror("recvfrom");
            continue;
        }

        // track who sent the message
        char ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peerAddr.sin_addr, ip, sizeof(ip));
        printf("Manager: Received %i bytes from %s:%i type= %i\n", bytesReceived, ip, ntohs(peerAddr.sin_port), msg.type);

        // wait for the matching completion before taking another command
        int allowedWhilePending = 0;
        if (pendingOperation == PENDING_BUILD && msg.type == MSG_DHT_COMPLETE){
            allowedWhilePending = 1;
        }
        else if ((pendingOperation == PENDING_LEAVE || pendingOperation == PENDING_JOIN) && msg.type == MSG_DHT_REBUILT) {
            allowedWhilePending = 1;
        }
        else if (pendingOperation==PENDING_TEARDOWN && msg.type == MSG_TEARDOWN_COMPLETE) {
            allowedWhilePending = 1;
        }

        if (pendingOperation != PENDING_NONE && !allowedWhilePending) {
            Message reply;
            memset(&reply, 0, sizeof(reply));

            reply.type = msg.type;
            reply.return_code = RC_FAILURE;

            printf("Manager: command %d FAILED, DHT operation in progres\n", msg.type);

            sendto(managerSocket, &reply, sizeof(reply), 0, (struct sockaddr *)&peerAddr, peerlen);

            continue;
        }


        // handle the request type
        switch (msg.type)
        {
        case MSG_REGISTER:
            registerPeer(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_SETUP_DHT:
            setupDht(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_DHT_COMPLETE:
            dhtComplete(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_QUERY_DHT:
            queryDht(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_LEAVE_DHT:
            leaveDht(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_JOIN_DHT:
            joinDht(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_DHT_REBUILT:
            dhtRebuilt(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_DEREGISTER:
            deregisterPeer(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_TEARDOWN_DHT:
            teardownDht(&msg, &peerAddr, peerlen, managerSocket);
            break;
        case MSG_TEARDOWN_COMPLETE:
            teardownComplete(&msg, &peerAddr, peerlen, managerSocket);
            break;
        default:
            printf("Manager:   unknown type %i\n", msg.type);
            break;
        }
    }
    close(managerSocket);
    return 0;
}

static void registerPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_REGISTER;

    // peer names must be 1 to 15 letters
    int nameLen = strlen(msg->peer_name);
    if (nameLen == 0 || nameLen > 15)
    {
        printf("Manager: register FAILED, invalid name length (%i)\n", nameLen);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // now check that it is only letters
    for (int i = 0; i < nameLen; i++)
    {
        if (!isalpha((unsigned char)msg->peer_name[i]))
        {
            printf("Manager: register FAILED, name %s is not alphabetic", msg->peer_name);
            reply.return_code = RC_FAILURE;
            sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
            return;
        }
    }

    if (numPeers >= MAX_PEERS)
    {
        printf("Manager:   register FAILED: peer table full\n");
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // check for duplicate name or duplicate ports
    for (int i = 0; i < numPeers; i++)
    {
        if (strcmp(peers[i].name, msg->peer_name) == 0)
        {
            printf("Manager:   register FAILED: duplicate name '%s'\n",
                   msg->peer_name);
            reply.return_code = RC_FAILURE;
            sendto(socketName, &reply, sizeof(reply), 0,
                   (struct sockaddr *)peerAddress, peerLength);
            return;
        }
        if (peers[i].m_port == msg->m_port ||
            peers[i].p_port == msg->p_port ||
            peers[i].m_port == msg->p_port ||
            peers[i].p_port == msg->m_port)
        {
            printf("Manager:   register FAILED: port collision "
                   "(new m=%i p=%i, existing m=%i p=%i)\n",
                   msg->m_port, msg->p_port,
                   peers[i].m_port, peers[i].p_port);
            reply.return_code = RC_FAILURE;
            sendto(socketName, &reply, sizeof(reply), 0,
                   (struct sockaddr *)peerAddress, peerLength);
            return;
        }
    }

    // all checks passed: store the peer
    Peer *p = &peers[numPeers];
    strncpy(p->name, msg->peer_name, MAX_NAME_LEN - 1);
    p->name[MAX_NAME_LEN - 1] = '\0';
    strncpy(p->ip, msg->ip, INET_ADDRSTRLEN - 1);
    p->ip[INET_ADDRSTRLEN - 1] = '\0';
    p->m_port = msg->m_port;
    p->p_port = msg->p_port;
    p->state = STATE_FREE;
    numPeers++;

    printf("Manager:   register SUCCESS: '%s' stored at peer #%i\n",
           p->name, numPeers - 1);

    reply.return_code = RC_SUCCESS;
    sendto(socketName, &reply, sizeof(reply), 0,
           (struct sockaddr *)peerAddress, peerLength);
}

// setup-dht
static void setupDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_SETUP_DHT;

    // check that sender is registered (linear search)
    int leaderIndex = -1;
    for (int i = 0; i < numPeers; i++)
    {
        if (strcmp(peers[i].name, msg->peer_name) == 0)
        {
            leaderIndex = i; // peer found at index i
            break;
        }
    }
    if (leaderIndex < 0)
    {
        printf("Manager: setup-dht FAILED, sender NOT REGISTERED\n");
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // ring size must be between 3 and max_peers
    if (msg->n < 3 || msg->n > MAX_PEERS)
    {
        printf("Manager: setup-dht FAILED, ring size %i\n", msg->n);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // only one dht can exist at a time
    if (dht_exists)
    {
        printf("Manager: setup-dht FAILED, a DHT already exists\n");
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // enough peers must be registered for this ring
    if (numPeers < msg->n)
    {
        printf("Manager: setup-dht FAILED, only %d peers registered, need %d\n", numPeers, msg->n);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // build ring, leader first
    peers[leaderIndex].state = STATE_LEADER;

    strncpy(reply.tuples[0].peer_name, peers[leaderIndex].name, MAX_NAME_LEN - 1);
    reply.tuples[0].peer_name[MAX_NAME_LEN - 1] = '\0';
    strncpy(reply.tuples[0].ip, peers[leaderIndex].ip, INET_ADDRSTRLEN - 1);
    reply.tuples[0].ip[INET_ADDRSTRLEN - 1] = '\0';
    reply.tuples[0].p_port = peers[leaderIndex].p_port;

    // random selection for free peers
    int freeIndicies[MAX_PEERS];
    int freeCount=0;

    for (int i = 0; i < numPeers; i++) {
        if (i!=leaderIndex && peers[i].state == STATE_FREE){
            freeIndicies[freeCount++] = i;
        }
    }

    if (freeCount < msg->n -1) {
        peers[leaderIndex].state = STATE_FREE;
        reply.return_code = RC_FAILURE;

        printf("Manager: setup-dht FAILED, not enough free peers\n");

        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    for (int i =freeCount-1; i >0; i--) {
        int j = rand() % (i+1);

        int temp = freeIndicies[i];
        freeIndicies[i] = freeIndicies[j];
        freeIndicies[j] = temp;
    }

    int count = 1;

    for (int k=0; k<msg->n-1; k++) {
        int i = freeIndicies[k];

        peers[i].state = STATE_INDHT;

        strncpy(reply.tuples[count].peer_name, peers[i].name, MAX_NAME_LEN - 1);
        reply.tuples[count].peer_name[MAX_NAME_LEN - 1] = '\0';

        strncpy(reply.tuples[count].ip, peers[i].ip, INET_ADDRSTRLEN - 1);
        reply.tuples[count].ip[INET_ADDRSTRLEN - 1] = '\0';

        reply.tuples[count].p_port = peers[i].p_port;

        count++;
    }

    // keep a copy of the ring for future membership changes
    ringSize = count;
    ringYear = msg->year;

    for (int i = 0; i < ringSize; i++) {
        dhtRing[i] = reply.tuples[i];
    }

    // record dht state
    dht_exists = 1;
    dht_built = 0; // not built until dht-complete
    pendingOperation = PENDING_BUILD;
    strncpy(dht_Leader, peers[leaderIndex].name, MAX_NAME_LEN - 1);
    dht_Leader[MAX_NAME_LEN - 1] = '\0';

    // log and reply
    printf("Manager: setup-dht SUCCESS, n=%i year=%i leader='%s'\n",
           msg->n, msg->year, dht_Leader);
    for (int k = 0; k < count; k++)
    {
        printf("Manager:   tuple[%i] = %s %s:%i\n", k, reply.tuples[k].peer_name, reply.tuples[k].ip, reply.tuples[k].p_port);
    }

    reply.tuple_count = count;
    reply.return_code = RC_SUCCESS;
    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

static void dhtComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_DHT_COMPLETE;

    // only the leader can confirm setup
    if (!dht_exists || pendingOperation != PENDING_BUILD || strcmp(dht_Leader, msg->peer_name) != 0)
    {
        printf("Manager: dht-complete FAILED, '%s' is not the leader\n", msg->peer_name);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    dht_built = 1;

    pendingOperation = PENDING_NONE;

    printf("Manager: dht-complete SUCCESS from leader '%s'\n", msg->peer_name);

    reply.return_code = RC_SUCCESS;
    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

static void queryDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_QUERY_DHT;

    // queries are allowed only after setup completes
    if (!dht_exists || !dht_built)
    {
        printf("Manager: query-dht FAILED, DHT is not built\n");
        reply.return_code = RC_FAILURE;

        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // find the requested peer
    int requesterIndex = -1;

    for (int i = 0; i < numPeers; i++)
    {
        if (strcmp(peers[i].name, msg->peer_name) == 0)
        {
            requesterIndex = i;
            break;
        }
    }

    if (requesterIndex < 0)
    {
        printf("Manager: query-dht FAILED, peer '%s' not registered\n",
               msg->peer_name);

        reply.return_code = RC_FAILURE;

        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // only a free peer can initiate a query
    if (peers[requesterIndex].state != STATE_FREE)
    {
        printf("Manager: query-dht FAILED, peer '%s' is not Free\n",
               msg->peer_name);

        reply.return_code = RC_FAILURE;

        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // collect peers maintaining dht
    int members[MAX_PEERS];
    int memberCount = 0;

    for (int i = 0; i < numPeers; i++)
    {
        if (peers[i].state == STATE_LEADER ||
            peers[i].state == STATE_INDHT)
        {
            members[memberCount++] = i;
        }
    }

    if (memberCount == 0)
    {
        printf("Manager: query-dht FAILED, no DHT members found\n");

        reply.return_code = RC_FAILURE;

        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // random selection of member
    int selectedIndex = members[rand() % memberCount];
    Peer *selected = &peers[selectedIndex];

    strncpy(reply.tuples[0].peer_name,
            selected->name, MAX_NAME_LEN - 1);
    reply.tuples[0].peer_name[MAX_NAME_LEN - 1] = '\0';

    strncpy(reply.tuples[0].ip,
            selected->ip, INET_ADDRSTRLEN - 1);
    reply.tuples[0].ip[INET_ADDRSTRLEN - 1] = '\0';

    reply.tuples[0].p_port = selected->p_port;
    reply.tuple_count = 1;
    reply.return_code = RC_SUCCESS;

    printf("Manager: query-dht SUCCESS for '%s'\n",
           msg->peer_name);
    printf("Manager:   selected starting peer '%s' at %s:%d\n",
           selected->name, selected->ip, selected->p_port);

    // send the starting peer back to the requester
    sendto(socketName, &reply, sizeof(reply), 0,
           (struct sockaddr *)peerAddress, peerLength);
}

// approve the request now; the peer handles the actual rebuild
static void leaveDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_LEAVE_DHT;
    reply.return_code = RC_FAILURE;

    if (!dht_exists || !dht_built)
    {
        printf("Manager: leave-dht FAILED, DHT is not built or does not exist\n");
    }else if (pendingOperation != PENDING_NONE) {
        printf("Manager: leave-dht FAILED, another operation is already in progress\n");

    }
    else {
        int peerIndex = -1;

        for (int i = 0; i < numPeers; i++) {
            if (strcmp(peers[i].name, msg->peer_name) == 0) {
                peerIndex = i;
                break;
            }
        }

        if (peerIndex < 0 || (peers[peerIndex].state != STATE_INDHT && peers[peerIndex].state != STATE_LEADER)) {
            printf("Manager: leave-dht FAILED, peer is not in the DHT\n");
        }
        else {
            pendingOperation = PENDING_LEAVE;
            strncpy(pendingPeerName, msg->peer_name, MAX_NAME_LEN - 1);
            pendingPeerName[MAX_NAME_LEN - 1] = '\0';

            reply.tuple_count = ringSize;
            reply.year = ringYear;

            for (int i = 0; i < ringSize; i++) {
                reply.tuples[i] = dhtRing[i];
            }

            reply.return_code = RC_SUCCESS;

            printf("Manager: leave-dht SUCCESS for '%s'\n", msg->peer_name);
            printf("Manager: waiting for dht-rebuilt\n");
        }
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

// give a free peer the ring information it needs to join
static void joinDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {

    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_JOIN_DHT;
    reply.return_code = RC_FAILURE;

    if (!dht_exists || !dht_built) {
        printf("Manager: join-dht FAILED, DHT is not built or does not exist\n");
    }
    else if (pendingOperation != PENDING_NONE) {
        printf("Manager: join-dht FAILED, another operation is already in progress\n");
    }
    else if(ringSize >= MAX_PEERS){
        printf("Manager: join-dht FAILED, too many peers or ring is full\n");
    }
    else {
        int peerIndex = -1;

        for (int i = 0; i < numPeers; i++) {
            if (strcmp(peers[i].name, msg->peer_name) == 0) {
                peerIndex = i;
                break;
            }
        }

        if (peerIndex < 0 || peers[peerIndex].state != STATE_FREE) {
            printf("Manager: join-dht FAILED, peer is not registered or not free\n");
        }
        else {
            pendingOperation = PENDING_JOIN;
            strncpy(pendingPeerName, msg->peer_name, MAX_NAME_LEN - 1);
            pendingPeerName[MAX_NAME_LEN - 1] = '\0';

            reply.tuple_count = ringSize;
            reply.year = ringYear;

            for (int i = 0; i < ringSize; i++) {
                reply.tuples[i] = dhtRing[i];
            }

            reply.return_code = RC_SUCCESS;
            printf("Manager: join-dht SUCCESS for '%s'\n", msg->peer_name);
            printf("Manager: waiting for dht-rebuilt\n");
        }
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

// update membership only after the peer reports a finished rebuild
static void dhtRebuilt(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {

    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_DHT_REBUILT;
    reply.return_code = RC_FAILURE;

    if ((pendingOperation != PENDING_LEAVE && pendingOperation != PENDING_JOIN) || strcmp(pendingPeerName, msg->peer_name) != 0) {
        printf("Manager: dht-rebuilt FAILED, invalid request\n");
    }
    else if (pendingOperation == PENDING_LEAVE) {
        int leavingIndex = -1;

        for (int i = 0; i < ringSize; i++) {
            if (strcmp(dhtRing[i].peer_name, msg->peer_name) == 0) {
                leavingIndex = i;
                break;
            }
        }

        if (leavingIndex < 0 || ringSize < 3) {
            printf("Manager: dht-rebuilt FAILED, invalid leaving peer or ring size\n");
        }
        else if (strcmp(msg->new_leader, dhtRing[(leavingIndex + 1) % ringSize].peer_name) !=0) {
            printf("Manager: dht-rebuilt FAILED, invalid new leader\n");
        }
        else {
            // start the smaller ring at the departing peer's right neighbor
            Tuple newRing[MAX_PEERS];
            int newSize = 0;

            for (int i = 1; i < ringSize; i++) {
                int index = (leavingIndex + i) % ringSize;
                newRing[newSize++] = dhtRing[index];
            }

            for (int i = 0; i < newSize; i++) {
                dhtRing[i] = newRing[i];
            }

            ringSize = newSize;

            for (int i = 0; i < numPeers; i++) {
                if (strcmp(peers[i].name, msg->peer_name) == 0) {
                    peers[i].state = STATE_FREE;
                    break;
                }
            }

            reply.return_code = RC_SUCCESS;
        }
    }
    else {
        int joiningIndex = -1;

        for (int i = 0; i < numPeers; i++) {
        if (strcmp(peers[i].name, msg->peer_name) == 0) {
            joiningIndex = i;
            break;
        }
    }
        if (joiningIndex < 0 || peers[joiningIndex].state != STATE_FREE || ringSize >= MAX_PEERS || strcmp(msg->new_leader, dht_Leader)!=0) {
            printf("Manager: dht-rebuilt FAILED, joining peer or leader failed\n");
        }
        else {
            // add the new member to the end of the ring
            Tuple *t = &dhtRing[ringSize];
            memset(t,0,sizeof(*t));

            strncpy(t->peer_name, peers[joiningIndex].name, MAX_NAME_LEN - 1);
            strncpy(t->ip, peers[joiningIndex].ip, INET_ADDRSTRLEN - 1);
            t->p_port = peers[joiningIndex].p_port;

            ringSize++;

            reply.return_code = RC_SUCCESS;
        }
    }
    // refresh the leader and member states after a successful rebuild
    if (reply.return_code == RC_SUCCESS) {
        strncpy(dht_Leader, msg->new_leader, MAX_NAME_LEN -1);
        dht_Leader[MAX_NAME_LEN -1 ] = '\0';

        for (int i = 0; i < ringSize; i++) {
            for (int j = 0; j < numPeers; j++) {
                if (strcmp(peers[j].name, dhtRing[i].peer_name) == 0) {
                    peers[j].state = (i==0) ? STATE_LEADER : STATE_INDHT;
                    break;
                }
            }
        }

        pendingOperation = PENDING_NONE;
        pendingPeerName[0] = '\0';
        dht_built = 1;

        printf("Manager: dht-rebuilt SUCCESS, n=%d leader='%s'\n", ringSize, dht_Leader);
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

static void deregisterPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {
    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_DEREGISTER;
    reply.return_code = RC_FAILURE;

    int peerIndex = -1;

    for (int i = 0; i < numPeers; i++) {
        if (strcmp(peers[i].name, msg->peer_name) == 0) {
            peerIndex = i;
            break;
        }
    }

    if (peerIndex < 0) {
        printf("Manager: deregister FAILED, peer is not registered\n");
    }
    else if (peers[peerIndex].state != STATE_FREE) {
        printf("Manager: deregister FAILED, peer is still maintaining the DHT\n");
    }
    else {

        for (int i = peerIndex; i < numPeers-1; i++) {
            peers[i] = peers[i+1];
        }

        numPeers--;

        reply.return_code = RC_SUCCESS;
        printf("Manager: deregister SUCCESS for '%s'\n", msg->peer_name);
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

static void teardownDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {
    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_TEARDOWN_DHT;
    reply.return_code = RC_FAILURE;

    if (!dht_exists || !dht_built) {
        printf("Manager: teardown-dht FAILED, no dht found\n");
    }
    else if (pendingOperation != PENDING_NONE) {
        printf("Manager: teardown-dht FAILED, pending operation not allowed\n");
    }
    else if (strcmp(msg->peer_name, dht_Leader) !=0) {
        printf("Manager: teardown-dht FAILED, peer is not the leader\n");
    }
    else {
        pendingOperation = PENDING_TEARDOWN;

        strncpy(pendingPeerName, msg->peer_name, MAX_NAME_LEN-1);
        pendingPeerName[MAX_NAME_LEN -1 ] = '\0';

        reply.tuple_count = ringSize;
        reply.year = ringYear;

        for (int i = 0; i < ringSize; i++) {
            reply.tuples[i] = dhtRing[i];
        }

        reply.return_code = RC_SUCCESS;

        printf("Manager: teardown-dht SUCCESS for '%s'\n", msg->peer_name);
        printf("Manager: waiting for teardown-complete\n");
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}

// return every ring member to free after teardown finishes
static void teardownComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName) {
    Message reply;
    memset(&reply, 0, sizeof(reply));

    reply.type = MSG_TEARDOWN_COMPLETE;
    reply.return_code = RC_FAILURE;

    if (pendingOperation!=PENDING_TEARDOWN || !dht_exists || strcmp(msg->peer_name, dht_Leader) !=0 || strcmp(msg->peer_name, pendingPeerName) !=0) {
        printf("Manager: teardown-complete FAILED, invalid request\n");
    }
    else {
        for (int i = 0; i < ringSize; i++) {
            for (int j = 0; j < numPeers; j++) {
                if (strcmp(peers[j].name, dhtRing[i].peer_name) == 0) {
                    peers[j].state = STATE_FREE;
                    break;
                }
            }
        }

        memset(dhtRing, 0, sizeof(dhtRing));
        ringSize = 0;
        ringYear=0;

        dht_exists = 0;
        dht_built = 0;
        dht_Leader[0] = '\0';

        pendingOperation = PENDING_NONE;
        pendingPeerName[0] = '\0';

        reply.return_code = RC_SUCCESS;

        printf("Manager: teardown-complete SUCCESS\n");
        printf("Manager: DHT deleted, all members are free now\n");
    }

    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}
