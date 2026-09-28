#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <arpa/inet.h>
#include "protocol.h"

int dht_exists = 0;                 // 0 does not exist, 1 does exist
int dht_built = 0;                  // 1 after it is built
char dht_Leader[MAX_NAME_LEN] = ""; // intialize empty leader string

// struct for the peer state storage
typedef struct
{
    char name[MAX_NAME_LEN];
    char ip[INET_ADDRSTRLEN];
    int m_port;
    int p_port;
    int state;
} Peer;

Peer peers[MAX_PEERS]; // we can hold up to a max num of peers (100)
int numPeers = 0;      // as of the beginning we have 0 peers

// prototypes
static void registerPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void setupDht(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);
static void dhtComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName);

// command line input, taking a string input as the second argument
int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        fprintf(stderr, "Only pass ./%s <port>\n", argv[0]); // if more or less than the wanted parameters are passed ERROR
        return 1;
    }

    int managerPort = atoi(argv[1]);                    // turn the string value 'port' into an interger value 'port' and store in managerPort
    int managerSocket = socket(AF_INET, SOCK_DGRAM, 0); // create a managerSocket using IPv4 and of type UDP (Datagram), 0 default protocol

    if (managerSocket < 0)
    {
        perror("socket");
        return 1;
    }

    // setting up the sockets information
    struct sockaddr_in serverAddr;
    memset(&serverAddr, 0, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;          // use ipv4
    serverAddr.sin_port = htons(managerPort); // use the given managerPort
    serverAddr.sin_addr.s_addr = INADDR_ANY;  // accept packets directed to any of the machines interfaces (addresses)

    // bind the socket to the actual address/port
    int bindResult = bind(managerSocket, (struct sockaddr *)&serverAddr, sizeof(serverAddr));
    if (bindResult < 0)
    {
        perror("bind");
        return 1;
    }

    printf("Manager: Listening on UDP port %i\n", managerPort);

    // lets now create the message to be recieved/sent
    Message msg; // where the message will be stored

    // create client structure since we are going to need that info to receive from, and its addr length
    struct sockaddr_in peerAddr;
    socklen_t peerlen;

    while (1)
    {
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

        // Dispatch
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
        default:
            printf("Manager:   unknown type %i\n", msg.type);
            break;
        }
        // to-do
    }
    close(managerSocket);
    return 0;
}

static void registerPeer(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_REGISTER;

    // check that peer name is an alphanetic string of at most 15 characters
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

    // check that n>=3
    if (msg->n < 3)
    {
        printf("Manager: setup-dht FAILED, ring size %i\n", msg->n);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // check if dht already exists
    if (dht_exists)
    {
        printf("Manager: setup-dht FAILED, a DHT already exists\n");
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0,
               (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    // check that at least n peers are registerd
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

    int count = 1;
    for (int i = 0; i < numPeers && count < msg->n; i++)
    {
        if (i == leaderIndex)
            continue; // skip leader
        if (peers[i].state != STATE_FREE)
            continue; // only Free peers

        peers[i].state = STATE_INDHT;

        strncpy(reply.tuples[count].peer_name, peers[i].name, MAX_NAME_LEN - 1);
        reply.tuples[count].peer_name[MAX_NAME_LEN - 1] = '\0';
        strncpy(reply.tuples[count].ip, peers[i].ip, INET_ADDRSTRLEN - 1);
        reply.tuples[count].ip[INET_ADDRSTRLEN - 1] = '\0';
        reply.tuples[count].p_port = peers[i].p_port;
        count++;
    }

    // record dht state
    dht_exists = 1;
    dht_built = 0; // not built until dht-complete
    strncpy(dht_Leader, peers[leaderIndex].name, MAX_NAME_LEN - 1);
    dht_Leader[MAX_NAME_LEN - 1] = '\0';

    // log and reply
    //  --- 7. Log and reply SUCCESS ---
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

static void dhtComplete(Message *msg, struct sockaddr_in *peerAddress, socklen_t peerLength, int socketName)
{
    Message reply;
    memset(&reply, 0, sizeof(reply));
    reply.type = MSG_DHT_COMPLETE;

    // Sender must be the current leader
    if (!dht_exists || strcmp(dht_Leader, msg->peer_name) != 0)
    {
        printf("Manager: dht-complete FAILED, '%s' is not the leader\n", msg->peer_name);
        reply.return_code = RC_FAILURE;
        sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
        return;
    }

    dht_built = 1;

    printf("Manager: dht-complete SUCCESS from leader '%s'\n", msg->peer_name);

    reply.return_code = RC_SUCCESS;
    sendto(socketName, &reply, sizeof(reply), 0, (struct sockaddr *)peerAddress, peerLength);
}