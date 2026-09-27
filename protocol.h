// protocol.h
#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <arpa/inet.h> // INET_ADDRSTRLEN, inet_pton, inet_ntop

// ---- Limits ----
#define MAX_PEERS 100
#define MAX_NAME_LEN 16 // spec: peer-name is at most 15 chars + null
#define MAX_BUFFER 4096
#define MAX_EVENTS 10000 // upper bound for the local hash table

// ---- Message type codes ----
#define MSG_REGISTER 1
#define MSG_SETUP_DHT 2
#define MSG_DHT_COMPLETE 3

// ---- Return codes ----
#define RC_SUCCESS 0
#define RC_FAILURE 1

// ---- Peer states ----
#define STATE_FREE 0
#define STATE_LEADER 1
#define STATE_INDHT 2

// ---- The wire format shared by both programs ----
// One struct, fixed size, easy to send/recv as raw bytes.
// Both programs must agree on this layout.
typedef struct
{
    int type; // MSG_* opcode

    // Fields used by REGISTER
    char peer_name[MAX_NAME_LEN]; // "alice"
    char ip[INET_ADDRSTRLEN];     // "127.0.0.1"
    int m_port;                   // peer <-> manager
    int p_port;                   // peer <-> peer

    // Fields used by SETUP-DHT
    int n;    // ring size
    int year; // YYYY

    // Return code used in replies from the manager
    int return_code; // RC_SUCCESS / RC_FAILURE

} Message;

#endif