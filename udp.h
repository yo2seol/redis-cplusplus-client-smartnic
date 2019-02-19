/*
    Raw UDP sockets
*/
#include <stdio.h> //for printf
#include <string.h> //memset
#include <sys/socket.h>    //for socket ofcourse
#include <stdlib.h> //for exit(0);
#include <errno.h> //For errno - the error number
#include <stdbool.h>
#include <arpa/inet.h>
#include <netinet/udp.h>   //Provides declarations for udp header
#include <netinet/ip.h>    //Provides declarations for ip header

#ifndef UDP_H
#define UDP_H
/* 
    96 bit (12 bytes) pseudo header needed for udp header checksum calculation 
*/
struct pseudo_header
{
    u_int32_t source_address;
    u_int32_t dest_address;
    u_int8_t placeholder;
    u_int8_t protocol;
    u_int16_t udp_length;
};

extern struct ip iph_g;

unsigned short csum(unsigned short *ptr, int nbytes);

int createSocket();
int udpWrite(int s, const char* saddr, const char* daddr, short sport, short dport,
             char* buf, int len, struct sockaddr_in *sin, bool chksum);
#endif
