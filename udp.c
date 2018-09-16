#include <netinet/udp.h>   //Provides declarations for udp header
#include <netinet/ip.h>    //Provides declarations for ip header

#include "udp.h"

int createSocket() {
    //Create a raw socket of type IPPROTO
    int s = socket (AF_INET, SOCK_RAW, IPPROTO_RAW);
    if(s == -1)
    {
        //socket creation failed, may be because of non-root privileges
        perror("Failed to create raw socket");
        exit(1);
    }
    return s;
}

/*
    Generic checksum calculation function
*/
unsigned short csum(unsigned short *ptr,int nbytes) {
    register long sum;
    unsigned short oddbyte;
    register short answer;

    sum=0;
    while(nbytes>1) {
        sum+=*ptr++;
        nbytes-=2;
    }
    if(nbytes==1) {
        oddbyte=0;
        *((u_char*)&oddbyte)=*(u_char*)ptr;
        sum+=oddbyte;
    }

    sum = (sum>>16)+(sum & 0xffff);
    sum = sum + (sum>>16);
    answer=(short)~sum;

    return(answer);
}

int udpWrite(int s, const char* saddr, const char* daddr, short sport, short dport,
             char* buf, bool chksum)
{ 
    // Datagram to represent the packet
    char datagram[4096] , source_ip[32] , *data , *pseudogram;
    // Zero out the packet buffer
    memset (datagram, 0, 4096);
    // IP header
    struct ip *iph = (struct ip *) datagram;
    // UDP header
    struct udphdr *udph = (struct udphdr *) (datagram + sizeof (struct ip));

    struct sockaddr_in sin;
    struct pseudo_header psh;

    // Copy data into datagram
    data = datagram + sizeof(struct ip) + sizeof(struct udphdr);
    strcpy(data , buf);


    strcpy(source_ip , saddr);
    sin.sin_family = AF_INET;
    sin.sin_port = htons(80);
    sin.sin_addr.s_addr = inet_addr(daddr);

    //Fill in the IP Header
    iph->ip_hl = 5;
    iph->ip_v = 4;
    iph->ip_tos = 0;
    iph->ip_len = sizeof (struct ip) + sizeof (struct udphdr) + strlen(data);
    iph->ip_id = htons(10); //Id of this packet
    iph->ip_off = 0;
    iph->ip_ttl = 255;
    iph->ip_p = IPPROTO_UDP;
    iph->ip_sum = 0; //Set to 0 before calculating checksum
    iph->ip_src.s_addr = inet_addr ( source_ip );  //Spoof the source ip address
    iph->ip_dst.s_addr = sin.sin_addr.s_addr;

    //Compute checksum if needed
    if (chksum) iph->ip_sum = csum ((unsigned short *) datagram, iph->ip_len);

    //UDP header
    udph->uh_sport = htons(sport);
    udph->uh_dport = htons(dport);
    udph->uh_ulen = htons(8 + strlen(data)); //tcp header size
    udph->uh_sum = 0; //leave checksum 0 now, filled later by pseudo header

    //Now the UDP checksum using the pseudo header
    if (chksum) {
        psh.source_address = inet_addr( source_ip );
        psh.dest_address = sin.sin_addr.s_addr;
        psh.placeholder = 0;
        psh.protocol = IPPROTO_UDP;
        psh.udp_length = htons(sizeof(struct udphdr) + strlen(data) );
        
        int psize = sizeof(struct pseudo_header) + sizeof(struct udphdr) + strlen(data);
        pseudogram = (char *) malloc(psize);
        
        memcpy(pseudogram , (char*) &psh , sizeof (struct pseudo_header));
        memcpy(pseudogram + sizeof(struct pseudo_header) , udph , sizeof(struct udphdr) + strlen(data));
        udph->uh_sum = csum( (unsigned short*) pseudogram , psize);
    }

    //Send the packet
    if (sendto (s, datagram, iph->ip_len ,  0, (struct sockaddr *) &sin, sizeof (sin)) < 0) {
        perror("sendto failed");
        return 1;
    }
    return 0;
}
