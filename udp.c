#include "udp.h"

struct ip iph_g;

int createSocket() {
    //Create a raw socket of type IPPROTO
    //int s = socket (AF_INET, SOCK_RAW, IPPROTO_RAW);
    int s = socket (AF_INET, SOCK_DGRAM, 0);
    if(s == -1)
    {
        //socket creation failed, may be because of non-root privileges
        perror("Failed to create raw socket");
        exit(1);
    }
    //Fill in the IP Header
    iph_g.ip_hl = 5;
    iph_g.ip_v = 4;
    iph_g.ip_tos = 0;
    iph_g.ip_id = htons(10); //Id of this packet
    iph_g.ip_off = 0;
    iph_g.ip_ttl = 255;
    iph_g.ip_p = IPPROTO_UDP;
    iph_g.ip_sum = 0; //Set to 0 before calculating checksum
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
             char* buf, int len, struct sockaddr_in *sin, bool chksum)
{ 
    // Datagram to represent the packet
    char datagram[4096] = {0}; 
    char *data , *pseudogram;
    // IP header
    struct ip *iph = (struct ip *) datagram;
    // UDP header
    struct udphdr *udph = (struct udphdr *) (datagram + sizeof (struct ip));

    struct pseudo_header psh;

    // Copy data into datagram
    data = datagram + sizeof(struct ip) + sizeof(struct udphdr);
    memcpy(iph, &iph_g, sizeof(struct ip));
    memcpy(data , buf, len);

    iph->ip_len = sizeof (struct ip) + sizeof (struct udphdr) + len;
    iph->ip_src.s_addr = inet_addr(saddr);  //Spoof the source ip address
    iph->ip_dst.s_addr = sin->sin_addr.s_addr;

    //Compute checksum if needed
    if (chksum) iph->ip_sum = csum ((unsigned short *) datagram, iph->ip_len);

    //UDP header
    udph->uh_sport = htons(sport);
    udph->uh_dport = htons(dport);
    udph->uh_ulen = htons(8 + len); //udp header size
    udph->uh_sum = 0; //leave checksum 0 now, filled later by pseudo header

    //Now the UDP checksum using the pseudo header
    if (chksum) {
        psh.source_address = inet_addr(saddr);
        psh.dest_address = inet_addr(daddr);
        psh.placeholder = 0;
        psh.protocol = IPPROTO_UDP;
        psh.udp_length = htons(sizeof(struct udphdr) + len);
        
        int psize = sizeof(struct pseudo_header) + sizeof(struct udphdr) + len;
        pseudogram = (char *) malloc(psize);
        
        memcpy(pseudogram , (char*) &psh , sizeof (struct pseudo_header));
        memcpy(pseudogram + sizeof(struct pseudo_header) , udph , sizeof(struct udphdr) + len);
        udph->uh_sum = csum( (unsigned short*) pseudogram , psize);
        free(pseudogram);
    }

    //Send the packet
    if (sendto (s, data, len,  0, (struct sockaddr *) sin, sizeof (*sin)) < 0) {
        perror("sendto failed");
        return 1;
    }
    return 0;
}
