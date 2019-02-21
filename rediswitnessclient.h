/* rediswitnesclient.h -- a C++ client library for redis.
 */

#ifndef REDISWITNESSCLIENT_H
#define REDISWITNESSCLIENT_H

#include <errno.h>
#include <sys/socket.h>

#include <string>
#include <vector>
#include <map>
#include <set>
#include <stdexcept>
#include <ctime>
#include <sstream>
#include <errno.h>

#include <boost/concept_check.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/functional/hash.hpp>
#include <boost/foreach.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/random.hpp>
#include <boost/cstdint.hpp>
#include <boost/date_time/posix_time/ptime.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/optional.hpp>
#include <boost/variant.hpp>

#include "util.h"
#include "udp.h"
#include "anet.h"
#include "UnsyncedRpcTracker.h"
#include "MurmurHash3.h"
#include "TimeTrace.h"

extern "C"
{
    #include "witnesscmd.h"
}

#define SRC_ADDR "10.10.105.101"
#define WITNESS_CLIENT_PORT 2222
#define WITNESS_PORT 1111

using PerfUtils::TimeTrace;

template<class Object >
struct make;
namespace redis {
    template<typename CONSISTENT_HASHER>
    class base_witness_client;

    enum reply_t {
        no_reply,
        status_code_reply,
        error_reply,
        int_reply,
        bulk_reply,
        multi_bulk_reply
    };

    struct connection_data {

        connection_data(const std::string & host = "localhost", uint16_t port = WITNESS_PORT,
                        uint16_t replayPort = WITNESS_CLIENT_PORT, int dbindex = 0)
        : host(host), port(port), replayPort(replayPort), dbindex(dbindex), socket(ANET_ERR) {
        }

        bool operator==(const connection_data & other) const {
            if (host != other.host)
                return false;
            if (port != other.port)
                return false;
            if (dbindex != other.dbindex)
                return false;

            return true;
        }

        std::string host;
        boost::uint16_t port;
        boost::uint16_t replayPort;
        int dbindex;

        private:
            int socket;
            struct sockaddr_in sin;
            template<typename CONSISTENT_HASHER>
            friend class base_witness_client;
    };

    // Generic error that is thrown when communicating with the redis server.

    class redis_error : public std::exception {
    public:

        redis_error(const std::string & err) : err_(err) {
        }

        virtual ~redis_error() throw () {
        }

        operator const std::string() const {
            return err_;
        }

        virtual const char* what() const throw () {
            return err_.c_str();
        }

    private:
        std::string err_;
    };

    // Some socket-level I/O or general connection error.

    class connection_error : public redis_error {
    public:

        connection_error(const std::string & err) : redis_error(err) {
        }
    };

    // Server is not yet ready for accepting normal request.
    class retry_error : public redis_error {
    public:
        retry_error() : redis_error(std::string()) {
        };
    };

    // Redis gave us a reply we were not expecting.
    // Possibly an internal error (here or in redis, probably here).

    class protocol_error : public redis_error {
    public:

        protocol_error(const std::string & err) : redis_error(err) {
        };
    };

    // A key that you expected to exist does not in fact exist.

    class key_error : public redis_error {
    public:

        key_error(const std::string & err) : redis_error(err) {
        };
    };

    // A operation with time limit does not deliver an result early enough.

    class timeout_error : public redis_error {
    public:

        timeout_error(const std::string & err) : redis_error(err) {
        };
    };

    // A value of an expected type or other semantics was found to be invalid.

    class value_error : public redis_error {
    public:

        value_error(const std::string & err) : redis_error(err) {
        };
    };

    struct key {

        explicit key(const std::string & name)
        : name(name) {
        }

        std::string name;
    };

    class fastcmd {
    public:

        explicit fastcmd(int argc, const std::string & cmd_name) {
            strbuf = inlineBuf;
            bufSize = inlineBufSize;
            strbuf[0] = '*';
            appended = 1;
            char* buf = strbuf + appended;
            itoa_custom(argc, buf, 10);
            buf += strlen(buf);
            memcpy(buf, newlineDollar, 3);
            buf += 3;

            itoa_custom(cmd_name.length(), buf, 10);
            buf += strlen(buf);
            memcpy(buf, newline, 2);
            buf += 2;
            memcpy(buf, cmd_name.data(), cmd_name.length());
            buf += cmd_name.length();
            memcpy(buf, newlineDollar, 3);
            buf += 3;
            appended = buf - strbuf;
        }

        ~fastcmd() {
            destroy();
        }

        inline fastcmd& append(const char* value, int size) {
            ensureSpace(size + 15);
            char* buf = strbuf + appended;

            itoa_custom(size, buf, 10);
            buf += strlen(buf);
            memcpy(buf, newline, 2);
            buf += 2;
            memcpy(buf, value, size);
            buf += size;
            memcpy(buf, newlineDollar, 3);
            buf += 3;

            appended = buf - strbuf;
            return *this;
        }

        inline fastcmd& operator<<(const std::string& value) {
            ensureSpace(value.length() + 15);
            char* buf = strbuf + appended;

            itoa_custom(value.length(), buf, 10);
            buf += strlen(buf);
            memcpy(buf, newline, 2);
            buf += 2;
            memcpy(buf, value.data(), value.length());
            buf += value.length();
            memcpy(buf, newlineDollar, 3);
            buf += 3;

            appended = buf - strbuf;
            return *this;
        }

        inline fastcmd& operator<<(const uint64_t datum) {
            ensureSpace(30);
            char* buf = strbuf + appended;
            char* bufPosForIntLen = buf;
            if (datum >= 0x40000000000000) { // TODO: check for correct number.
                buf += 4; // 2 for int value, 2 for \r\n
            } else {
                buf += 3; // 1 for int value, 2 for \r\n
            }

            ulltoa64_custom(buf, 30 - 4, datum);
//            ulltoa(datum, buf, 10);
            int datumLen = strlen(buf);
            buf += datumLen;
            memcpy(buf, newlineDollar, 3);
            buf += 3;

            itoa_custom(datumLen, bufPosForIntLen, 10);
            bufPosForIntLen += strlen(bufPosForIntLen);
            memcpy(bufPosForIntLen, newline, 2);

            appended = buf - strbuf;
            return *this;
        }

        inline fastcmd& operator<<(const int datum) {
            ensureSpace(20);
            char* buf = strbuf + appended;
            char* bufPosForIntLen = buf;
            if (datum < 0) {
                fprintf(stderr, "negative integer is not supported in fast cmd.");
                exit(1);
            }
            if (datum >= 1000000000) {
                buf += 4; // 2 for int value, 2 for \r\n
            } else {
                buf += 3; // 1 for int value, 2 for \r\n
            }

            itoa_custom(datum, buf, 10);
            int datumLen = strlen(buf);
            buf += datumLen;
            memcpy(buf, newlineDollar, 3);
            buf += 3;

            itoa_custom(datumLen, bufPosForIntLen, 10);
            bufPosForIntLen += strlen(bufPosForIntLen);
            memcpy(bufPosForIntLen, newline, 2);

            appended = buf - strbuf;
            return *this;
        }

        char* c_str() {
            strbuf[appended - 1] = 0;
            return strbuf;
        }
        int size() {
            return appended - 1; // Don't count tailing '$'.
        }
        char* data() {
            return strbuf;
        }

    private:
        inline void ensureSpace(int size) {
            if (appended + size > bufSize) {
                char* newBuf = new char[bufSize * 2];
                memcpy(newBuf, strbuf, appended);
                destroy();
                strbuf = newBuf;
                bufSize = bufSize * 2;
            }
        }

        void destroy() {
            if (strbuf != inlineBuf) {
                delete[] strbuf;
            }
        }


        char* strbuf;
        int bufSize;
        int appended = 0;
        char inlineBuf[1000];
        const static int inlineBufSize = 1000;

        static constexpr const char* newline = "\r\n";
        static constexpr const char* newlineDollar = "\r\n$";
    };

    template<typename CONSISTENT_HASHER>
    class base_witness_client;

    // You should construct a 'client' object per connection to a redis-server.
    //
    // Please read the online redis command reference:
    // http://code.google.com/p/redis/wiki/CommandReference
    //
    // No provisions for customizing the allocator on the string/bulk value type
    // (std::string) are provided.  If needed, you can always change the
    // string_type typedef in your local version.

    template<typename CONSISTENT_HASHER>
    class base_witness_client {
    private:

        void init(connection_data & con) {
            char err[ANET_ERR_LEN];
            con.socket = createSocket();
            if (con.socket == ANET_ERR) {
                std::ostringstream os;
                os << err << " (redis://" << con.host << ':' << con.port << ")";
                throw connection_error(os.str());
            }
            con.sin.sin_family = AF_INET;
            con.sin.sin_port = htons(WITNESS_PORT);
            con.sin.sin_addr.s_addr = inet_addr(con.host.c_str());
        }

    public:
        typedef std::string string_type;
        typedef std::vector<string_type> string_vector;
        typedef std::pair<string_type, string_type> string_pair;
        typedef std::vector<string_pair> string_pair_vector;
        typedef std::pair<string_type, double> string_score_pair;
        typedef std::vector<string_score_pair> string_score_vector;
        typedef std::set<string_type> string_set;

        typedef long int_type;

        explicit base_witness_client(const string_type & host,
                                     uint16_t port = WITNESS_PORT,
                                     uint16_t replayPort = WITNESS_CLIENT_PORT,
                                     int_type dbindex = 0)
        {
            clientId = rand() + 1; // Must not be 0.
            lastRequestId = 0;
            connection_data con;
            con.host = host;
            con.port = port;
            con.replayPort = replayPort;
            con.dbindex = dbindex;
            init(con);
            connections_.push_back(con);
        }

        template<typename CON_ITERATOR>
        base_witness_client(CON_ITERATOR begin, CON_ITERATOR end) {
            while (begin != end) {
                connection_data con = *begin;
                init(con);
                connections_.push_back(con);
                begin++;
            }

            if (connections_.empty())
                throw std::runtime_error("No connections given!");
        }

        ~base_witness_client() {

            BOOST_FOREACH(connection_data & con, connections_) {
                if (con.socket != ANET_ERR)
                    close(con.socket);
            }
        }

        const std::vector<connection_data> & connections() const {
            return connections_;
        }

        /**
         * Receive one reply which has witness record result.
         * \return
         *      returns true if accepted. false if rejected.
         */
        bool recv_witness_reply_(int socket, sockaddr_in sin) {
            uint blen;
            char buffer;
            recvfrom(socket, &buffer, 1,
                     MSG_WAITALL, (struct sockaddr *) &sin,
                     &blen);
            TimeTrace::record("Received reply from a witness.");
            return buffer == 0;
        }

        // Only use for single witness benchmarking.
        void witnessset(const string_type & key,
                        const string_type & value) {
            TimeTrace::record("Staring witnessset operation.");
            uint32_t keyHash;
            MurmurHash3_x86_32(key.data(), key.size(), connections_[0].dbindex, &keyHash);
            int hashIndex = keyHash & 1023;
            int socket = connections_[0].socket;
            fastcmd request(5, "SET");
            request << key << value << clientId << ++lastRequestId;
            witnesscmd_t cmd;
            create_add_wcmd(&cmd, clientId, ++lastRequestId,
                hashIndex, request.data(), request.size()); 
            TimeTrace::record("Constructed witness record request string.");
            udpWrite(socket,
                     SRC_ADDR,
                     connections_[0].host.c_str(),
                     WITNESS_CLIENT_PORT,
                     WITNESS_PORT,
                     witness_data(&cmd),
                     witness_size(&cmd),
                     &connections_[0].sin,
                     false);
            recv_witness_reply_(socket, connections_[0].sin);
            TimeTrace::record("Sent to witness");
        }

        // Only use for single witness benchmarking.
        void witnessgc(const string_type & key,
                        const uint64_t requestId) {
            TimeTrace::record("Staring witnessgc operation.");
            uint32_t keyHash;
            MurmurHash3_x86_32(key.data(), key.size(), connections_[0].dbindex, &keyHash);
            int hashIndex = keyHash & 1023;
            int socket = connections_[0].socket;
            /*
            fastcmd cmd(5, "WGC");
            cmd << "1" << (uint64_t)hashIndex << clientId << requestId;
            send_(socket, cmd.data(), cmd.size());
            string_vector unsyncedRPCCounts;
            recv_multi_bulk_reply_(socket, unsyncedRPCCounts); 

            //recv_ok_reply_(socket);
            */
            TimeTrace::record("Sent gc to witness");
        }

        /**
         * Receive one reply which has witness record result.
         * \return
         *      returns true if accepted. false if rejected.
         */
        bool recv_witness_reply_(int socket) {
            return true;
            /*
            std::string reply = recv_single_line_reply_(socket);
            TimeTrace::record("Received reply from a witness.");
            std::string content = reply.substr(0,6);
            if (content == REDIS_STATUS_REPLY_REJECT) {
                return false;
            } else if (content == REDIS_STATUS_REPLY_ACCEPT) {
                return true;
            } else {
                throw protocol_error("Reply format for witness record request doesn't match.");
            }
            */
        }

    private:
        base_witness_client(const base_witness_client &);
        base_witness_client & operator=(const base_witness_client &);

        void send_(int socket, const std::string & msg) {
            if (anetWrite(socket, const_cast<char *> (msg.data()), msg.size()) == -1)
                throw connection_error(strerror(errno));
//            handle_connection_error(socket);
        }
        void send_(int socket, const char* data, int size) {
            if (anetWrite(socket, const_cast<char *>(data), size) == -1)
                throw connection_error(strerror(errno));
//            handle_connection_error(socket);
        }

        void handle_connection_error(int socket) {
            connection_data conn = connections_[get_connIdx(socket)];
            tracker.flushSession(socket, conn.host, conn.replayPort);
//            throw connection_error(strerror(errno));
        }

        std::string recv_single_line_reply_(int socket) {
            return std::string("");
        }

        void recv_ok_reply_(int socket) {

        }

        connection_data& get_conn(const string_type & key) {
            size_t con_count = connections_.size();
            if (con_count == 1)
                return connections_[0];
            size_t idx = hasher_(key, static_cast<const std::vector<connection_data> &> (connections_));
            return connections_[idx];
        }

        int get_connIdx(int socket) {
            for (uint i = 0; i < connections_.size(); ++i) {
                if (connections_[i].socket == socket) {
                    return static_cast<int>(i);
                }
            }
            fprintf(stderr, "connIdx not found. socket: %d\n", socket);
            throw redis_error("Something wrong.. get_connIdx failed.");
        }

        inline int get_socket(const string_type & key) {
            size_t con_count = connections_.size();
            if (con_count == 1)
                return connections_[0].socket;
            size_t idx = hasher_(key, static_cast<const std::vector<connection_data> &> (connections_));
            return connections_[idx].socket;
        }

        int get_socket(const string_vector & keys) {
            assert(!keys.empty());

            if (connections_.size() == 1)
                return connections_[0].socket;

            int socket = -1;
            for (size_t i = 0; i < keys.size(); i++) {
                int cur_socket = get_socket(keys[i]);
                if (i > 0 && socket != cur_socket)
                    return -1;
                //throw std::runtime_error("not possible in cluster mode");

                socket = cur_socket;
            }

            return socket;
        }

    private:
        std::vector<connection_data> connections_;
        CONSISTENT_HASHER hasher_;
    public:
        uint64_t clientId; // Must not be 0. either random or assigned by server.
        uint64_t lastRequestId;
        RAMCloud::UnsyncedRpcTracker tracker;
    };

    struct default_hasher {

        inline size_t operator()(const std::string & key, const std::vector<connection_data> & connections) {
            return boost::hash<std::string>()(key) % connections.size();
        }
    };

    typedef base_witness_client<default_hasher> witness_client;
}

#endif // REDISWITNESSCLIENT_H
