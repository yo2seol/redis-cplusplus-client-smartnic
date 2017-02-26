/* Copyright (c) 2017 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

#include "UnsyncedRpcTracker.h"
#include <cstring>
#include <iostream>
#include "anet.h"
#include "redisclient.h"

namespace RAMCloud {

/**
 * Default constructor
 */
UnsyncedRpcTracker::UnsyncedRpcTracker()
    : masters()
    , mutex()
    , lastOpNum(0)
{
}

/**
 * Default destructor
 */
UnsyncedRpcTracker::~UnsyncedRpcTracker()
{
    Lock _(mutex);
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        while (!master->rpcs.empty()) {
            free(master->rpcs.front().data);
            master->rpcs.pop();
        }
        delete master;
    }
}

/**
 * Saves the information of an non-durable RPC whose response is received.
 * Any non-durable RPC should register itself before returning wait() call.
 *
 * \param session
 *      Transport session where this RPC is sent to.
 * \param rpcRequest
 *      Pointer to RPC request which is previously sent to target master.
 *      This memory must be either from Allocator or malloc.
 *      Onwership of this memory is now in UnsyncedRpcTracker. No one should
 *      modify or free this memory.
 * \param tableId
 *      The table containing the desired object.
 * \param keyHash
 *      Key hash that identifies a particular tablet.
 * \param objVer
 *      Version of object after applying modifications by this RPC.
 * \param logPos
 *      Location of new value of object in master after applying this RPC.
 * \param callback
 *      Callback desired to be invoked when the modifications on the object
 *      by this RPC becomes durable (by replicating to backups).
 */
void
UnsyncedRpcTracker::registerUnsynced(int socket, int dbindex,
                                     const std::string & msg,
                                     uint64_t opNumInServer,
                                     uint64_t syncedInServer)
{
    Lock lock(mutex);
    Master* master = getOrInitMasterRecord(socket);

    int size = msg.size();
    char* data = new char[size];
    std::memcpy(data, msg.data(), size);

    if (opNumInServer <= lastOpNum) {
        printf("Error. duplicate request? opNumInServer %lld, lastOpNum %lld\n",
                opNumInServer, lastOpNum);
    }
    lastOpNum = opNumInServer;
    master->rpcs.emplace(dbindex, data, size, opNumInServer, [](){});
    master->updateSyncState(syncedInServer);
}


inline static std::string&
rtrim(std::string & str, const std::string & ws = REDIS_WHITESPACE) {
    std::string::size_type pos = str.find_last_not_of(ws);
    str.erase(pos + 1);
    return str;
}

// Reads a single line of character data from the given blocking socket.
// Returns the line that was read, not including EOL delimiter(s).  Both LF
// ('\n') and CRLF ("\r\n") delimiters are supported.  If there was an I/O
// error reading from the socket, connection_error is raised.  If max_size
// bytes are read before finding an EOL delimiter, a blank string is
// returned.
static std::string
read_line(int socket, ssize_t max_size = 2048) {
    assert(socket > 0);
    assert(max_size > 0);

    std::ostringstream oss;

    enum {
        buffer_size = 64
    };
    char buffer[buffer_size];
    memset(buffer, 0, buffer_size);

    ssize_t total_bytes_read = 0;
    bool found_delimiter = false;

    while (total_bytes_read < max_size && !found_delimiter) {
        // Peek at what's available.
        ssize_t bytes_received = redis::recv_or_throw(socket, buffer, buffer_size, MSG_PEEK);

        // Some data is available; Length might be < buffer_size.
        // Look for newline in whatever was read though.
        char * eol = static_cast<char *> (memchr(buffer, '\n', bytes_received));

        // If found, write data from the buffer to the output string.
        // Else, write the entire buffer and continue reading more data.
        ssize_t to_read = bytes_received;

        if (eol) {
            to_read = eol - buffer + 1;
            oss.write(buffer, to_read);
            found_delimiter = true;
        } else
            oss.write(buffer, bytes_received);

        // Now read from the socket to remove the peeked data from the socket's
        // read buffer.  This will not block since we've peeked already and know
        // there's data waiting.  It might fail if we were interrupted however.
        bytes_received = redis::recv_or_throw(socket, buffer, to_read, 0);
    }

    // Construct final line string. Remove trailing CRLF-based whitespace.

    std::string line = oss.str();
    return rtrim(line, REDIS_LBR);
}

static bool recv_ok_reply_(int socket) {
    std::string line = read_line(socket);

    if (line.empty())
        throw redis::protocol_error("empty single line reply");

    if (line.find(REDIS_PREFIX_STATUS_REPLY_LOADING) == 0) {
        return false;
    }

    if (line.find(REDIS_PREFIX_STATUS_REPLY_ERROR) == 0) {
        std::string error_msg = line.substr(strlen(REDIS_PREFIX_STATUS_REPLY_ERROR));
        if (error_msg.empty())
            error_msg = "unknown error";
        throw redis::protocol_error(error_msg);
    }
    if (line.find(REDIS_PREFIX_STATUS_REPLY_RETRY) == 0) {
        throw redis::retry_error();
    }

    if (line[0] != REDIS_PREFIX_STATUS_REPLY_VALUE &&
            line[0] != REDIS_PREFIX_STATUS_REPLY_UNSYNCED) {
        fprintf(stderr, "Unexpected prefix. Resp: %s", line.c_str());
        throw redis::protocol_error("unexpected prefix for status reply");
    }

    if (line.substr(1,2) != REDIS_STATUS_REPLY_OK) {
        fprintf(stderr, "Unexpected OK response. Resp: %s", line.c_str());
        throw redis::protocol_error("expected OK response");
    }
    return true;
}

/**
 * Invoked if there is a problem with a session to master, which hints
 * a possible crash of the master. It will recover all possibly lost updates
 * by retrying requests that are not known to be replicated to backups.
 *
 * \param sessionPtr
 *      A raw pointer to session which is being destroyed.
 */
void
UnsyncedRpcTracker::flushSession(int disconnectedSocket, std::string hostIp, uint16_t replayPort)
{
    Lock lock(mutex);
    Master* master;
    MasterMap::iterator it = masters.find(disconnectedSocket);
    if (it != masters.end()) {
        master = it->second;
    } else {
        return;
    }

    fprintf(stderr, "Flushing session in UnsyncedRpcTracker. Total commands: %d\n",
                static_cast<int>(master->rpcs.size()));


conn_err:
    char err[ANET_ERR_LEN];
    int socketForReplay = anetTcpConnect(err, const_cast<char*>(hostIp.c_str()), replayPort);
    if (socketForReplay == ANET_ERR) {
        std::cerr << err << " (redis_replay://" << hostIp << ':' << replayPort << ")";
        sleep(1);
        goto conn_err;
    }
    anetTcpNoDelay(NULL, socketForReplay);

    int currentDbIndex = -1;

    int sentCommands;
    for (sentCommands = 0; !master->rpcs.empty(); ++sentCommands) {
        UnsyncedRpc& rpc = master->rpcs.front();
        if (rpc.dbindex != currentDbIndex) {
            std::string selectCmd = (std::string)(redis::makecmd("SELECT") << rpc.dbindex);
            if (anetWrite(socketForReplay, const_cast<char*>(selectCmd.data()), selectCmd.size()) == -1)
                goto conn_err;
            ++sentCommands;
            currentDbIndex = rpc.dbindex;
            if (!recv_ok_reply_(socketForReplay)) continue;
        }

        if (anetWrite(socketForReplay, rpc.data, rpc.size) == -1)
            goto conn_err;
        if (!recv_ok_reply_(socketForReplay)) continue;
        rpc.callback();
        free(rpc.data);
        master->rpcs.pop();
    }

    // TODO: pipelining for faster recovery...
//    for (int i = 0; i < sentCommands; ++i) {
//        uint64_t opNumInServer, syncNum;
//        recv_unsynced_ok_reply_(socketForReplay, &opNumInServer, &syncNum);
//    }

    lastOpNum = 0;
}

/**
 * Garbage collect RPC information for requests whose updates are made durable
 * and invoke callbacks for those requests.
 *
 * \param sessionPtr
 *      Session which represents a target master.
 * \param masterLogState
 *      Master's log state including the master's log position up to which
 *      all log is replicated to backups.
 */
void
UnsyncedRpcTracker::updateSyncState(int socket, uint64_t syncedInServer)
{
    Lock lock(mutex);
    Master* master;
    MasterMap::iterator it = masters.find(socket);
    if (it != masters.end()) {
        master = it->second;
    } else {
        return;
    }

    master->updateSyncState(syncedInServer);
}

/**
 * Check the liveness of the masters that has not been contacted recently.
 */
void
UnsyncedRpcTracker::pingMasterByTimeout()
{
    /*
    vector<ClientLease> victims;
    {
        Lock lock(mutex);

        // Sweep table and pick candidates.
        victims.reserve(Cleaner::maxIterPerPeriod / 10);

        ClientMap::iterator it;
        if (cleaner.nextClientToCheck) {
            it = clients.find(cleaner.nextClientToCheck);
        } else {
            it = clients.begin();
        }
        for (int i = 0; i < Cleaner::maxIterPerPeriod && it != clients.end();
                        //&& victims.size() < Cleaner::maxCheckPerPeriod;
             ++i, ++it) {
            Client* client = it->second;

            ClientLease lease = {it->first,
                                 client->leaseExpiration.getEncoded(),
                                 0};
            if (leaseValidator->needsValidation(lease)) {
                victims.push_back(lease);
            }
        }
        if (it == clients.end()) {
            cleaner.nextClientToCheck = 0;
        } else {
            cleaner.nextClientToCheck = it->first;
        }
    }

    // Check with coordinator whether the lease is expired.
    // And erase entry if the lease is expired.
    for (uint32_t i = 0; i < victims.size(); ++i) {
        Lock lock(mutex);
        // Do not clean if this client record is protected
        if (clients[victims[i].leaseId]->doNotRemove)
            continue;
        // Do not clean if there are RPCs still in progress for this client.
        if (clients[victims[i].leaseId]->numRpcsInProgress)
            continue;

        ClientLease lease = victims[i];
        if (leaseValidator->validate(lease, &lease)) {
            clients[victims[i].leaseId]->leaseExpiration =
                                            ClusterTime(lease.leaseExpiration);
        } else {
            TabletManager::Protector tp(tabletManager);
            if (tp.notReadyTabletExists()) {
                // Since there is a NOT_READY tablet (eg. recovery/migration),
                // we cannot garbage collect expired clients safely.
                // Both RpcResult entries and participant list entry need to be
                // recovered to make a correct GC decision, but with a tablet
                // currently NOT_READY, it is possible to have only RpcResult
                // recovered, not Transaction ParticipantList entry yet.
                return;
            }
            // After preventing the start of tablet migration or recovery,
            // check SingleClientProtector once more before deletion.
            if (clients[victims[i].leaseId]->doNotRemove)
                continue;

            clients.erase(victims[i].leaseId);
        }
    }
     */
}

/**
 * Wait for backup replication of all changes made by this client up to now.
 */
void
UnsyncedRpcTracker::sync()
{
    Lock lock(mutex);
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        if (!master->rpcs.empty()) {
            // Ask redis server to sync.
            int syncNum = 0; // TODO: extract from response.
            master->updateSyncState(syncNum);


//            master->syncRpcHolder.construct(ramcloud->clientContext,
//                                            master->session,
//                                            master->lastestLogState);
        }
    }
}

/**
 * Register a callback that will be invoked when all currently outstanding RPCs
 * are made durable.
 *
 * \param callback
 *      Callback desired to be invoked.
 */
void
UnsyncedRpcTracker::sync(std::function<void()> callback)
{
    Lock lock(mutex);
    int numMasters = 0;
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        if (!master->rpcs.empty()) {
            numMasters++;
        }
    }
    if (numMasters == 0) {
        return;
    }
    // TODO(seojin): Atomic necessary? think about cleaner thread more...
    //               Probably it is safe without atomic...
    //               Or it is already protected by mutex??
    int* syncedMasterCounter = new int;
    *syncedMasterCounter = 0;
    for (MasterMap::iterator it = masters.begin(); it != masters.end(); ++it) {
        Master* master = it->second;
        if (master->rpcs.empty()) {
            continue;
        }

        // TODO(seojin): chain the original callback.
        //               Use performant way to check previous value is empty.
        //std::function<void()> callback = master->rpcs.back().callback;
        master->rpcs.back().callback = [callback, numMasters,
                                        syncedMasterCounter] {
            (*syncedMasterCounter)++;
            if (*syncedMasterCounter >= numMasters) {
                callback();
                // This is the last access to syncedMasterCounter.
                delete syncedMasterCounter;
            }
        };
    }
}

/**
 * Return a pointer to the requested client record; create a new record if
 * one does not already exist.
 *
 * \param session
 *      The boost_intrusive pointer to transport session
 * \return
 *      Pointer to the existing or newly inserted master record.
 */
UnsyncedRpcTracker::Master*
UnsyncedRpcTracker::getOrInitMasterRecord(int socket)
{
    Master* master = NULL;
    MasterMap::iterator it = masters.find(socket);
    if (it != masters.end()) {
        master = it->second;
    } else {
        master = new Master();
        masters[socket] = master;
    }
    return master;
}

/////////////////////////////////////////
// Master
/////////////////////////////////////////

/**
 * Update the saved log state if the given one is newer, and
 * garbage collect RPC information for requests whose updates are made durable
 * and invoke callbacks for those requests.
 *
 * \param newLogState
 *      Master's log state including the master's log position up to which
 *      all log is replicated to backups.
 */
void
UnsyncedRpcTracker::Master::updateSyncState(uint64_t syncNum)
{
    if (lastestSyncNum < syncNum) {
        lastestSyncNum = syncNum;
    }

    while (!rpcs.empty()) {
        UnsyncedRpc& rpc = rpcs.front();
        if (rpc.opNum > syncNum) {
            break;
        }
        rpc.callback();
        free(rpc.data);
        rpcs.pop();
    }
}

} // namespace RAMCloud
