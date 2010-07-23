#ifndef QCONTENTHUB_RPC_H
#define QCONTENTHUB_RPC_H

#include <msgpack/rpc/server.h>
#include <mp/sync.h>

#include <pthread.h>
#include <map>
#include <queue>
#define DEFAULT_QUEUE_CAPACITY 512

#define QCONTENTHUB_OK 0
#define QCONTENTHUB_WARN 2
#define QCONTENTHUB_ERROR -1

#define QCONTENTHUB_AGAIN 1


static const std::string QCONTENTHUB_STRAGAIN  = "#_#_#again";
static const std::string QCONTENTHUB_STRERROR = "#_#_#error";

struct cond_stat_t {
    cond_stat_t() : push_cnt(0), pop_cnt(0) {}
    int push_cnt;
    int pop_cnt;
};

struct queue_t {
    int capacity;
    int stop;
    pthread_mutex_t lock;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
    std::queue<std::string> str_q;
};

typedef std::map<std::string, queue_t *> queue_map_t;
typedef std::map<std::string, queue_t *>::iterator queue_map_it_t;

class QContentHubServer : public msgpack::rpc::server::base {

public:
    void add_queue(msgpack::rpc::request req, const std::string &name, int capacity);
    void del_queue(msgpack::rpc::request req, const std::string &name);
    void start_queue(msgpack::rpc::request req, const std::string &name);
    void stop_queue(msgpack::rpc::request req, const std::string &name);
    void force_del_queue(msgpack::rpc::request req, const std::string &name);
    void push_queue(msgpack::rpc::request req, const std::string &name, const std::string &obj);
    void pop_queue(msgpack::rpc::request req, const std::string &name);
    void stats(msgpack::rpc::request req);
    void stat_queue(msgpack::rpc::request req, const std::string &name);

public:
    void dispatch(msgpack::rpc::request req);

private:
	mp::sync<queue_map_t> q_map;
	mp::sync<cond_stat_t> cond_stat;
};

#endif
