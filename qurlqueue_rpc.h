#ifndef QURLQUEUE_RPC_H
#define QURLQUEUE_RPC_H

#include <msgpack/rpc/loop.h>
#include <msgpack/rpc/server.h>
#include <mp/sync.h>
#include <map>
#include <queue>
#include <string>
#include "qcontenthub.h"

namespace qurlqueue {

class Site;
class SiteCmp;

typedef std::map<std::string, Site *> site_map_t;
typedef std::map<std::string, Site *>::iterator site_map_it_t;
typedef std::map<std::string, int> interval_map_t;
typedef std::map<std::string, int>::iterator interval_map_it_t;

class Site {
public:
    Site(): stop(false), ref_cnt(0), enqueue_items(0), dequeue_items(0), next_crawl_time(0) {};

    bool stop;
    std::string name;
    int ref_cnt;
    uint64_t enqueue_items;
    uint64_t dequeue_items;
    uint64_t next_crawl_time;

    std::queue<std::string> url_queue;
};

class SiteCmp {

public:
    bool operator() (Site* &lhs, Site* &rhs) const
    {
        return lhs->next_crawl_time < rhs->next_crawl_time;
    }

};

class QUrlQueueServer : public msgpack::rpc::server::base {

public:
    QUrlQueueServer(msgpack::rpc::loop lo = msgpack::rpc::loop()) : msgpack::rpc::server::base(lo), m_enqueue_items(0), m_dequeue_items(0){}
    void push_url(msgpack::rpc::request &req, const std::string &site, const std::string &record);
    void push_url(const std::string &site, const std::string &record);
    void pop_url(msgpack::rpc::request &req);
    void pop_url(std::string &ret);
    void set_default_interval(msgpack::rpc::request &req, int interval);
    void set_site_interval(msgpack::rpc::request &req, const std::string &site, int interval);
    void set_capacity(msgpack::rpc::request &req, int capacity);
    void stats(msgpack::rpc::request &req);
    void stat_site(msgpack::rpc::request &req, const std::string &site);
    void start_site(msgpack::rpc::request &req, const std::string &site);
    void stop_site(msgpack::rpc::request &req, const std::string &site);
    void clear_empty_site(msgpack::rpc::request &req);
    int clear_empty_site();

public:
    void dispatch(msgpack::rpc::request req);

public:
    static bool set_current_time();
private:
    uint64_t m_capacity;
    static int  m_default_interval;
    uint64_t m_enqueue_items;
    uint64_t m_dequeue_items;

    std::priority_queue<Site *, std::vector<Site*>, SiteCmp>  ordered_sites;
    interval_map_t m_interval_map;
    mp::sync<site_map_t> m_site_map;

    static volatile uint64_t m_current_time;
};

} // end namespace qurlqueue

#endif

