#include "qurlqueue_rpc.h"
#include <iostream>
#include <sys/time.h>

namespace qurlqueue {

int QUrlQueueServer::m_default_interval = 1000;
volatile uint64_t QUrlQueueServer::m_current_time = 0;

bool QUrlQueueServer::set_current_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    m_current_time = tv.tv_sec * 1000 + (int)tv.tv_usec / 1000;
    return true;
}

void QUrlQueueServer::push_url(const std::string &site, const std::string &record)
{
    mp::sync<site_map_t>::ref ref(m_site_map);
    site_map_it_t it = ref->find(site);
    if (it == ref->end()) {
        Site * s = new Site();
        s->url_queue.push(record);
        s->name = site;
        s->enqueue_items = 1;
        ref->insert(std::pair<std::string, Site *>(site, s));
        s->ref_cnt++;
        ordered_sites.push(s);
    } else {
        Site * s = it->second;
        if (s->url_queue.size() == 0) {
            s->ref_cnt++;
            ordered_sites.push(s);
        }
        s->url_queue.push(record);
        s->enqueue_items++;
    }
    m_enqueue_items++;

}

void QUrlQueueServer::push_url(msgpack::rpc::request &req, const std::string &site, const std::string &record)
{	
    push_url(site, record);
    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::pop_url(std::string &content)
{
    mp::sync<site_map_t>::ref ref(m_site_map);

    while (!ordered_sites.empty()) {
        Site * s = ordered_sites.top();
        if (s->stop || s->url_queue.size() == 0) {
            s->ref_cnt--;
            ordered_sites.pop();
           // do nothing
        } else if (s->next_crawl_time > m_current_time) {
            content = QCONTENTHUB_STRAGAIN;
            return;
        } else {
            ordered_sites.pop();
            int interval;
            interval_map_it_t it = m_interval_map.find(s->name);
            if (it == m_interval_map.end()) {
                interval = m_default_interval;
            } else {
                interval = it->second;
            }
            s->next_crawl_time = m_current_time + interval;
            ordered_sites.push(s);
            content = s->url_queue.front();
            s->dequeue_items++;
            m_dequeue_items++;
            s->url_queue.pop();
            return;
        }
    }

    content = QCONTENTHUB_STRAGAIN;
}

void QUrlQueueServer::pop_url(msgpack::rpc::request &req)
{
    std::string ret;
    pop_url(ret);
    req.result(ret);
}

void QUrlQueueServer::set_default_interval(msgpack::rpc::request &req, int interval)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        m_default_interval = interval;
    }
    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::set_site_interval(msgpack::rpc::request &req, const std::string &site, int interval)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        m_interval_map[site] = interval;
    }
    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::set_capacity(msgpack::rpc::request &req, int capacity)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        m_capacity = capacity;
    }
    req.result(QCONTENTHUB_OK);
}


void QUrlQueueServer::stats(msgpack::rpc::request &req)
{
    char buf[64];
    std::string ret;
    ret.append("STAT time ");
    sprintf(buf, "%ld", m_current_time / 1000);
    ret.append(buf);

    ret.append("\nSTAT default_interval ");
    sprintf(buf, "%d", m_default_interval);
    ret.append(buf);

    ret.append("\nSTAT site_items ");
    sprintf(buf, "%ld", m_site_map.unsafe_ref().size());
    ret.append(buf);

    ret.append("\nSTAT ordered_site_items ");
    sprintf(buf, "%ld", ordered_sites.size());
    ret.append(buf);

    ret.append("\nSTAT enqueue_items ");
    sprintf(buf, "%ld", m_enqueue_items);
    ret.append(buf);

    ret.append("\nSTAT dequeue_items ");
    sprintf(buf, "%ld", m_dequeue_items);
    ret.append(buf);

    // TODO:
    // STAT curr_connections 141

    ret.append("\nEND\r\n");
    req.result(ret);
}

void QUrlQueueServer::stat_site(msgpack::rpc::request &req, const std::string &site)
{
    char buf[64];
    std::string ret;
    mp::sync<site_map_t>::ref ref(m_site_map);
    ret.append("STAT site ");
    ret.append(site);

    interval_map_it_t interval_it = m_interval_map.find(site);
    ret.append("\nSTAT interval ");
    if (interval_it == m_interval_map.end()) {
        ret.append("default ");
        sprintf(buf, "%d", m_default_interval);
        ret.append(buf);
    } else {
        sprintf(buf, "%d", interval_it->second);
        ret.append(buf);
    }

    site_map_it_t it = ref->find(site);
    if (it != ref->end()) {
        ret.append("\nSTAT stop ");
        if (it->second->stop) {
            ret.append("1");
        } else {
            ret.append("0");
        }

        ret.append("\nSTAT enqueue_items ");
        sprintf(buf, "%ld", it->second->enqueue_items);
        ret.append(buf);

        ret.append("\nSTAT dequeue_items ");
        sprintf(buf, "%ld", it->second->dequeue_items);
        ret.append(buf);
    }

    ret.append("\nEND\r\n");
    req.result(ret);
}

void QUrlQueueServer::start_site(msgpack::rpc::request &req, const std::string &site)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        site_map_it_t it = ref->find(site);
        if (it != ref->end()) {
            Site * s = it->second;
            it->second->stop = false;
            s->ref_cnt++;
            ordered_sites.push(s);
        }
    }

    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::stop_site(msgpack::rpc::request &req, const std::string &site)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        site_map_it_t it = ref->find(site);
        if (it != ref->end()) {
            it->second->stop = true;
        }
    }

    req.result(QCONTENTHUB_OK);
}


void QUrlQueueServer::clear_empty_site(msgpack::rpc::request &req)
{
    int ret = clear_empty_site();
    req.result(ret);
}

int QUrlQueueServer::clear_empty_site()
{
    mp::sync<site_map_t>::ref ref(m_site_map);
    std::vector<std::string> site_vec;
    for (site_map_it_t it = ref->begin(); it != ref->end(); it++) {
        Site *s = it->second;
        if (s->next_crawl_time > 0 && s->next_crawl_time < m_current_time - 86400000  && s->url_queue.size() == 0 && s->ref_cnt == 0) {
            site_vec.push_back(it->first);
        }
        if (site_vec.size() > 2000) {
            break;
        }
    }

    int site_vec_size = site_vec.size();
    for (int i = 0; i < site_vec_size; i++) {
        site_map_it_t del_it = ref->find(site_vec[i]);
        if (del_it != ref->end()) {
            delete del_it->second;
            ref->erase(del_it);
        }
    }

    return site_vec_size;
}


void QUrlQueueServer::dispatch(msgpack::rpc::request req)
{
    try {
        std::string method;
        req.method().convert(&method);

        if(method == "push") {
            msgpack::type::tuple<std::string, std::string> params;
            req.params().convert(&params);
            push_url(req, params.get<0>(), params.get<1>());
        } else if(method == "pop") {
            pop_url(req);
        } else if(method == "stats") {
            stats(req);
        } else if(method == "set_default_interval") {
            msgpack::type::tuple<int> params;
            req.params().convert(&params);
            set_default_interval(req, params.get<0>());
        } else if(method == "set_site_interval") {
            msgpack::type::tuple<std::string, int> params;
            req.params().convert(&params);
            set_site_interval(req, params.get<0>(), params.get<1>());
        } else if(method == "stat_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            stat_site(req, params.get<0>());
        } else if(method == "start_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            start_site(req, params.get<0>());
        } else if(method == "stop_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            stop_site(req, params.get<0>());
        } else if(method == "clear_empty_site") {
            clear_empty_site(req);
        } else {
            req.error(msgpack::rpc::NO_METHOD_ERROR);
        }
    } catch (msgpack::type_error& e) {
        req.error(msgpack::rpc::ARGUMENT_ERROR);
        return;

    } catch (std::exception& e) {
        req.error(std::string(e.what()));
        return;
    }
}

} // end namespace qurlqueue
