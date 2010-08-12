#include "qurlqueue_rpc.h"
#include <iostream>
#include <sys/time.h>

namespace qurlqueue {

int QUrlQueueServer::m_default_interval = 1000;
volatile uint64_t QUrlQueueServer::m_current_time = 0;

bool QUrlQueueServer::set_current_time()
{
    m_current_time = get_current_time();
    return true;
}

uint64_t QUrlQueueServer::get_current_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec * 1000 + (int)tv.tv_usec / 1000;
}


int QUrlQueueServer::push_url(const std::string &site, const std::string &record, bool push_front)
{
    if (m_stop_all) {
        return QCONTENTHUB_AGAIN;
    }

    mp::sync<site_map_t>::ref ref(m_site_map);
    site_map_it_t it = ref->find(site);
    if (it == ref->end()) {
        Site * s = new Site();
        s->url_queue.push_back(record);
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
        if (push_front) {
            s->url_queue.push_front(record);
        } else {
            s->url_queue.push_back(record);
        }

        s->enqueue_items++;
    }
    m_enqueue_items++;

    return QCONTENTHUB_OK;
}

void QUrlQueueServer::push_url(msgpack::rpc::request &req, const std::string &site, const std::string &record)
{	
    int ret = push_url(site, record);
    req.result(ret);
}

void QUrlQueueServer::push_list(msgpack::rpc::request &req, const std::string &site, const std::string &record)
{	
    int ret = push_url(site, record, true);
    req.result(ret);
}

void QUrlQueueServer::pop_url(std::string &content)
{
    mp::sync<site_map_t>::ref ref(m_site_map);

    if (m_stop_all) {
        content = QCONTENTHUB_STRAGAIN;
        return;
    }

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
            s->url_queue.pop_front();
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

void QUrlQueueServer::start_all(msgpack::rpc::request &req)
{
    m_stop_all = false;
    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::stop_all(msgpack::rpc::request &req)
{
    m_stop_all = true;
    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::clear_all(msgpack::rpc::request &req)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        for (site_map_it_t it = ref->begin(); it != ref->end(); it++) {
            it->second->url_queue.clear();
        }
    }

    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::start_dump_all(msgpack::rpc::request &req)
{
    if (m_dump_all_dumping) {
        req.result(QCONTENTHUB_ERROR);
    } else {
        m_dump_all_dumping = true;
        m_dump_all_it = m_site_map.unsafe_ref().begin();
        req.result(QCONTENTHUB_OK);
    }
}

void QUrlQueueServer::dump_all(msgpack::rpc::request &req)
{
    std::string content;
    if (!m_dump_all_dumping) {
        content = QCONTENTHUB_STRERROR;
    } else {
        mp::sync<site_map_t>::ref ref(m_site_map);
        while (m_dump_all_it != ref->end()) {
            Site *s = m_dump_all_it->second;
            if (s->url_queue.size() == 0) {
                s->dump_all_site_dumping = false;
                m_dump_all_it++;
                continue;
            } else {
                if (s->dump_all_site_dumping) {
                    if (s->dump_all_site_dump_it == s->url_queue.end()) {
                        s->dump_all_site_dumping = false;
                        m_dump_all_it++;
                        continue;
                    } else {
                        content = *(s->dump_all_site_dump_it);
                        s->dump_all_site_dump_it++;
                        break;
                    }
                } else {
                    s->dump_all_site_dumping = true;
                    s->dump_all_site_dump_it = s->url_queue.begin();
                    content = *(s->dump_all_site_dump_it);
                    s->dump_all_site_dump_it++;
                    break;
                }
            }
        }
        if (m_dump_all_it == ref->end()) {
            content = QCONTENTHUB_STREND;
            m_dump_all_dumping = false;
        }
    }
    req.result(content);
}

void QUrlQueueServer::start_dump_site(msgpack::rpc::request &req, const std::string &site)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        site_map_it_t it = ref->find(site);
        if (it != ref->end()) {
            Site *s = it->second;
            s->site_dumping = true;
            s->site_dump_it = s->url_queue.begin();
        }
    }

    req.result(QCONTENTHUB_OK);
}

void QUrlQueueServer::dump_site(msgpack::rpc::request &req, const std::string &site)
{
    std::string content;
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        site_map_it_t it = ref->find(site);
        if (it == ref->end()) {
            content = QCONTENTHUB_STREND;
        } else {
            Site *s = it->second;
            if (s->site_dump_it == s->url_queue.end()) {
                s->site_dumping = false;
                content = QCONTENTHUB_STREND;
            } else {
                content = *(s->site_dump_it);
                s->site_dump_it++;
            }
        }
    }

    req.result(content);
}

void QUrlQueueServer::clear_site(msgpack::rpc::request &req, const std::string &site)
{
    {
        mp::sync<site_map_t>::ref ref(m_site_map);
        site_map_it_t it = ref->find(site);
        if (it != ref->end()) {
            it->second->url_queue.clear();
        }
    }
    req.result(QCONTENTHUB_OK);
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

void QUrlQueueServer::stats(msgpack::rpc::request &req)
{
    char buf[64];
    std::string ret;
    uint64_t current = m_current_time / 1000;

    ret.append("STAT uptime ");
    sprintf(buf, "%ld", current - m_start_time);
    ret.append(buf);

    ret.append("STAT time ");
    sprintf(buf, "%ld", current);
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
        } else if(method == "push_list") {
            msgpack::type::tuple<std::string, std::string> params;
            req.params().convert(&params);
            push_list(req, params.get<0>(), params.get<1>());
        } else if(method == "start_dump_all") {
            start_dump_all(req);
        } else if(method == "dump_all") {
            dump_all(req);
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
        } else if(method == "clear_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            clear_site(req, params.get<0>());
        } else if(method == "start_dump_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            start_dump_site(req, params.get<0>());
        } else if(method == "dump_site") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            dump_site(req, params.get<0>());
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

void QUrlQueueServer::start(int multiple)
{
    m_start_time = get_current_time() / 1000;
    this->instance.run(multiple);
}

} // end namespace qurlqueue
