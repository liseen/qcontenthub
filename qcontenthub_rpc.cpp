#include  "qcontenthub_rpc.h"

#define ENTER_FUNCTION \
    std::cout << "enter " << __FUNCTION__ << std::endl;

#define QUIT_FUNCTION \
    std::cout << "quit " << __FUNCTION__ << std::endl;

static int start_thread_num = 100;
static int limit_thread_num = 70;

void QContentHubServer::add_queue(msgpack::rpc::request req, const std::string &name, int capacity)
{
	mp::sync<queue_map_t>::ref ref(q_map);
    queue_map_it_t it = ref->find(name);
    if (it == ref->end()) {
        queue_t * q = new queue_t();
        if (q == NULL) {
            req.result(QCONTENTHUB_ERROR);
            return;
        }
        pthread_mutex_init(&q->lock,NULL);
        pthread_cond_init(&q->not_empty,NULL);
        pthread_cond_init(&q->not_full,NULL);

        q->stop = 0;
        q->capacity = capacity;
        (*ref)[name] = q;
        req.result(QCONTENTHUB_OK);
    } else {
        req.result(QCONTENTHUB_WARN);
    }
}

void QContentHubServer::del_queue(msgpack::rpc::request req, const std::string &name)
{
    mp::sync<queue_map_t>::ref ref(q_map);
    queue_map_it_t it = ref->find(name);
    if (it == ref->end()) {
        req.result(QCONTENTHUB_WARN);
    } else if (it->second->str_q.size() == 0) {
        delete it->second;
        ref->erase(it);
        req.result(QCONTENTHUB_OK);
    } else {
        req.result(QCONTENTHUB_WARN);
    }
}

void QContentHubServer::start_queue(msgpack::rpc::request req, const std::string &name)
{
    queue_map_t &qmap = q_map.unsafe_ref();
    queue_map_it_t it = qmap.find(name);
    if (it == qmap.end()) {
        req.result(QCONTENTHUB_WARN);
    } else {
        queue_t *q = it->second;
        pthread_mutex_lock(&q->lock);
        q->stop = 0;
        pthread_mutex_unlock(&q->lock);
    }
}

void QContentHubServer::stop_queue(msgpack::rpc::request req, const std::string &name)
{
    queue_map_t &qmap = q_map.unsafe_ref();
    queue_map_it_t it = qmap.find(name);
    if (it == qmap.end()) {
        req.result(QCONTENTHUB_WARN);
    } else {
        queue_t *q = it->second;
        pthread_mutex_lock(&q->lock);
        q->stop = 1;
        pthread_mutex_unlock(&q->lock);
    }
}

void QContentHubServer::force_del_queue(msgpack::rpc::request req, const std::string &name)
{
	mp::sync<queue_map_t>::ref ref(q_map);
    queue_map_it_t it = ref->find(name);
    if (it == ref->end()) {
        req.result(QCONTENTHUB_WARN);
    } else {
        delete it->second;
        ref->erase(it);
        req.result(QCONTENTHUB_OK);
    }
}


void QContentHubServer::push_queue(msgpack::rpc::request req, const std::string &name, const std::string &obj)
{
    queue_map_t &qmap = q_map.unsafe_ref();

    queue_map_it_t it = qmap.find(name);
    if (it == qmap.end()) {
        /*
        queue_t * q = new queue_t();
        q->capacity = DEFAULT_QUEUE_CAPACITY;
        q->str_q.push(obj);
        q_map[name] = q;
        req.result(QCONTENTHUB_OK);
        */
        req.result(QCONTENTHUB_ERROR);
    } else {
        queue_t *q = it->second;
        pthread_mutex_lock(&q->lock);

        if (q->stop || (int)q->str_q.size() >= q->capacity) {
            {
	            mp::sync<cond_stat_t>::ref ref(cond_stat);
                ref->push_cnt++;
            }

            if (cond_stat.unsafe_ref().push_cnt > limit_thread_num ) {
                pthread_mutex_unlock(&q->lock);
                req.result(QCONTENTHUB_ERROR);
                return;
            }

            pthread_cond_wait(&q->not_full, &q->lock);

            {
	            mp::sync<cond_stat_t>::ref ref(cond_stat);
                ref->push_cnt--;
            }

        }
        q->str_q.push(obj);
        req.result(QCONTENTHUB_OK);
        pthread_cond_signal(&q->not_empty);
        pthread_mutex_unlock(&q->lock);
    }
}

void QContentHubServer::pop_queue(msgpack::rpc::request req, const std::string &name)
{
    queue_map_t &qmap = q_map.unsafe_ref();
    queue_map_it_t it = qmap.find(name);
    if (it == qmap.end()) {
        req.result(QCONTENTHUB_STRAGAIN);
    } else {
        queue_t *q = it->second;
        pthread_mutex_lock(&q->lock);
        if (q->str_q.size() == 0) {
            {
	            mp::sync<cond_stat_t>::ref ref(cond_stat);
                ref->pop_cnt++;
            }
            if (cond_stat.unsafe_ref().pop_cnt > limit_thread_num ) {
                pthread_mutex_unlock(&q->lock);
                req.result(QCONTENTHUB_ERROR);
                return;
            }

            pthread_cond_wait(&q->not_empty, &q->lock);

            {
	            mp::sync<cond_stat_t>::ref ref(cond_stat);
                ref->pop_cnt++;
            }
        }

        req.result(q->str_q.front());
        q->str_q.pop();
        if (!q->stop) {
            pthread_cond_signal(&q->not_full);
        }
        pthread_mutex_unlock(&q->lock);
    }
}

void QContentHubServer::stats(msgpack::rpc::request req)
{
    char buf[30];
    std::string ret;
    queue_map_t &qmap = q_map.unsafe_ref();
    for (queue_map_it_t it = qmap.begin(); it != qmap.end(); it++) {
        ret.append("STAT name ");
        ret.append(it->first);
        ret.append("\n");
        ret.append("STAT size ");
        sprintf(buf, "%ld", it->second->str_q.size());
        ret.append(buf);
        ret.append("\n");
    }
    req.result(ret);
}

void QContentHubServer::stat_queue(msgpack::rpc::request req, const std::string &name)
{
    char buf[30];
    std::string ret;
    queue_map_t &qmap = q_map.unsafe_ref();
    queue_map_it_t it = qmap.find(name);
    if (it == qmap.end()) {
        req.result(ret);
    } else {
        ret.append("STAT name ");
        ret.append(name);
        ret.append("\n");
        ret.append("STAT size ");
        sprintf(buf, "%ld", it->second->str_q.size());
        ret.append(buf);
        ret.append("\n");
        req.result(ret);
    }
}

void QContentHubServer::dispatch(msgpack::rpc::request req) {
    try {
        std::string method;
        req.method().convert(&method);

        if(method == "add") {
            msgpack::type::tuple<std::string, int> params;
            req.params().convert(&params);
            add_queue(req, params.get<0>(), params.get<1>());
        } else if(method == "del") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            del_queue(req, params.get<0>());
        } else if(method == "start") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            start_queue(req, params.get<0>());
        } else if(method == "stop") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            stop_queue(req, params.get<0>());
        } else if(method == "fdel") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            force_del_queue(req, params.get<0>());
        } else if(method == "push") {
            msgpack::type::tuple<std::string, std::string> params;
            req.params().convert(&params);
            push_queue(req, params.get<0>(), params.get<1>());
        } else if(method == "pop") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            pop_queue(req, params.get<0>());
        } else if(method == "stats") {
            stats(req);
        } else if(method == "stat_queue") {
            msgpack::type::tuple<std::string> params;
            req.params().convert(&params);
            stat_queue(req, params.get<0>());
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

int main(void)
{
    QContentHubServer svr;
    svr.instance.listen("0.0.0.0", 9090);
    svr.instance.run(start_thread_num);

    return 0;
}
