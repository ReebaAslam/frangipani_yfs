#include "paxos.h"
#include "handle.h"
// #include <signal.h>
#include <stdio.h>

// This module implements the proposer and acceptor of the Paxos
// distributed algorithm as described by Lamport's "Paxos Made
// Simple".  To kick off an instance of Paxos, the caller supplies a
// list of nodes, a proposed value, and invokes the proposer.  If the
// majority of the nodes agree on the proposed value after running
// this instance of Paxos, the acceptor invokes the upcall
// paxos_commit to inform higher layers of the agreed value for this
// instance.


bool
operator> (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m > b.m));
}

bool
operator>= (const prop_t &a, const prop_t &b)
{
  return (a.n > b.n || (a.n == b.n && a.m >= b.m));
}

std::string
print_members(const std::vector<std::string> &nodes)
{
  std::string s;
  s.clear();
  for (unsigned i = 0; i < nodes.size(); i++) {
    s += nodes[i];
    if (i < (nodes.size()-1))
      s += ",";
  }
  return s;
}

bool isamember(std::string m, const std::vector<std::string> &nodes)
{
  for (unsigned i = 0; i < nodes.size(); i++) {
    if (nodes[i] == m) return 1;
  }
  return 0;
}

bool
proposer::isrunning()
{
  bool r;
  assert(pthread_mutex_lock(&pxs_mutex)==0);
  r = !stable;
  assert(pthread_mutex_unlock(&pxs_mutex)==0);
  return r;
}

// check if the servers in l2 contains a majority of servers in l1
bool
proposer::majority(const std::vector<std::string> &l1, 
		const std::vector<std::string> &l2)
{
  unsigned n = 0;

  for (unsigned i = 0; i < l1.size(); i++) {
    if (isamember(l1[i], l2))
      n++;
  }
  return n >= (l1.size() >> 1) + 1;
}

proposer::proposer(class paxos_change *_cfg, class acceptor *_acceptor, 
		   std::string _me)
  : cfg(_cfg), acc (_acceptor), me (_me), break1 (false), break2 (false), 
    stable (true)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

}

void
proposer::setn()
{
  my_n.n = acc->get_n_h().n + 1 > my_n.n + 1 ? acc->get_n_h().n + 1 : my_n.n + 1;
}

bool
proposer::run(int instance, std::vector<std::string> c_nodes, std::string c_v)
{
  std::vector<std::string> accepts;
  std::vector<std::string> nodes;
  std::vector<std::string> nodes1;
  std::string v;
  bool r = false;

  pthread_mutex_lock(&pxs_mutex);
  printf("start: initiate paxos for %s w. i=%d v=%s stable=%d\n", print_members(c_nodes).c_str(), instance, c_v.c_str(), stable);
  if (!stable) {  // already running proposer?
    printf("proposer::run: already running\n");
    pthread_mutex_unlock(&pxs_mutex);
    return false;
  }
  stable = false;
  setn();
  my_n.m = me;
  nodes = c_nodes;
  v = c_v;
  pthread_mutex_unlock(&pxs_mutex);
  if (prepare(instance, accepts, nodes, v)) {
    printf("c_nodes: %s\n", print_members(c_nodes).c_str());
    printf("accepts: %s\n", print_members(accepts).c_str());
    if (majority(c_nodes, accepts)) {
      printf("paxos::manager: received a majority of prepare responses\n");

      if (v.size() == 0) {
        v = c_v;
      }

      breakpoint1();

      nodes1 = accepts;
      accepts.clear();
      accept(instance, accepts, nodes1, v);

      if (majority(c_nodes, accepts)) {
	      printf("paxos::manager: received a majority of accept responses\n");

        breakpoint2();

        decide(instance, accepts, v);
        r = true;
      } 
      else {
	      printf("paxos::manager: no majority of accept responses\n");
      }
    } else {
      printf("paxos::manager: no majority of prepare responses\n");
    }
  } else {
    printf("paxos::manager: prepare is rejected %d\n", stable);
  }
  pthread_mutex_lock(&pxs_mutex);
  stable = true;
  pthread_mutex_unlock(&pxs_mutex);
  return r;
}

bool
proposer::prepare(unsigned instance, std::vector<std::string> &accepts, 
         std::vector<std::string> nodes,
         std::string &v)
{
  // iterate through all nodes and send a prepare request
  printf("proposer::prepare: instance=%d n=%d.%s v=%s\n", 
         instance, my_n.n, my_n.m.c_str(), v.c_str());
  paxos_protocol::preparearg a;
  paxos_protocol::prepareres r;
  a.instance = instance;
  a.n = my_n;
  unsigned highest_n = 0;
  bool result = true;
  for (unsigned i = 0; i < nodes.size(); i++) {
    std::string clt = nodes[i];
    handle h(clt); // Create a handle for this node (manages connection)
    rpcc* cl = h.get_rpcc();
    if (!cl) {
        printf("proposer::prepare: failed to get rpcc for %s\n", clt.c_str());
        continue;
    }
    bool ret = cl->call(paxos_protocol::preparereq, clt, a, r, rpcc::to(1000));
    if (ret != paxos_protocol::OK) {
      printf("proposer::prepare: call to %s failed\n", nodes[i].c_str());
      continue;
    }
    if(r.oldinstance == 1) {
      printf("proposer::prepare: call to %s returned old instance %d\n", 
             nodes[i].c_str(), r.oldinstance);
        result = false;
        acc->commit(instance, r.v_a);
        break;
    }
    if(r.accept == 1) {
      printf("proposer::prepare: call to %s accepted\n", nodes[i].c_str());
      accepts.push_back(nodes[i]);
      if (r.n_a.n > highest_n & r.v_a.size() > 0) {
        highest_n = r.n_a.n;
        v = r.v_a;
      }
    }
    printf("proposer::prepare: got response from %s: oldinstance=%d accept=%d n_a=%d.%s v_a=%s\n",
           nodes[i].c_str(), r.oldinstance, r.accept, r.n_a.n, r.n_a.m.c_str(), r.v_a.c_str());
  }
  return result;
}


void
proposer::accept(unsigned instance, std::vector<std::string> &accepts,
        std::vector<std::string> nodes, std::string v)
{
  // iterate through all nodes and send an accept request
  printf("proposer::accept: instance=%d n=%d.%s v=%s\n", 
         instance, my_n.n, my_n.m.c_str(), v.c_str());
  paxos_protocol::acceptarg a;
  int r;
  a.instance = instance;
  a.n = my_n;
  a.v = v;
  for (unsigned i = 0; i < nodes.size(); i++) {
    std::string clt = nodes[i];
    handle h(clt); // Create a handle for this node (manages connection)
    rpcc* cl = h.get_rpcc();
    if (!cl) {
        printf("proposer::accept: failed to get rpcc for %s\n", clt.c_str());
        continue;
    }
    bool ret = cl->call(paxos_protocol::acceptreq, clt, a, r, rpcc::to(1000));
    if (ret != paxos_protocol::OK) {
      printf("proposer::accept: call to %s failed\n", nodes[i].c_str());
      continue;
    }
    if(r==1){
      accepts.push_back(nodes[i]);
    }
    printf("proposer::accept: got response from %s\n", nodes[i].c_str());
  }
}

void
proposer::decide(unsigned instance, std::vector<std::string> accepts, 
	      std::string v)
{
  // iterate through all nodes and send a decide request
  printf("proposer::decide: instance=%d v=%s\n", instance, v.c_str());
  paxos_protocol::decidearg a;
  int r;
  a.instance = instance;
  a.v = v;
  for (unsigned i = 0; i < accepts.size(); i++) {
    std::string clt = accepts[i];
    handle h(clt); // Create a handle for this node (manages connection)
    rpcc* cl = h.get_rpcc();
    if (!cl) {
        printf("proposer::decide: failed to get rpcc for %s\n", clt.c_str());
        continue;
    }

    bool ret = cl->call(paxos_protocol::decidereq, clt, a, r, rpcc::to(1000));
    if (ret != paxos_protocol::OK) {
      printf("proposer::decide: call to %s failed\n", accepts[i].c_str());
      continue;
    }
    printf("proposer::decide: got response from %s\n", accepts[i].c_str());
  }
}

acceptor::acceptor(class paxos_change *_cfg, bool _first, std::string _me, 
	     std::string _value)
  : cfg(_cfg), me (_me), instance_h(0)
{
  assert (pthread_mutex_init(&pxs_mutex, NULL) == 0);

  n_h.n = 0;
  n_h.m = me;
  n_a.n = 0;
  n_a.m = me;
  v_a.clear();

  l = new log (this, me);

  if (instance_h == 0 && _first) {
    values[1] = _value;
    l->loginstance(1, _value);
    instance_h = 1;
  }

  pxs = new rpcs(atoi(_me.c_str()));
  pxs->reg(paxos_protocol::preparereq, this, &acceptor::preparereq);
  pxs->reg(paxos_protocol::acceptreq, this, &acceptor::acceptreq);
  pxs->reg(paxos_protocol::decidereq, this, &acceptor::decidereq);
}

paxos_protocol::status
acceptor::preparereq(std::string src, paxos_protocol::preparearg a,
    paxos_protocol::prepareres &r)
{
  // handle a preparereq message from proposer
  printf("acceptor::preparereq: current instance_h=%d, n_h=%d.%s, n_a=%d.%s, v_a=%s\n",
         instance_h, n_h.n, n_h.m.c_str(), n_a.n, n_a.m.c_str(), v_a.c_str());
  printf("acceptor::preparereq: received instance=%d n=%d.%s v=%s\n",
         a.instance, a.n.n, a.n.m.c_str(), a.v.c_str());
  r.oldinstance = 0; // not an old instance
  r.accept = 0; // not accepted
  r.n_a = n_a;
  r.v_a = v_a;
  if (a.instance <= instance_h) {
    printf("acceptor::preparereq: instance %d already decided, returning n_a=%d.%s v_a=%s\n",
           a.instance, n_a.n, n_a.m.c_str(), values[a.instance].c_str());
    r.oldinstance = 1;
    r.v_a = value(a.instance);
  }
  else if(a.n > n_h) {
    // we accept this proposal
    printf("acceptor::preparereq: instance %d accepted n=%d.%s v=%s\n",
           a.instance, a.n.n, a.n.m.c_str(), a.v.c_str());
    n_h = a.n;
    l->loghigh(n_h);
    r.accept = 1; // accepted
  } 
  else {
    // we reject this proposal
    printf("acceptor::preparereq: instance %d rejected n=%d.%s v=%s\n",
           a.instance, a.n.n, a.n.m.c_str(), a.v.c_str());
  }
  return paxos_protocol::OK;

}

paxos_protocol::status
acceptor::acceptreq(std::string src, paxos_protocol::acceptarg a, int &r)
{
  r = 0;
  if(a.instance <= instance_h) {
    // already accepted
    printf("acceptor::acceptreq: instance %d already decided, returning n_a=%d.%s v_a=%s\n",
           a.instance, n_a.n, n_a.m.c_str(), v_a.c_str());
  } else if (a.n >= n_h) {
    // we accept this proposal
    printf("acceptor::acceptreq: instance %d accepted n=%d.%s v=%s\n",
           a.instance, a.n.n, a.n.m.c_str(), a.v.c_str());
    n_h = a.n;
    n_a = a.n;
    v_a = a.v;
    r = 1; // accepted
    l->logprop(n_a, v_a);
  } else {
    // we reject this proposal
    printf("acceptor::acceptreq: instance %d rejected n=%d.%s v=%s\n",
           a.instance, a.n.n, a.n.m.c_str(), a.v.c_str());
  }
  return paxos_protocol::OK;
}

paxos_protocol::status
acceptor::decidereq(std::string src, paxos_protocol::decidearg a, int &r)
{
  // handle an decide message from proposer
  if( a.instance <= instance_h) {
    // already decided
    printf("acceptor::decidereq: instance %d already decided, returning n_a=%d.%s v_a=%s\n",
           a.instance, n_a.n, n_a.m.c_str(), v_a.c_str());
    r = 0; // not accepted
  }
  else{
    commit_wo(a.instance, a.v);
    r = 1;
  }
  return paxos_protocol::OK;
}

void
acceptor::commit_wo(unsigned instance, std::string value)
{
  //assume pxs_mutex is held
  printf("acceptor::commit: instance=%d has v= %s\n", instance, value.c_str());
  if (instance > instance_h) {
    printf("commit: highestaccepteinstance = %d\n", instance);
    values[instance] = value;
    l->loginstance(instance, value);
    instance_h = instance;
    n_h.n = 0;
    n_h.m = me;
    n_a.n = 0;
    n_a.m = me;
    v_a.clear();
    if (cfg) {
      pthread_mutex_unlock(&pxs_mutex);
      cfg->paxos_commit(instance, value);
      pthread_mutex_lock(&pxs_mutex);
    }
  }
}

void
acceptor::commit(unsigned instance, std::string value)
{
  pthread_mutex_lock(&pxs_mutex);
  commit_wo(instance, value);
  pthread_mutex_unlock(&pxs_mutex);
}

std::string
acceptor::dump()
{
  return l->dump();
}

void
acceptor::restore(std::string s)
{
  l->restore(s);
  l->logread();
}



// For testing purposes

// Call this from your code between phases prepare and accept of proposer
void
proposer::breakpoint1()
{
  if (break1) {
    printf("Dying at breakpoint 1!\n");
    exit(1);
  }
}

// Call this from your code between phases accept and decide of proposer
void
proposer::breakpoint2()
{
  if (break2) {
    printf("Dying at breakpoint 2!\n");
    exit(1);
  }
}

void
proposer::breakpoint(int b)
{
  if (b == 3) {
    printf("Proposer: breakpoint 1\n");
    break1 = true;
  } else if (b == 4) {
    printf("Proposer: breakpoint 2\n");
    break2 = true;
  }
}
