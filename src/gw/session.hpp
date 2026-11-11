// One authenticated client WebSocket. Two coroutines: a reader that feeds the
// Hub, and a writer that drains this session's outbound queues.
//
// Two queues on purpose:
//   events_  order/exec/ack messages. Never dropped. If a client lets this grow
//            past max_queue it is disconnected (we can't drop a fill).
//   md_      market data, conflated per symbol: a slow client gets the latest
//            book, not a backlog. Memory per client is bounded by #symbols.
#pragma once
#include <deque>
#include <set>
#include <unordered_map>

#include "../common/net.hpp"

namespace tg {

class Hub;

class WsSession : public std::enable_shared_from_this<WsSession> {
 public:
  using Stream = websocket::stream<beast::tcp_stream>;
  WsSession(Stream ws, Hub& hub, int acct, std::string key_hash);

  awaitable<void> run();
  void send(std::shared_ptr<const std::string> msg);
  void send(const json::object& msg) { send(std::make_shared<const std::string>(json::serialize(msg))); }
  void send_md(const std::string& sym, std::shared_ptr<const std::string> msg);
  void kick(std::string reason);  // close with 1008 (policy violation)

  int account() const { return acct_; }
  const std::string& key_hash() const { return key_hash_; }
  std::set<std::string>& subs() { return subs_; }
  size_t queued() const { return events_.size() + md_.size(); }

 private:
  static awaitable<void> writer(std::shared_ptr<WsSession> self);
  void hard_close();

  Stream ws_;
  Hub& hub_;
  int acct_;
  std::string key_hash_;
  std::set<std::string> subs_;
  std::deque<std::shared_ptr<const std::string>> events_;
  std::unordered_map<std::string, std::shared_ptr<const std::string>> md_;
  std::deque<std::string> md_order_;
  net::steady_timer signal_;
  bool closed_ = false, kick_ = false;
  std::string kick_reason_;
};

// Accepts TCP connections and serves HTTP + the /ws upgrade.
awaitable<void> listen(Hub& hub, tcp::endpoint ep);

}  // namespace tg
