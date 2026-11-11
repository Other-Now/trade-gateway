// The Hub is the gateway's single-threaded core: it owns the current config
// snapshot, the client sessions, the order table and the routing tables.
// Everything runs on one io_context thread, so there are no locks anywhere.
#pragma once
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "../common/net.hpp"
#include "../common/util.hpp"
#include "config.hpp"
#include "pgwatch.hpp"
#include "risk.hpp"

namespace tg {

class WsSession;
class Upstream;

struct HubOptions {
  std::string gw_id = "a";
  bool no_conflate = false;  // control: unbounded per-client queue, no conflation
  size_t max_queue = 10000;  // reliable events queued for one client before we cut it off
};

struct OrderRec {
  std::string cid, symbol, side, type, status;
  int acct = 0;
  int64_t qty = 0, price = 0, cum = 0, exch_id = 0;
};

struct Metrics {
  int64_t orders_in = 0, risk_rejects = 0, rate_rejects = 0, orders_sent = 0,
          upstream_rejects = 0, exec_events = 0, foreign_exec = 0, md_in = 0, md_out = 0,
          md_conflated = 0, kicked = 0, slow_disconnects = 0, auth_failures = 0,
          config_reloads = 0;
  Samples cfg_apply_us;   // trigger clock_timestamp() -> snapshot swapped in this process
  Samples cfg_reload_us;  // duration of the snapshot query itself
};

class Hub {
 public:
  explicit Hub(HubOptions o);
  void attach(Upstream* up, PgWatcher* pg) { up_ = up, pg_ = pg; }
  const HubOptions& opts() const { return opt_; }

  // --- config
  void apply_config(std::shared_ptr<const Config> cfg, const ReloadInfo& info);
  std::shared_ptr<const Config> config() const { return cfg_; }
  std::optional<int> authenticate(const std::string& api_key, std::string* hash_out = nullptr);

  // --- client sessions
  void add(std::shared_ptr<WsSession> s);
  void remove(WsSession* s);
  void on_client_msg(WsSession& s, std::string_view text);

  // --- orders (shared by the HTTP and WebSocket APIs)
  struct Result {
    bool ok = false;
    std::string cid, reason;
  };
  Result place(int acct, const json::object& req);
  Result cancel(int acct, const std::string& cid);
  json::array orders_for(int acct) const;

  // --- upstream callbacks
  void on_md(const json::object& book_ticker, int64_t rx_us);
  void on_exec(const json::object& report);
  void on_upstream_reject(const std::string& cid, int64_t code, const std::string& msg);
  void on_cancel_reject(const std::string& cid, const std::string& msg);

  std::string health() const;
  std::string metrics() const;
  Metrics m;

 private:
  void send_to_account(int acct, const json::object& msg);

  HubOptions opt_;
  std::shared_ptr<const Config> cfg_;
  Upstream* up_ = nullptr;
  PgWatcher* pg_ = nullptr;
  std::unordered_map<WsSession*, std::shared_ptr<WsSession>> sessions_;
  std::unordered_map<int, std::set<WsSession*>> by_acct_;
  std::unordered_map<std::string, std::set<WsSession*>> by_sym_;
  std::unordered_map<std::string, OrderRec> orders_;
  std::unordered_map<int, TokenBucket> buckets_;
  struct Book {
    int64_t bid = 0, ask = 0;
    std::shared_ptr<const std::string> last;
    json::object last_obj;  // for the on-subscribe snapshot
  };
  std::unordered_map<std::string, Book> books_;
  uint64_t seq_;
};

}  // namespace tg
