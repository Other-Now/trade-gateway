// tg-bench: drives one or more running gateways and measures them.
//
//   rtt        order -> ack / NEW / FILLED round trips, plus gateway-only risk rejects
//   config     UPDATE committed -> "config" event seen on every gateway's sockets
//   burst      1000 config commits back to back: NOTIFYs received vs reloads run
//   revoke     UPDATE api_keys SET revoked -> that key's live socket closed
//   resync     kill the gateway's LISTEN connection, commit a change while it is
//              down, see whether the gateway ever learns about it
//   isolation  5 accounts x 2 sockets (one per gateway), every event checked
//   fanout     market data latency for N readers with/without a stalled reader
//   live       count real Binance testnet book ticks through the gateway
//
// Each WebSocket gets its own io_context thread that timestamps frames on
// arrival, so a blocking libpq call on the main thread can't skew a latency.
#include <libpq-fe.h>

#include <atomic>
#include <condition_variable>
#include <cstdio>
#include <map>
#include <mutex>
#include <optional>
#include <random>
#include <set>
#include <thread>

#include "../common/net.hpp"
#include "../common/util.hpp"

using namespace tg;

struct Msg {
  int64_t t_us;
  json::object o;
};

class Conn {
 public:
  Conn(std::string host, std::string port, std::string key, bool read = true, int rcvbuf = 0)
      : ws_(ioc_), work_(net::make_work_guard(ioc_)) {
    tcp::resolver r(ioc_);
    auto eps = r.resolve(host, port);
    auto& sock = beast::get_lowest_layer(ws_).socket();
    sock.open(tcp::v4());
    if (rcvbuf) sock.set_option(net::socket_base::receive_buffer_size(rcvbuf));
    sock.connect(*eps.begin());
    sock.set_option(tcp::no_delay(true));
    ws_.set_option(websocket::stream_base::decorator(
        [key](websocket::request_type& req) { req.set("X-API-Key", key); }));
    ws_.handshake(host, "/ws");
    ws_.text(true);
    out_ = std::make_shared<Outbox>(ioc_.get_executor());
    auto ws = std::shared_ptr<WsT>(&ws_, [](WsT*) {});
    net::co_spawn(ioc_, Outbox::run(out_, ws), net::detached);
    if (read) net::co_spawn(ioc_, reader(), net::detached);
    th_ = std::thread([this] { ioc_.run(); });
  }
  ~Conn() {
    ioc_.stop();
    th_.join();
    boost::system::error_code ec;
    beast::get_lowest_layer(ws_).socket().close(ec);
  }
  void send(json::object o) {
    net::post(ioc_, [this, s = json::serialize(o)] { out_->push(s); });
  }
  // Pop the next message matching pred within timeout; non-matching ones are kept.
  template <class P>
  std::optional<Msg> wait(P pred, int timeout_ms) {
    std::unique_lock lk(mu_);
    auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(timeout_ms);
    for (;;) {
      for (auto it = q_.begin(); it != q_.end(); ++it)
        if (pred(it->o)) {
          Msg m = std::move(*it);
          q_.erase(it);
          return m;
        }
      if (cv_.wait_until(lk, deadline) == std::cv_status::timeout) return std::nullopt;
    }
  }
  std::vector<Msg> drain() {
    std::lock_guard lk(mu_);
    std::vector<Msg> v(std::make_move_iterator(q_.begin()), std::make_move_iterator(q_.end()));
    q_.clear();
    return v;
  }
  bool closed() const { return closed_; }
  std::string close_reason() {
    std::lock_guard lk(mu_);
    return reason_;
  }
  int64_t closed_at() const { return closed_at_; }

 private:
  using WsT = websocket::stream<beast::tcp_stream>;
  awaitable<void> reader() {
    beast::flat_buffer buf;
    try {
      for (;;) {
        co_await ws_.async_read(buf, use_awaitable);
        int64_t t = now_us();
        auto d = buf.data();
        boost::system::error_code ec;
        auto v = json::parse(std::string_view(static_cast<const char*>(d.data()), d.size()), ec);
        buf.consume(buf.size());
        if (ec || !v.is_object()) continue;
        std::lock_guard lk(mu_);
        q_.push_back(Msg{t, std::move(v.as_object())});
        cv_.notify_all();
      }
    } catch (const std::exception& e) {
      closed_at_ = now_us();
      std::lock_guard lk(mu_);
      reason_ = std::string(e.what()) + " / close: " + std::string(ws_.reason().reason.c_str());
      closed_ = true;
      cv_.notify_all();
    }
  }
  net::io_context ioc_;
  WsT ws_;
  net::executor_work_guard<net::io_context::executor_type> work_;
  std::shared_ptr<Outbox> out_;
  std::thread th_;
  std::mutex mu_;
  std::condition_variable cv_;
  std::deque<Msg> q_;
  std::atomic<bool> closed_{false};
  std::atomic<int64_t> closed_at_{0};
  std::string reason_;
};

// ---------------------------------------------------------------- helpers

struct Pg {
  PGconn* c;
  explicit Pg(const std::string& ci) : c(PQconnectdb(ci.c_str())) {
    if (PQstatus(c) != CONNECTION_OK) throw std::runtime_error(PQerrorMessage(c));
  }
  ~Pg() { PQfinish(c); }
  std::string exec(const std::string& sql) {  // returns first cell, if any
    PGresult* r = PQexec(c, sql.c_str());
    auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK) {
      std::string e = PQresultErrorMessage(r);
      PQclear(r);
      throw std::runtime_error(sql + ": " + e);
    }
    std::string v = PQntuples(r) > 0 ? PQgetvalue(r, 0, 0) : "";
    PQclear(r);
    return v;
  }
};

static std::pair<std::string, std::string> hp(const std::string& s) {
  auto c = s.find(':');
  return {s.substr(0, c), s.substr(c + 1)};
}

static std::string http_get(const std::string& hostport, const std::string& target) {
  auto [h, p] = hp(hostport);
  net::io_context ioc;
  beast::tcp_stream s(ioc);
  s.connect(*tcp::resolver(ioc).resolve(h, p).begin());
  http::request<http::empty_body> req{http::verb::get, target, 11};
  req.set(http::field::host, h);
  http::write(s, req);
  beast::flat_buffer b;
  http::response<http::string_body> res;
  http::read(s, b, res);
  return res.body();
}

static std::map<std::string, int64_t> metrics(const std::string& gw) {
  std::map<std::string, int64_t> m;
  auto body = http_get(gw, "/metrics");
  size_t pos = 0;
  while (pos < body.size()) {
    auto nl = body.find('\n', pos);
    auto line = body.substr(pos, nl - pos);
    auto sp = line.find(' ');
    if (sp != std::string::npos) m[line.substr(0, sp)] = std::stoll(line.substr(sp + 1));
    pos = nl == std::string::npos ? body.size() : nl + 1;
  }
  return m;
}

static int64_t health_version(const std::string& gw) {
  auto v = json::parse(http_get(gw, "/health"));
  return v.as_object().at("config_version").as_int64();
}

static void report(const char* name, const Samples& s, const char* unit = "us") {
  std::printf("  %-34s n=%-6zu p50=%-8lld p90=%-8lld p99=%-8lld max=%-8lld %s\n", name, s.v.size(),
              (long long)s.pct(50), (long long)s.pct(90), (long long)s.pct(99),
              (long long)s.max(), unit);
}

static std::vector<std::string> split(const std::string& s, char d = ',') {
  std::vector<std::string> v;
  size_t p = 0;
  while (p <= s.size()) {
    auto n = s.find(d, p);
    if (n == std::string::npos) n = s.size();
    if (n > p) v.push_back(s.substr(p, n - p));
    p = n + 1;
  }
  return v;
}

static auto type_is(const char* t) {
  return [t](const json::object& o) { return jstr(o, "type") == t; };
}

static int failures = 0;
static void expect(bool ok, const std::string& what) {
  std::printf("  [%s] %s\n", ok ? "PASS" : "FAIL", what.c_str());
  if (!ok) ++failures;
}

// ---------------------------------------------------------------- benches

static void bench_rtt(std::map<std::string, std::string>& a) {
  auto [h, p] = hp(a["gw"]);
  int n = std::stoi(a["n"]);
  std::string sym = a.count("symbol") ? a["symbol"] : "BTCUSDT";
  std::string qty = a.count("qty") ? a["qty"] : "0.001";
  Conn c(h, p, a["key"]);
  c.send(json::object{{"op", "subscribe"}, {"symbols", json::array{json::value(sym)}}});
  if (!c.wait(type_is("md"), 10000)) throw std::runtime_error("no market data");

  Samples ack, neu, fill, rej;
  int rejected_upstream = 0;
  for (int i = 0; i < n; i++) {
    int64_t t0 = now_us();
    c.send(json::object{{"op", "order"}, {"req_id", i}, {"symbol", sym}, {"side", i % 2 ? "SELL" : "BUY"},
                        {"type", "MARKET"}, {"qty", qty}});
    auto r = c.wait([&](auto& o) { return jint(o, "req_id", -1) == i; }, 5000);
    if (!r) throw std::runtime_error("no ack");
    if (jstr(r->o, "type") != "ack") throw std::runtime_error("rejected: " + jstr(r->o, "reason"));
    ack.add(r->t_us - t0);
    auto cid = jstr(r->o, "cid");
    auto is = [&](const char* x) {
      return [&, x](auto& o) { return jstr(o, "type") == "exec" && jstr(o, "cid") == cid && jstr(o, "exec_type") == x; };
    };
    auto nw = c.wait([&](auto& o) { return jstr(o, "type") == "exec" && jstr(o, "cid") == cid && jstr(o, "exec_type") != "TRADE"; }, 10000);
    if (!nw) throw std::runtime_error("no NEW for " + cid);
    if (jstr(nw->o, "exec_type") == "REJECTED") {
      ++rejected_upstream;
      continue;
    }
    neu.add(nw->t_us - t0);
    auto f = c.wait(is("TRADE"), 10000);
    if (!f) throw std::runtime_error("no fill for " + cid);
    fill.add(f->t_us - t0);
  }
  // Gateway-only round trip: an order the risk check rejects never leaves the box.
  for (int i = 0; i < n; i++) {
    int64_t t0 = now_us();
    c.send(json::object{{"op", "order"}, {"req_id", 1000000 + i}, {"symbol", sym}, {"side", "BUY"},
                        {"type", "MARKET"}, {"qty", "999999"}});
    auto r = c.wait([&](auto& o) { return jint(o, "req_id", -1) == 1000000 + i; }, 5000);
    if (!r || jstr(r->o, "type") != "reject") throw std::runtime_error("expected risk reject");
    rej.add(r->t_us - t0);
  }
  std::printf("rtt: %d sequential MARKET orders via %s (%s)\n", n, a["gw"].c_str(), sym.c_str());
  report("risk reject (gateway only)", rej);
  report("ack (risk passed, sent upstream)", ack);
  report("exec NEW from exchange", neu);
  report("exec TRADE (fill)", fill);
  if (rejected_upstream) std::printf("  upstream rejects: %d\n", rejected_upstream);
}

static void bench_config(std::map<std::string, std::string>& a) {
  auto gws = split(a["gws"]);
  int n = std::stoi(a["n"]);
  Pg pg(a["pg"]);
  std::vector<std::unique_ptr<Conn>> cs;
  for (auto& g : gws) {
    auto [h, p] = hp(g);
    cs.push_back(std::make_unique<Conn>(h, p, a["key"]));
  }
  std::vector<Samples> lat(gws.size());
  for (int i = 0; i < n; i++) {
    // Alternate the value so every UPDATE is a real change.
    pg.exec(std::string("UPDATE account_limits SET max_qty = ") + (i % 2 ? "100" : "100.5") +
            " WHERE account_id = 1 AND symbol = 'BTCUSDT'");
    int64_t t1 = now_us();  // PQexec returned => committed
    int64_t v = std::stoll(pg.exec("SELECT version FROM config_version"));
    for (size_t g = 0; g < cs.size(); g++) {
      auto m = cs[g]->wait([&](auto& o) { return jstr(o, "type") == "config" && jint(o, "version") >= v; }, 5000);
      if (!m) throw std::runtime_error("gateway " + gws[g] + " never saw v" + std::to_string(v));
      lat[g].add(std::max<int64_t>(0, m->t_us - t1));
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  std::printf("config: %d committed UPDATEs, time from COMMIT returning to client seeing the new version\n", n);
  for (size_t g = 0; g < gws.size(); g++) {
    report(("gateway " + gws[g]).c_str(), lat[g]);
    auto m = metrics(gws[g]);
    std::printf("    in-process trigger->applied p50=%lld p99=%lld us; snapshot query p50=%lld p99=%lld us\n",
                (long long)m["config_apply_us_p50"], (long long)m["config_apply_us_p99"],
                (long long)m["config_reload_query_us_p50"], (long long)m["config_reload_query_us_p99"]);
  }
}

static void bench_burst(std::map<std::string, std::string>& a) {
  auto gws = split(a["gws"]);
  int n = std::stoi(a["n"]);
  int writers = a.count("writers") ? std::stoi(a["writers"]) : 1;
  Pg pg(a["pg"]);
  std::vector<std::map<std::string, int64_t>> before;
  for (auto& g : gws) before.push_back(metrics(g));
  auto t0 = std::chrono::steady_clock::now();
  // `writers` connections committing concurrently, each its own account row
  // (accounts 1..6) so they don't queue on one row lock.
  std::vector<std::thread> ths;
  if (a.count("txn")) {  // n statements in ONE transaction: n NOTIFYs delivered together at COMMIT
    std::string sql = "BEGIN;";
    for (int i = 0; i < n; i++)
      sql += "UPDATE account_limits SET max_notional = " + std::to_string(1000000 + i) +
             " WHERE account_id = 1 AND symbol = 'BTCUSDT';";
    pg.exec(sql + "COMMIT;");
    writers = 0;
  }
  for (int w = 0; w < writers; w++)
    ths.emplace_back([&, w] {
      Pg mine(a["pg"]);
      for (int i = w; i < n; i += writers)
        mine.exec(std::string("UPDATE account_limits SET max_notional = ") + std::to_string(1000000 + i) +
                  " WHERE account_id = " + std::to_string(1 + w % 6) + " AND symbol = 'BTCUSDT'");
    });
  for (auto& t : ths) t.join();
  double commit_ms = std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - t0).count();
  int64_t v = std::stoll(pg.exec("SELECT version FROM config_version"));
  if (a.count("txn"))
    std::printf("burst: %d UPDATE statements in ONE transaction, %.0f ms\n", n, commit_ms);
  else
    std::printf("burst: %d single-row commits from %d concurrent writer(s) in %.0f ms (%.0f commits/s)\n", n,
                writers, commit_ms, n / commit_ms * 1000);
  for (size_t g = 0; g < gws.size(); g++) {
    int64_t got = 0;
    for (int k = 0; k < 500 && (got = health_version(gws[g])) < v; k++)
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto after = metrics(gws[g]);
    std::printf("  gateway %s: reached v%lld (target v%lld); NOTIFYs received %lld, snapshot reloads %lld\n",
                gws[g].c_str(), (long long)got, (long long)v,
                (long long)(after["pg_notifies"] - before[g]["pg_notifies"]),
                (long long)(after["pg_reloads"] - before[g]["pg_reloads"]));
    expect(got == v, "gateway " + gws[g] + " converged on final version");
  }
}

static void bench_revoke(std::map<std::string, std::string>& a) {
  auto [h, p] = hp(a["gw"]);
  int n = std::stoi(a["n"]);
  Pg pg(a["pg"]);
  Samples lat;
  int still_open = 0, http_after = 0;
  for (int i = 0; i < n; i++) {
    std::string key = "revoke-test-" + std::to_string(i) + "-" + std::to_string(now_us());
    std::string hash = sha256_hex(key);
    pg.exec("INSERT INTO api_keys(key_hash, account_id, label) VALUES ('" + hash + "', 2, 'bench')");
    int64_t v = std::stoll(pg.exec("SELECT version FROM config_version"));
    for (int k = 0; k < 200 && health_version(a["gw"]) < v; k++)
      std::this_thread::sleep_for(std::chrono::milliseconds(2));
    Conn c(h, p, key);
    pg.exec("UPDATE api_keys SET revoked = true WHERE key_hash = '" + hash + "'");
    int64_t t1 = now_us();
    for (int k = 0; k < 3000 && !c.closed(); k++) std::this_thread::sleep_for(std::chrono::microseconds(200));
    if (!c.closed()) {
      ++still_open;
      continue;
    }
    lat.add(c.closed_at() - t1);
    if (i == 0) std::printf("  close reason seen by client: %s\n", c.close_reason().c_str());
    // And the key is dead for new requests too.
    try {
      Conn again(h, p, key);
      ++http_after;
    } catch (const std::exception&) {
    }
  }
  pg.exec("DELETE FROM api_keys WHERE label = 'bench'");
  std::printf("revoke: %d keys revoked while their socket was open\n", n);
  report("COMMIT -> socket closed by gateway", lat);
  expect(still_open == 0, "every revoked key's socket closed (" + std::to_string(still_open) + " left open)");
  expect(http_after == 0, "revoked key refused on reconnect (" + std::to_string(http_after) + " accepted)");
}

static void bench_resync(std::map<std::string, std::string>& a) {
  auto [h, p] = hp(a["gw"]);
  Pg pg(a["pg"]);
  std::string app = "tgw-" + a["id"];
  std::string reset = "UPDATE account_limits SET max_qty = 100 WHERE account_id = 1 AND symbol = 'BTCUSDT'";
  pg.exec(reset);
  std::this_thread::sleep_for(std::chrono::milliseconds(300));
  Conn c(h, p, a["key"]);
  c.send(json::object{{"op", "subscribe"}, {"symbols", json::array{json::value("BTCUSDT")}}});
  c.wait(type_is("md"), 10000);

  // Kill the gateway's LISTEN connection, then tighten the limit while it is
  // gone. Postgres does not queue NOTIFY for a listener that isn't there.
  pg.exec("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = '" + app + "'");
  pg.exec("UPDATE account_limits SET max_qty = 0.0001 WHERE account_id = 1 AND symbol = 'BTCUSDT'");
  int64_t t1 = now_us();
  int64_t v = std::stoll(pg.exec("SELECT version FROM config_version"));
  int64_t seen_at = 0;
  for (int k = 0; k < 300; k++) {  // 3 s
    if (health_version(a["gw"]) >= v) {
      seen_at = now_us();
      break;
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  // The question that matters: does an order over the NEW limit still get through?
  c.send(json::object{{"op", "order"}, {"req_id", 1}, {"symbol", "BTCUSDT"}, {"side", "BUY"},
                      {"type", "MARKET"}, {"qty", "0.001"}});
  auto r = c.wait([](auto& o) { return jint(o, "req_id", -1) == 1; }, 5000);
  bool accepted = r && jstr(r->o, "type") == "ack";
  std::printf("resync (%s): listener killed, max_qty tightened 100 -> 0.0001 while disconnected\n", app.c_str());
  if (seen_at) std::printf("  gateway reached v%lld %.1f ms after the commit\n", (long long)v, (seen_at - t1) / 1000.0);
  else std::printf("  gateway still on v%lld after 3 s (target v%lld): STALE\n", (long long)health_version(a["gw"]), (long long)v);
  std::printf("  order for 0.001 BTC: %s%s\n", accepted ? "ACCEPTED (limit violated)" : "rejected: ",
              accepted ? "" : jstr(r->o, "reason").c_str());
  pg.exec(reset);
  if (a.count("expect-stale")) expect(!seen_at && accepted, "control gateway stayed stale and let the order through");
  else expect(seen_at && !accepted, "gateway resynced and rejected the order");
}

static void bench_isolation(std::map<std::string, std::string>& a) {
  auto gws = split(a["gws"]);
  int orders = std::stoi(a["n"]);
  auto keys = split(a["keys"]);  // one key per account, accounts 1..k in order
  struct Acct {
    int id;
    std::vector<std::unique_ptr<Conn>> conns;
    std::set<std::string> cids;
  };
  std::vector<Acct> accts;
  for (size_t i = 0; i < keys.size(); i++) {
    Acct ac{int(i + 1), {}, {}};
    for (auto& g : gws) {
      auto [h, p] = hp(g);
      ac.conns.push_back(std::make_unique<Conn>(h, p, keys[i]));
    }
    accts.push_back(std::move(ac));
  }
  // Each account places orders through its first gateway, all accounts in parallel.
  std::vector<std::thread> ths;
  std::atomic<int> errs{0};
  for (auto& ac : accts)
    ths.emplace_back([&ac, orders, &errs] {
      for (int i = 0; i < orders; i++) {
        ac.conns[0]->send(json::object{{"op", "order"}, {"req_id", i}, {"symbol", "BTCUSDT"},
                                       {"side", i % 2 ? "SELL" : "BUY"}, {"type", "MARKET"}, {"qty", "0.001"}});
        auto r = ac.conns[0]->wait([i](auto& o) { return jint(o, "req_id", -1) == i; }, 5000);
        if (!r || jstr(r->o, "type") != "ack") {
          ++errs;
          continue;
        }
        ac.cids.insert(jstr(r->o, "cid"));
      }
    });
  for (auto& t : ths) t.join();
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));

  // Probes: another account's order id, and a symbol this account may not see.
  auto& a2 = accts[1];
  a2.conns[0]->send(json::object{{"op", "cancel"}, {"req_id", "x"}, {"cid", *accts[0].cids.begin()}});
  auto cx = a2.conns[0]->wait([](auto& o) { return jstr(o, "req_id") == "x"; }, 3000);
  a2.conns[0]->send(json::object{{"op", "subscribe"}, {"req_id", "s"}, {"symbols", json::array{json::value("ETHUSDT")}}});
  auto sx = a2.conns[0]->wait([](auto& o) { return jstr(o, "req_id") == "s"; }, 3000);

  int64_t events = 0, leaks = 0, missing = 0;
  for (auto& ac : accts)
    for (size_t g = 0; g < ac.conns.size(); g++) {
      std::map<std::string, std::set<std::string>> seen;  // cid -> exec types
      for (auto& m : ac.conns[g]->drain()) {
        if (jstr(m.o, "type") != "exec") continue;
        ++events;
        auto cid = jstr(m.o, "cid");
        if (account_from_cid(cid).value_or(-1) != ac.id) ++leaks;
        seen[cid].insert(jstr(m.o, "exec_type"));
      }
      for (auto& cid : ac.cids)
        if (!seen[cid].count("NEW") || !seen[cid].count("TRADE")) ++missing;
    }
  std::printf("isolation: %zu accounts x %zu sockets (one per gateway), %d orders each\n", accts.size(),
              gws.size(), orders);
  std::printf("  exec events delivered %lld, ack errors %d\n", (long long)events, errs.load());
  expect(leaks == 0, "events for another account: " + std::to_string(leaks));
  expect(missing == 0, "own orders missing NEW/TRADE on some socket: " + std::to_string(missing));
  expect(cx && jstr(cx->o, "reason") == "unknown order",
         "cancel of another account's order -> " + (cx ? jstr(cx->o, "reason") : std::string("timeout")));
  expect(sx && jstr(sx->o, "reason") == "not permitted",
         "subscribe to unpermitted symbol -> " + (sx ? jstr(sx->o, "reason") : std::string("timeout")));
}

static void bench_fanout(std::map<std::string, std::string>& a) {
  auto [h, p] = hp(a["gw"]);
  int readers = std::stoi(a["readers"]);
  int stalled = std::stoi(a["stalled"]);
  int secs = std::stoi(a["secs"]);
  auto sub = json::object{{"op", "subscribe"}, {"symbols", json::array{json::value("BTCUSDT")}}};
  auto m0 = metrics(a["gw"]);
  std::vector<std::unique_ptr<Conn>> slow, fast;
  for (int i = 0; i < stalled; i++) {
    slow.push_back(std::make_unique<Conn>(h, p, a["key"], /*read=*/false, /*rcvbuf=*/4096));
    slow.back()->send(sub);
  }
  for (int i = 0; i < readers; i++) {
    fast.push_back(std::make_unique<Conn>(h, p, a["key"]));
    fast.back()->send(sub);
  }
  Samples lat;
  int64_t msgs = 0, gaps = 0;
  lat.cap = 10000000;
  std::vector<int64_t> last_u(readers, 0);
  long rss_peak = 0;
  auto end = std::chrono::steady_clock::now() + std::chrono::seconds(secs);
  bool warm = false;
  while (std::chrono::steady_clock::now() < end) {
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    rss_peak = std::max<long>(rss_peak, metrics(a["gw"])["rss_kb"]);
    for (int i = 0; i < readers; i++)
      for (auto& m : fast[i]->drain()) {
        if (jstr(m.o, "type") != "md" || m.o.contains("snapshot")) continue;
        int64_t u = jint(m.o, "u");
        if (warm) {
          ++msgs;
          lat.add(m.t_us - jint(m.o, "rx_us"));
          if (last_u[i] && u != last_u[i] + 1) ++gaps;
        }
        last_u[i] = u;
      }
    warm = true;
  }
  auto m1 = metrics(a["gw"]);
  std::printf("fanout: %d readers + %d stalled sockets, %d s, mode=%s\n", readers, stalled, secs,
              a.count("label") ? a["label"].c_str() : "?");
  report("gateway rx -> reader rx", lat);
  std::printf("  ticks delivered to readers %lld (%lld/s), sequence gaps seen by readers %lld\n",
              (long long)msgs, (long long)(msgs / std::max(1, secs)), (long long)gaps);
  std::printf("  gateway md_in %lld, md_conflated %lld, slow_disconnects %lld, rss start %lld kB, peak %ld kB\n",
              (long long)(m1["md_in"] - m0["md_in"]), (long long)(m1["md_conflated"] - m0["md_conflated"]),
              (long long)(m1["slow_disconnects"] - m0["slow_disconnects"]), (long long)m0["rss_kb"], rss_peak);
}

static void bench_live(std::map<std::string, std::string>& a) {
  auto [h, p] = hp(a["gw"]);
  int secs = std::stoi(a["secs"]);
  auto syms = split(a.count("symbols") ? a["symbols"] : "BTCUSDT,ETHUSDT");
  Conn c(h, p, a["key"]);
  json::array arr;
  for (auto& s : syms) arr.push_back(json::value(s));
  c.send(json::object{{"op", "subscribe"}, {"symbols", arr}});
  std::this_thread::sleep_for(std::chrono::seconds(secs));
  std::map<std::string, int64_t> count;
  std::map<std::string, std::string> last;
  Samples lat;
  for (auto& m : c.drain()) {
    if (jstr(m.o, "type") != "md" || m.o.contains("snapshot")) continue;
    auto s = jstr(m.o, "symbol");
    ++count[s];
    lat.add(m.t_us - jint(m.o, "rx_us"));
    last[s] = jstr(m.o, "bid") + " / " + jstr(m.o, "ask");
  }
  auto mt = metrics(a["gw"]);
  std::printf("live: Binance spot testnet bookTicker through the gateway for %d s\n", secs);
  for (auto& s : syms)
    std::printf("  %-8s %6lld updates (%.1f/s)  last bid/ask %s\n", s.c_str(), (long long)count[s],
                double(count[s]) / secs, last[s].c_str());
  report("gateway rx -> client rx", lat);
  std::printf("  upstream md reconnects %lld\n", (long long)mt["upstream_md_reconnects"]);
  expect(!count.empty(), "received live market data");
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::fprintf(stderr, "usage: tg-bench <rtt|config|burst|revoke|resync|isolation|fanout|live> --flags\n");
    return 2;
  }
  std::string cmd = argv[1];
  std::map<std::string, std::string> a{{"n", "200"}, {"key", "alice-key"}, {"secs", "10"},
                                        {"readers", "4"}, {"stalled", "0"}, {"id", "a"},
                                        {"pg", "host=127.0.0.1 port=5544 user=tg dbname=tg"}};
  for (int i = 2; i < argc; i++) {
    std::string k = argv[i];
    if (!k.starts_with("--")) continue;
    if (i + 1 < argc && std::string_view(argv[i + 1]).substr(0, 2) != "--") a[k.substr(2)] = argv[++i];
    else a[k.substr(2)] = "1";
  }
  try {
    if (cmd == "rtt") bench_rtt(a);
    else if (cmd == "config") bench_config(a);
    else if (cmd == "burst") bench_burst(a);
    else if (cmd == "revoke") bench_revoke(a);
    else if (cmd == "resync") bench_resync(a);
    else if (cmd == "isolation") bench_isolation(a);
    else if (cmd == "fanout") bench_fanout(a);
    else if (cmd == "live") bench_live(a);
    else throw std::runtime_error("unknown command " + cmd);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "tg-bench %s: %s\n", cmd.c_str(), e.what());
    return 1;
  }
  std::fflush(stdout);
  return failures ? 1 : 0;
}
