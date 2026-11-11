// tg-mockex: a local stand-in for Binance spot, speaking the same subset the
// gateway uses, over TLS with a self-signed cert:
//   /ws          bookTicker stream with live SUBSCRIBE/UNSUBSCRIBE
//   /ws-api/v3   order.place, order.cancel, userDataStream.subscribe.signature
//                (HMAC signatures verified) -> executionReport events
// It exists so tests and benchmarks are repeatable and need no API key. It is
// not a matching engine: marketable orders fill in full at the touch, resting
// limits fill when the random-walk book crosses them.
//
//   tg-mockex --port 9443 --cert certs/mock.pem --key certs/mock.key
//             --secret mock-secret [--rate 50] [--reject-every 0]
#include <csignal>
#include <cstdio>
#include <map>
#include <random>
#include <set>

#include "../common/net.hpp"
#include "../common/util.hpp"

using namespace tg;
using Wss = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;

struct Sess {
  std::shared_ptr<Wss> ws;
  std::shared_ptr<Outbox> out;
  std::set<std::string> subs;  // md: upper-case symbols
  bool user_stream = false;
};

struct Book {
  int64_t mid = 0, tick = 1'000'000;  // 0.01
  uint64_t u = 0;
  int64_t bid() const { return mid - tick; }
  int64_t ask() const { return mid; }
};

struct Resting {
  std::string cid, sym, side;
  int64_t qty, price, oid;
};

struct Mock {
  std::string secret;
  int rate = 50;
  int reject_every = 0;
  std::set<std::shared_ptr<Sess>> md, api;
  std::map<std::string, Book> books;
  std::map<std::string, Resting> resting;  // by cid
  int64_t next_oid = 1, next_tid = 1, orders = 0;
  std::mt19937_64 rng{42};

  Book& book(const std::string& sym) {
    auto [it, fresh] = books.try_emplace(sym);
    if (fresh) it->second.mid = sym.starts_with("BTC") ? 60000 * kScale
                                : sym.starts_with("ETH") ? 3000 * kScale
                                                          : 100 * kScale;
    return it->second;
  }

  void tick() {
    std::uniform_int_distribution<int> step(-3, 3);
    for (auto& [sym, b] : books) {
      bool watched = false;
      for (auto& s : md) watched |= s->subs.count(sym) > 0;
      if (!watched) continue;
      b.mid += step(rng) * b.tick;
      ++b.u;
      auto msg = json::serialize(json::object{{"u", b.u}, {"s", sym}, {"b", fmt_dec(b.bid())},
                                              {"B", "1.5"}, {"a", fmt_dec(b.ask())}, {"A", "2.25"}});
      for (auto& s : md)
        if (s->subs.count(sym)) s->out->push(msg);
    }
    for (auto it = resting.begin(); it != resting.end();) {
      auto& r = it->second;
      auto& b = book(r.sym);
      bool cross = r.side == "BUY" ? r.price >= b.ask() : r.price <= b.bid();
      if (cross) {
        report(r.cid, r.sym, r.side, "LIMIT", r.qty, r.price, r.oid, "TRADE", "FILLED", r.qty,
               r.side == "BUY" ? b.ask() : b.bid(), "");
        it = resting.erase(it);
      } else {
        ++it;
      }
    }
  }

  void report(const std::string& cid, const std::string& sym, const std::string& side,
              const std::string& type, int64_t q, int64_t p, int64_t oid, const char* x,
              const char* X, int64_t last_q, int64_t last_p, const std::string& orig) {
    json::object e{{"e", "executionReport"}, {"E", now_ms()}, {"s", sym}, {"c", cid},
                   {"S", side}, {"o", type}, {"q", fmt_dec(q)}, {"p", fmt_dec(p)},
                   {"x", x}, {"X", X}, {"i", oid}, {"l", fmt_dec(last_q)},
                   {"L", fmt_dec(last_p)}, {"z", fmt_dec(std::string_view(X) == "FILLED" ? q : 0)},
                   {"T", now_ms()}, {"t", last_q ? next_tid++ : -1}, {"C", orig}};
    auto msg = json::serialize(json::object{{"subscriptionId", 0}, {"event", e}});
    for (auto& s : api)
      if (s->user_stream) s->out->push(msg);
  }

  // Same rule the gateway signs with: every param except signature, sorted by
  // name, k=v joined with &.
  bool verify(const json::object& p) const {
    std::vector<std::pair<std::string, std::string>> kv;
    std::string sig;
    for (auto& [k, v] : p) {
      std::string val = v.is_string() ? std::string(v.as_string()) : json::serialize(v);
      if (k == "signature") sig = val;
      else kv.emplace_back(std::string(k), val);
    }
    std::sort(kv.begin(), kv.end());
    std::string q;
    for (auto& [k, v] : kv) q += (q.empty() ? "" : "&") + k + "=" + v;
    return !sig.empty() && hmac_sha256_hex(secret, q) == sig;
  }

  json::object error(const json::value& id, int code, const char* msg) {
    return json::object{{"id", id}, {"status", 400},
                        {"error", json::object{{"code", code}, {"msg", msg}}}};
  }

  void api_request(Sess& s, const json::object& r) {
    auto id = r.contains("id") ? r.at("id") : json::value(nullptr);
    auto method = jstr(r, "method");
    auto* pp = r.if_contains("params");
    if (!pp || !pp->is_object()) return s.out->push(json::serialize(error(id, -1102, "params")));
    auto& p = pp->as_object();
    if (!verify(p))
      return s.out->push(json::serialize(error(id, -1022, "Signature for this request is not valid.")));

    if (method == "userDataStream.subscribe.signature") {
      s.user_stream = true;
      s.out->push(json::serialize(json::object{
          {"id", id}, {"status", 200}, {"result", json::object{{"subscriptionId", 0}}}}));
      return;
    }
    if (method == "order.place") {
      auto cid = jstr(p, "newClientOrderId"), sym = jstr(p, "symbol"), side = jstr(p, "side"),
           type = jstr(p, "type");
      auto q = parse_dec(jstr(p, "quantity")).value_or(0);
      auto px = parse_dec(jstr(p, "price")).value_or(0);
      if (reject_every && ++orders % reject_every == 0)
        return s.out->push(json::serialize(
            error(id, -2010, "Account has insufficient balance for requested action.")));
      int64_t oid = next_oid++;
      s.out->push(json::serialize(json::object{
          {"id", id}, {"status", 200},
          {"result", json::object{{"symbol", sym}, {"orderId", oid}, {"clientOrderId", cid},
                                  {"transactTime", now_ms()}}}}));
      auto& b = book(sym);
      report(cid, sym, side, type, q, px, oid, "NEW", "NEW", 0, 0, "");
      bool marketable = type == "MARKET" || (side == "BUY" ? px >= b.ask() : px <= b.bid());
      if (marketable)
        report(cid, sym, side, type, q, px, oid, "TRADE", "FILLED", q,
               side == "BUY" ? b.ask() : b.bid(), "");
      else
        resting[cid] = Resting{cid, sym, side, q, px, oid};
      return;
    }
    if (method == "order.cancel") {
      auto orig = jstr(p, "origClientOrderId");
      auto it = resting.find(orig);
      if (it == resting.end()) return s.out->push(json::serialize(error(id, -2011, "Unknown order sent.")));
      auto r2 = it->second;
      resting.erase(it);
      s.out->push(json::serialize(json::object{
          {"id", id}, {"status", 200},
          {"result", json::object{{"symbol", r2.sym}, {"origClientOrderId", orig}, {"status", "CANCELED"}}}}));
      report("cxl" + std::to_string(next_oid++), r2.sym, r2.side, "LIMIT", r2.qty, r2.price, r2.oid,
             "CANCELED", "CANCELED", 0, 0, orig);
      return;
    }
    s.out->push(json::serialize(error(id, -1100, "unknown method")));
  }

  void md_request(Sess& s, const json::object& r) {
    auto method = jstr(r, "method");
    if (auto* p = r.if_contains("params"); p && p->is_array()) {
      for (auto& v : p->as_array()) {
        if (!v.is_string()) continue;
        std::string st(v.as_string());
        auto sym = st.substr(0, st.find('@'));
        std::transform(sym.begin(), sym.end(), sym.begin(), ::toupper);
        if (method == "SUBSCRIBE") s.subs.insert(sym), book(sym);
        else s.subs.erase(sym);
      }
    }
    s.out->push(json::serialize(json::object{{"result", nullptr}, {"id", r.contains("id") ? r.at("id") : json::value(nullptr)}}));
  }
};

static awaitable<void> serve(Mock& m, ssl::context& ctx, tcp::socket sock) {
  try {
    auto ws = std::make_shared<Wss>(std::move(sock), ctx);
    co_await ws->next_layer().async_handshake(ssl::stream_base::server, use_awaitable);
    beast::flat_buffer buf;
    http::request<http::string_body> req;
    co_await http::async_read(ws->next_layer(), buf, req, use_awaitable);
    bool is_api = req.target().starts_with("/ws-api");
    co_await ws->async_accept(req, use_awaitable);
    ws->text(true);
    auto s = std::make_shared<Sess>();
    s->ws = ws;
    s->out = std::make_shared<Outbox>(ws->get_executor());
    net::co_spawn(ws->get_executor(), Outbox::run(s->out, ws), net::detached);
    (is_api ? m.api : m.md).insert(s);
    try {
      for (;;) {
        buf.consume(buf.size());
        co_await ws->async_read(buf, use_awaitable);
        auto d = buf.data();
        boost::system::error_code ec;
        auto v = json::parse(std::string_view(static_cast<const char*>(d.data()), d.size()), ec);
        if (ec || !v.is_object()) continue;
        if (is_api) m.api_request(*s, v.as_object());
        else m.md_request(*s, v.as_object());
      }
    } catch (const std::exception&) {
    }
    s->out->stop();
    (is_api ? m.api : m.md).erase(s);
  } catch (const std::exception& e) {
    std::fprintf(stderr, "[mockex] %s\n", e.what());
  }
}

int main(int argc, char** argv) {
  std::map<std::string, std::string> a;
  for (int i = 1; i + 1 < argc; i += 2) a[std::string(argv[i]).substr(2)] = argv[i + 1];
  auto get = [&](const char* k, std::string d) { return a.count(k) ? a[k] : d; };

  Mock m;
  m.secret = get("secret", "mock-secret");
  m.rate = std::stoi(get("rate", "50"));
  m.reject_every = std::stoi(get("reject-every", "0"));
  net::io_context ioc{1};
  ssl::context ctx(ssl::context::tls_server);
  ctx.use_certificate_chain_file(get("cert", "certs/mock.pem"));
  ctx.use_private_key_file(get("key", "certs/mock.key"), ssl::context::pem);

  auto port = static_cast<unsigned short>(std::stoi(get("port", "9443")));
  net::co_spawn(ioc, [&]() -> awaitable<void> {
    tcp::acceptor acc(ioc, tcp::endpoint(tcp::v4(), port));
    for (;;) {
      auto s = co_await acc.async_accept(use_awaitable);
      s.set_option(tcp::no_delay(true));
      net::co_spawn(ioc, serve(m, ctx, std::move(s)), net::detached);
    }
  }, net::detached);

  // Book ticks: rate per second per watched symbol, batched per 1 ms timer.
  net::co_spawn(ioc, [&]() -> awaitable<void> {
    net::steady_timer t(ioc);
    auto period = std::chrono::microseconds(1'000'000 / std::max(1, std::min(m.rate, 1000)));
    int per_tick = std::max(1, m.rate / 1000);
    auto next = std::chrono::steady_clock::now();
    for (;;) {
      next += period;
      t.expires_at(next);
      co_await t.async_wait(use_awaitable);
      for (int i = 0; i < per_tick; i++) m.tick();
    }
  }, net::detached);

  net::signal_set sigs(ioc, SIGINT, SIGTERM);
  sigs.async_wait([&](auto, auto) { ioc.stop(); });
  std::fprintf(stderr, "[mockex] wss on :%u, %d ticks/s/symbol\n", port, m.rate);
  ioc.run();
}
