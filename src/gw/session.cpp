#include "session.hpp"

#include "hub.hpp"

namespace tg {

WsSession::WsSession(Stream ws, Hub& hub, int acct, std::string key_hash)
    : ws_(std::move(ws)), hub_(hub), acct_(acct), key_hash_(std::move(key_hash)),
      signal_(ws_.get_executor()) {}

awaitable<void> WsSession::run() {
  auto self = shared_from_this();
  ws_.set_option(websocket::stream_base::timeout::suggested(beast::role_type::server));
  ws_.text(true);
  hub_.add(self);
  net::co_spawn(ws_.get_executor(), writer(self), net::detached);
  try {
    beast::flat_buffer buf;
    for (;;) {
      co_await ws_.async_read(buf, use_awaitable);
      auto d = buf.data();
      hub_.on_client_msg(*this, std::string_view(static_cast<const char*>(d.data()), d.size()));
      buf.consume(buf.size());
    }
  } catch (const std::exception&) {
  }
  closed_ = true;
  signal_.cancel();
  hub_.remove(this);
}

void WsSession::send(std::shared_ptr<const std::string> msg) {
  if (closed_) return;
  if (!hub_.opts().no_conflate && events_.size() >= hub_.opts().max_queue) {
    // The client is not reading and we will not drop order events, so the
    // only safe move is to cut it off; it reconnects and asks for "orders".
    ++hub_.m.slow_disconnects;
    hard_close();
    return;
  }
  events_.push_back(std::move(msg));
  signal_.cancel();
}

void WsSession::send_md(const std::string& sym, std::shared_ptr<const std::string> msg) {
  if (closed_) return;
  if (hub_.opts().no_conflate) {  // control: every tick queued, unbounded
    events_.push_back(std::move(msg));
    signal_.cancel();
    return;
  }
  auto [it, fresh] = md_.try_emplace(sym, msg);
  if (fresh) {
    md_order_.push_back(sym);
  } else {
    it->second = std::move(msg);  // overwrite the unsent tick: conflation
    ++hub_.m.md_conflated;
  }
  signal_.cancel();
}

void WsSession::kick(std::string reason) {
  if (closed_) return;
  kick_ = true;
  kick_reason_ = std::move(reason);
  signal_.cancel();
  // If the client isn't reading, the close frame may never get out; don't
  // let a revoked key keep its socket because of that.
  auto t = std::make_shared<net::steady_timer>(ws_.get_executor(), std::chrono::seconds(2));
  t->async_wait([w = weak_from_this(), t](boost::system::error_code) {
    if (auto s = w.lock(); s && !s->closed_) s->hard_close();
  });
}

void WsSession::hard_close() {
  closed_ = true;
  signal_.cancel();
  boost::system::error_code ec;
  beast::get_lowest_layer(ws_).socket().close(ec);
}

awaitable<void> WsSession::writer(std::shared_ptr<WsSession> self) {
  try {
    while (!self->closed_) {
      // Order events first: a fill must never wait behind market data.
      if (!self->events_.empty()) {
        auto m = std::move(self->events_.front());
        self->events_.pop_front();
        co_await self->ws_.async_write(net::buffer(*m), use_awaitable);
        continue;
      }
      if (!self->md_order_.empty()) {
        auto sym = std::move(self->md_order_.front());
        self->md_order_.pop_front();
        auto it = self->md_.find(sym);
        auto m = std::move(it->second);
        self->md_.erase(it);
        co_await self->ws_.async_write(net::buffer(*m), use_awaitable);
        continue;
      }
      if (self->kick_) {
        co_await self->ws_.async_close(
            websocket::close_reason(websocket::close_code::policy_error, self->kick_reason_),
            use_awaitable);
        self->closed_ = true;
        break;
      }
      self->signal_.expires_at(net::steady_timer::time_point::max());
      boost::system::error_code ec;
      co_await self->signal_.async_wait(net::redirect_error(use_awaitable, ec));
    }
  } catch (const std::exception&) {
    self->closed_ = true;
  }
}

// ---------------------------------------------------------------- HTTP

using Req = http::request<http::string_body>;
using Res = http::response<http::string_body>;

static Res reply(const Req& req, http::status st, std::string body,
                 const char* ctype = "application/json") {
  Res r{st, req.version()};
  r.set(http::field::content_type, ctype);
  r.body() = std::move(body);
  return r;
}

static std::string err(std::string_view why) {
  return json::serialize(json::object{{"error", why}});
}

static Res handle_http(Hub& hub, const Req& req) {
  std::string_view target = req.target();
  if (req.method() == http::verb::get && target == "/health") return reply(req, http::status::ok, hub.health());
  if (req.method() == http::verb::get && target == "/metrics")
    return reply(req, http::status::ok, hub.metrics(), "text/plain");
  if (!target.starts_with("/api/")) return reply(req, http::status::not_found, err("not found"));

  auto acct = hub.authenticate(std::string(req["X-API-Key"]));
  if (!acct) return reply(req, http::status::unauthorized, err("invalid api key"));

  if (target == "/api/orders" && req.method() == http::verb::post) {
    boost::system::error_code ec;
    auto v = json::parse(req.body(), ec);
    if (ec || !v.is_object()) return reply(req, http::status::bad_request, err("bad json"));
    auto r = hub.place(*acct, v.as_object());
    if (!r.ok) return reply(req, http::status::unprocessable_entity, err(r.reason));
    return reply(req, http::status::accepted, json::serialize(json::object{{"cid", r.cid}}));
  }
  if (target == "/api/orders" && req.method() == http::verb::get)
    return reply(req, http::status::ok, json::serialize(hub.orders_for(*acct)));
  if (target.starts_with("/api/orders/") && req.method() == http::verb::delete_) {
    auto r = hub.cancel(*acct, std::string(target.substr(12)));
    if (!r.ok)
      return reply(req, r.reason == "unknown order" ? http::status::not_found
                                                    : http::status::unprocessable_entity,
                   err(r.reason));
    return reply(req, http::status::accepted, json::serialize(json::object{{"cid", r.cid}}));
  }
  if (target == "/api/instruments" && req.method() == http::verb::get) {
    auto cfg = hub.config();
    json::array arr;
    for (auto& [sym, lim] : cfg->account(*acct)->limits) {
      if (!cfg->permitted(*acct, sym)) continue;
      auto& i = cfg->instruments.at(sym);
      arr.push_back(json::object{{"symbol", sym}, {"tick_size", fmt_dec(i.tick)},
                                 {"lot_size", fmt_dec(i.lot)}, {"max_qty", fmt_dec(lim.max_qty)},
                                 {"max_notional", fmt_dec(lim.max_notional)}});
    }
    return reply(req, http::status::ok, json::serialize(arr));
  }
  return reply(req, http::status::not_found, err("not found"));
}

static awaitable<void> serve(Hub& hub, tcp::socket sock) {
  beast::tcp_stream stream(std::move(sock));
  beast::flat_buffer buf;
  try {
    for (;;) {
      Req req;
      stream.expires_after(std::chrono::seconds(30));
      co_await http::async_read(stream, buf, req, use_awaitable);

      if (websocket::is_upgrade(req)) {
        std::string hash;
        auto acct = req.target() == "/ws"
                        ? hub.authenticate(std::string(req["X-API-Key"]), &hash)
                        : std::nullopt;
        if (!acct) {
          auto r = reply(req, http::status::unauthorized, err("invalid api key"));
          r.prepare_payload();
          co_await http::async_write(stream, r, use_awaitable);
          co_return;
        }
        stream.expires_never();  // the websocket has its own idle/ping timeouts
        WsSession::Stream ws(std::move(stream));
        co_await ws.async_accept(req, use_awaitable);
        auto s = std::make_shared<WsSession>(std::move(ws), hub, *acct, hash);
        co_await s->run();
        co_return;
      }

      auto res = handle_http(hub, req);
      res.keep_alive(req.keep_alive());
      res.prepare_payload();
      co_await http::async_write(stream, res, use_awaitable);
      if (!req.keep_alive()) break;
    }
  } catch (const std::exception&) {
  }
  boost::system::error_code ec;
  stream.socket().shutdown(tcp::socket::shutdown_both, ec);
}

awaitable<void> listen(Hub& hub, tcp::endpoint ep) {
  auto ex = co_await net::this_coro::executor;
  tcp::acceptor acc(ex, ep);
  for (;;) {
    auto sock = co_await acc.async_accept(use_awaitable);
    sock.set_option(tcp::no_delay(true));
    net::co_spawn(ex, serve(hub, std::move(sock)), net::detached);
  }
}

}  // namespace tg
