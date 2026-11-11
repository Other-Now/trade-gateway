#include "upstream.hpp"

#include <openssl/ssl.h>

#include <algorithm>
#include <cstdio>

#include "hub.hpp"

namespace tg {

Upstream::Upstream(net::io_context& ioc, Options o, Hub& hub)
    : ioc_(ioc), opt_(std::move(o)), hub_(hub) {
  if (opt_.insecure) {
    ctx_.set_verify_mode(ssl::verify_none);
  } else {
    ctx_.set_default_verify_paths();
    ctx_.set_verify_mode(ssl::verify_peer);
  }
}

void Upstream::start() {
  net::co_spawn(ioc_, md_loop(), net::detached);
  if (trading_configured()) net::co_spawn(ioc_, trade_loop(), net::detached);
}

awaitable<std::shared_ptr<Upstream::Wss>> Upstream::connect(const Url& u) {
  auto ex = co_await net::this_coro::executor;
  tcp::resolver res(ex);
  auto eps = co_await res.async_resolve(u.host, u.port, use_awaitable);
  auto ws = std::make_shared<Wss>(ex, ctx_);
  auto& tcp_layer = beast::get_lowest_layer(*ws);
  tcp_layer.expires_after(std::chrono::seconds(10));
  co_await tcp_layer.async_connect(eps, use_awaitable);
  tcp_layer.socket().set_option(tcp::no_delay(true));
  // SNI: Binance sits behind a CDN that needs the host name in the ClientHello.
  if (!SSL_set_tlsext_host_name(ws->next_layer().native_handle(), u.host.c_str()))
    throw std::runtime_error("SNI failed");
  if (!opt_.insecure) ws->next_layer().set_verify_callback(ssl::host_name_verification(u.host));
  co_await ws->next_layer().async_handshake(ssl::stream_base::client, use_awaitable);
  tcp_layer.expires_never();
  ws->set_option(websocket::stream_base::timeout::suggested(beast::role_type::client));
  ws->text(true);
  co_await ws->async_handshake(u.host, u.target, use_awaitable);
  co_return ws;
}

// ---------------------------------------------------------------- market data

void Upstream::set_symbols(const std::set<std::string>& syms) {
  want_ = syms;
  sync_md_subs();
}

void Upstream::sync_md_subs() {
  if (!md_out_) return;  // applied on (re)connect
  auto stream = [](std::string s) {
    std::transform(s.begin(), s.end(), s.begin(), ::tolower);
    return s + "@bookTicker";
  };
  json::array add, del;
  for (auto& s : want_)
    if (!have_.count(s)) add.push_back(json::value(stream(s)));
  for (auto& s : have_)
    if (!want_.count(s)) del.push_back(json::value(stream(s)));
  if (!add.empty())
    md_out_->push(json::serialize(
        json::object{{"method", "SUBSCRIBE"}, {"params", add}, {"id", ++md_req_id_}}));
  if (!del.empty())
    md_out_->push(json::serialize(
        json::object{{"method", "UNSUBSCRIBE"}, {"params", del}, {"id", ++md_req_id_}}));
  if (!add.empty() || !del.empty())
    std::fprintf(stderr, "[md] subscribe +%zu -%zu\n", add.size(), del.size());
  have_ = want_;
}

awaitable<void> Upstream::md_loop() {
  auto url = parse_url(opt_.md_url);
  if (!url || !url->tls) throw std::runtime_error("md url must be wss://");
  net::steady_timer t(ioc_);
  int backoff_ms = 250;
  for (;;) {
    try {
      auto ws = co_await connect(*url);
      md_ws_ = ws;
      md_out_ = std::make_shared<Outbox>(ws->get_executor());
      net::co_spawn(ioc_, Outbox::run(md_out_, ws), net::detached);
      have_.clear();
      sync_md_subs();
      backoff_ms = 250;
      std::fprintf(stderr, "[md] connected %s\n", opt_.md_url.c_str());
      beast::flat_buffer buf;
      for (;;) {
        co_await ws->async_read(buf, use_awaitable);
        int64_t rx = now_us();
        auto d = buf.data();
        boost::system::error_code ec;
        auto v = json::parse(std::string_view(static_cast<const char*>(d.data()), d.size()), ec);
        buf.consume(buf.size());
        if (ec || !v.is_object()) continue;
        auto& o = v.as_object();
        if (auto* data = o.if_contains("data"); data && data->is_object())  // /stream wrapper
          hub_.on_md(data->as_object(), rx);
        else if (o.contains("s") && o.contains("b"))  // raw /ws bookTicker
          hub_.on_md(o, rx);
        // else: {"result":null,"id":n} subscription acks
      }
    } catch (const std::exception& e) {
      std::fprintf(stderr, "[md] %s\n", e.what());
    }
    if (md_out_) md_out_->stop();
    md_out_.reset();
    md_ws_.reset();
    ++md_reconnects;
    t.expires_after(std::chrono::milliseconds(backoff_ms));
    co_await t.async_wait(use_awaitable);
    backoff_ms = std::min(backoff_ms * 2, 5000);
  }
}

// ---------------------------------------------------------------- trading

std::string Upstream::signing_payload(std::vector<std::pair<std::string, std::string>> p) {
  std::sort(p.begin(), p.end());
  std::string q;
  for (auto& [k, v] : p) {
    if (!q.empty()) q += '&';
    q += k + "=" + v;
  }
  return q;
}

json::object Upstream::sign(std::vector<std::pair<std::string, std::string>> p) {
  int64_t ts = now_ms();
  p.emplace_back("apiKey", opt_.api_key);
  p.emplace_back("timestamp", std::to_string(ts));
  auto sig = hmac_sha256_hex(opt_.api_secret, signing_payload(p));
  json::object o;
  for (auto& [k, v] : p) o[k] = v;
  o["timestamp"] = ts;  // numeric in JSON, decimal text in the signed payload
  o["signature"] = sig;
  return o;
}

void Upstream::place(const std::string& cid, const OrderReq& o) {
  if (!tr_out_) return;
  std::vector<std::pair<std::string, std::string>> p{
      {"symbol", o.symbol}, {"side", o.side}, {"type", o.type}, {"quantity", fmt_dec(o.qty)},
      {"newClientOrderId", cid}, {"newOrderRespType", "ACK"}};
  if (o.type == "LIMIT") {
    p.emplace_back("timeInForce", o.tif);
    p.emplace_back("price", fmt_dec(o.price));
  }
  tr_out_->push(json::serialize(
      json::object{{"id", "p-" + cid}, {"method", "order.place"}, {"params", sign(std::move(p))}}));
  inflight_.insert(cid);
}

void Upstream::cancel(const std::string& cid, const std::string& symbol) {
  if (!tr_out_) return;
  tr_out_->push(json::serialize(json::object{
      {"id", "c-" + cid},
      {"method", "order.cancel"},
      {"params", sign({{"symbol", symbol}, {"origClientOrderId", cid}})}}));
}

awaitable<void> Upstream::trade_loop() {
  auto url = parse_url(opt_.api_url);
  if (!url || !url->tls) throw std::runtime_error("api url must be wss://");
  net::steady_timer t(ioc_);
  int backoff_ms = 250;
  for (;;) {
    try {
      auto ws = co_await connect(*url);
      tr_ws_ = ws;
      tr_out_ = std::make_shared<Outbox>(ws->get_executor());
      net::co_spawn(ioc_, Outbox::run(tr_out_, ws), net::detached);
      tr_out_->push(json::serialize(json::object{
          {"id", "sub"}, {"method", "userDataStream.subscribe.signature"}, {"params", sign({})}}));
      beast::flat_buffer buf;
      for (;;) {
        co_await ws->async_read(buf, use_awaitable);
        auto d = buf.data();
        boost::system::error_code ec;
        auto v = json::parse(std::string_view(static_cast<const char*>(d.data()), d.size()), ec);
        buf.consume(buf.size());
        if (ec || !v.is_object()) continue;
        auto& o = v.as_object();
        if (auto* ev = o.if_contains("event"); ev && ev->is_object()) {
          if (jstr(ev->as_object(), "e") == "executionReport") hub_.on_exec(ev->as_object());
          continue;
        }
        auto id = jstr(o, "id");
        int64_t st = jint(o, "status");
        std::string code, msg;
        if (auto* e = o.if_contains("error"); e && e->is_object()) {
          code = jstr(e->as_object(), "code");
          msg = jstr(e->as_object(), "msg");
        }
        if (id == "sub") {
          if (st != 200) throw std::runtime_error("user data subscribe failed: " + msg);
          trade_ready_ = true;
          backoff_ms = 250;
          std::fprintf(stderr, "[trade] connected, user data stream subscribed\n");
        } else if (id.starts_with("p-")) {
          auto cid = id.substr(2);
          inflight_.erase(cid);
          if (st != 200) hub_.on_upstream_reject(cid, code.empty() ? -1 : std::stoll(code), msg);
        } else if (id.starts_with("c-") && st != 200) {
          hub_.on_cancel_reject(id.substr(2), msg);
        }
      }
    } catch (const std::exception& e) {
      std::fprintf(stderr, "[trade] %s\n", e.what());
    }
    trade_ready_ = false;
    if (tr_out_) tr_out_->stop();
    tr_out_.reset();
    tr_ws_.reset();
    ++trade_reconnects;
    // We sent these but never heard back: they may or may not be live at the
    // exchange. Say so instead of guessing (reconciliation is future work).
    for (auto& cid : inflight_) hub_.on_upstream_reject(cid, 0, "upstream disconnected; order state unknown");
    inflight_.clear();
    t.expires_after(std::chrono::milliseconds(backoff_ms));
    co_await t.async_wait(use_awaitable);
    backoff_ms = std::min(backoff_ms * 2, 5000);
  }
}

}  // namespace tg
