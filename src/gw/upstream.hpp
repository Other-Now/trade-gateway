// Binance spot (testnet) connectivity. Two TLS WebSockets:
//   market data  wss://stream.testnet.binance.vision/ws   <sym>@bookTicker, live SUBSCRIBE/UNSUBSCRIBE
//   trading      wss://ws-api.testnet.binance.vision/ws-api/v3
//                order.place / order.cancel (HMAC-SHA256 signed) and
//                userDataStream.subscribe.signature for executionReports.
// The gateway trades through ONE exchange account for all its clients; the
// client order id carries the client account so reports route back.
#pragma once
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "../common/net.hpp"
#include "../common/util.hpp"
#include "risk.hpp"

namespace tg {

class Hub;

class Upstream {
 public:
  struct Options {
    std::string md_url = "wss://stream.testnet.binance.vision/ws";
    std::string api_url = "wss://ws-api.testnet.binance.vision/ws-api/v3";
    std::string api_key, api_secret;
    bool insecure = false;  // skip TLS verification (only for the local mock's self-signed cert)
  };
  Upstream(net::io_context& ioc, Options o, Hub& hub);
  void start();

  void set_symbols(const std::set<std::string>& syms);
  void place(const std::string& cid, const OrderReq& o);
  void cancel(const std::string& cid, const std::string& symbol);

  bool md_connected() const { return md_ws_ != nullptr; }
  bool trading_configured() const { return !opt_.api_key.empty(); }
  bool trading_ready() const { return trade_ready_; }
  int64_t md_reconnects = 0, trade_reconnects = 0;

  // Exposed for tests: the WS-API signing rule (params sorted by name,
  // joined as k=v&k=v, HMAC-SHA256 hex with the secret).
  static std::string signing_payload(std::vector<std::pair<std::string, std::string>> p);

 private:
  using Wss = websocket::stream<beast::ssl_stream<beast::tcp_stream>>;
  awaitable<std::shared_ptr<Wss>> connect(const Url& u);
  awaitable<void> md_loop();
  awaitable<void> trade_loop();
  void sync_md_subs();
  json::object sign(std::vector<std::pair<std::string, std::string>> p);

  net::io_context& ioc_;
  Options opt_;
  Hub& hub_;
  ssl::context ctx_{ssl::context::tls_client};

  std::shared_ptr<Wss> md_ws_;
  std::shared_ptr<Outbox> md_out_;
  std::set<std::string> want_, have_;
  int64_t md_req_id_ = 0;

  std::shared_ptr<Wss> tr_ws_;
  std::shared_ptr<Outbox> tr_out_;
  bool trade_ready_ = false;
  std::set<std::string> inflight_;  // order.place sent, response not yet seen
};

}  // namespace tg
