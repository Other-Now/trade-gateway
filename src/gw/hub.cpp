#include "hub.hpp"

#include <format>

#include "session.hpp"
#include "upstream.hpp"

namespace tg {

Hub::Hub(HubOptions o) : opt_(std::move(o)), seq_(uint64_t(now_ms()) * 1000) {}

std::optional<int> Hub::authenticate(const std::string& api_key, std::string* hash_out) {
  if (!cfg_ || api_key.empty()) return std::nullopt;
  auto h = sha256_hex(api_key);
  auto a = cfg_->auth(h);
  if (!a) ++m.auth_failures;
  if (hash_out) *hash_out = h;
  return a;
}

// ---------------------------------------------------------------- config

void Hub::apply_config(std::shared_ptr<const Config> cfg, const ReloadInfo& info) {
  cfg_ = std::move(cfg);
  ++m.config_reloads;
  m.cfg_reload_us.add(info.reload_us);
  if (info.notify_ts_us) m.cfg_apply_us.add(now_us() - info.notify_ts_us);
  std::fprintf(stderr, "[cfg] v%lld applied (%s, query %lld us)\n", (long long)cfg_->version,
               info.cause, (long long)info.reload_us);

  // Re-check every live session against the new snapshot. Authentication is
  // not a one-off at connect time: a revoked key loses its open sockets too.
  std::vector<WsSession*> kick;
  json::object note{{"type", "config"}, {"version", cfg_->version}};
  auto note_msg = std::make_shared<const std::string>(json::serialize(note));
  for (auto& [p, s] : sessions_) {
    auto a = cfg_->auth(s->key_hash());
    if (!a || *a != s->account()) {
      kick.push_back(p);
      continue;
    }
    for (auto it = s->subs().begin(); it != s->subs().end();) {
      if (cfg_->permitted(s->account(), *it)) {
        ++it;
        continue;
      }
      by_sym_[*it].erase(p);
      s->send(json::object{{"type", "unsubscribed"}, {"symbol", *it}, {"reason", "not permitted"}});
      it = s->subs().erase(it);
    }
    s->send(note_msg);
  }
  for (auto* p : kick) {
    sessions_[p]->kick("api key revoked or account disabled");
    remove(p);
    ++m.kicked;
  }
  if (up_) up_->set_symbols(cfg_->md_symbols());
}

// ---------------------------------------------------------------- sessions

void Hub::add(std::shared_ptr<WsSession> s) {
  auto* p = s.get();
  by_acct_[s->account()].insert(p);
  sessions_[p] = std::move(s);
}

void Hub::remove(WsSession* p) {
  auto it = sessions_.find(p);
  if (it == sessions_.end()) return;
  for (auto& sym : p->subs()) by_sym_[sym].erase(p);
  auto a = by_acct_.find(p->account());
  if (a != by_acct_.end()) {
    a->second.erase(p);
    if (a->second.empty()) by_acct_.erase(a);
  }
  sessions_.erase(it);  // the session's own coroutines keep it alive until they finish
}

void Hub::send_to_account(int acct, const json::object& msg) {
  auto it = by_acct_.find(acct);
  if (it == by_acct_.end()) return;
  auto s = std::make_shared<const std::string>(json::serialize(msg));
  for (auto* p : it->second) p->send(s);
}

void Hub::on_client_msg(WsSession& s, std::string_view text) {
  boost::system::error_code ec;
  auto v = json::parse(text, ec);
  if (ec || !v.is_object()) {
    s.send(json::object{{"type", "error"}, {"reason", "bad json"}});
    return;
  }
  auto& o = v.as_object();
  auto op = jstr(o, "op");
  json::value req_id = o.contains("req_id") ? o.at("req_id") : json::value(nullptr);

  if (op == "subscribe" || op == "unsubscribe") {
    auto* syms = o.if_contains("symbols");
    if (!syms || !syms->is_array()) {
      s.send(json::object{{"type", "error"}, {"req_id", req_id}, {"reason", "symbols[] required"}});
      return;
    }
    for (auto& sv : syms->as_array()) {
      if (!sv.is_string()) continue;
      std::string sym(sv.as_string());
      if (op == "unsubscribe") {
        s.subs().erase(sym);
        by_sym_[sym].erase(&s);
        s.send(json::object{{"type", "unsubscribed"}, {"symbol", sym}});
      } else if (cfg_ && cfg_->permitted(s.account(), sym)) {
        s.subs().insert(sym);
        by_sym_[sym].insert(&s);
        s.send(json::object{{"type", "subscribed"}, {"symbol", sym}});
        auto b = books_.find(sym);  // snapshot so the client doesn't wait for the next tick
        if (b != books_.end() && b->second.last) {
          json::object snap = b->second.last_obj;  // marked, so nobody mistakes its age for latency
          snap["snapshot"] = true;
          s.send_md(sym, std::make_shared<const std::string>(json::serialize(snap)));
        }
      } else {
        s.send(json::object{{"type", "error"}, {"req_id", req_id}, {"symbol", sym},
                            {"reason", "not permitted"}});
      }
    }
    return;
  }
  if (op == "order" || op == "cancel") {
    Result r = op == "order" ? place(s.account(), o) : cancel(s.account(), jstr(o, "cid"));
    if (r.ok)
      s.send(json::object{{"type", "ack"}, {"op", op}, {"req_id", req_id}, {"cid", r.cid}});
    else
      s.send(json::object{{"type", "reject"}, {"op", op}, {"req_id", req_id}, {"reason", r.reason}});
    return;
  }
  if (op == "orders") {
    s.send(json::object{{"type", "orders"}, {"req_id", req_id}, {"orders", orders_for(s.account())}});
    return;
  }
  s.send(json::object{{"type", "error"}, {"req_id", req_id}, {"reason", "unknown op"}});
}

// ---------------------------------------------------------------- orders

Hub::Result Hub::place(int acct, const json::object& req) {
  ++m.orders_in;
  if (!cfg_) return {false, "", "config not loaded"};
  const Account* a = cfg_->account(acct);
  if (!a) return {false, "", "account disabled"};

  OrderReq o;
  o.symbol = jstr(req, "symbol");
  o.side = jstr(req, "side");
  if (auto t = jstr(req, "type"); !t.empty()) o.type = t;
  if (auto t = jstr(req, "tif"); !t.empty()) o.tif = t;
  auto q = parse_dec(jstr(req, "qty"));
  if (!q) return {false, "", "bad qty"};
  o.qty = *q;
  if (o.type == "LIMIT") {
    auto p = parse_dec(jstr(req, "price"));
    if (!p) return {false, "", "bad price"};
    o.price = *p;
  }

  if (!buckets_[acct].take(a->max_orders_per_sec, now_us())) {
    ++m.rate_rejects;
    return {false, "", "rate limit"};
  }
  int64_t ref = 0;
  if (auto b = books_.find(o.symbol); b != books_.end())
    ref = o.side == "BUY" ? b->second.ask : b->second.bid;
  if (auto why = check_order(*cfg_, acct, o, ref); !why.empty()) {
    ++m.risk_rejects;
    return {false, "", why};
  }
  if (!up_ || !up_->trading_ready()) return {false, "", "upstream trading unavailable"};

  auto cid = std::format("tg-{}-{}{}", acct, opt_.gw_id, ++seq_);
  orders_[cid] = OrderRec{cid, o.symbol, o.side, o.type, "PENDING_NEW", acct, o.qty, o.price, 0, 0};
  up_->place(cid, o);
  ++m.orders_sent;
  return {true, cid, ""};
}

Hub::Result Hub::cancel(int acct, const std::string& cid) {
  auto it = orders_.find(cid);
  // Another account's order gets exactly the same answer as a non-existent
  // one, so the API can't be used to probe other clients' order ids.
  if (it == orders_.end() || it->second.acct != acct) return {false, "", "unknown order"};
  auto& st = it->second.status;
  if (st == "FILLED" || st == "CANCELED" || st == "REJECTED" || st == "EXPIRED")
    return {false, "", "order not open"};
  if (!up_ || !up_->trading_ready()) return {false, "", "upstream trading unavailable"};
  up_->cancel(cid, it->second.symbol);
  return {true, cid, ""};
}

json::array Hub::orders_for(int acct) const {
  json::array arr;
  for (auto& [cid, r] : orders_) {
    if (r.acct != acct) continue;
    arr.push_back(json::object{{"cid", cid}, {"symbol", r.symbol}, {"side", r.side},
                               {"type", r.type}, {"status", r.status}, {"qty", fmt_dec(r.qty)},
                               {"price", fmt_dec(r.price)}, {"cum_qty", fmt_dec(r.cum)},
                               {"exch_order_id", r.exch_id}});
  }
  return arr;
}

// ---------------------------------------------------------------- upstream events

void Hub::on_md(const json::object& d, int64_t rx_us) {
  ++m.md_in;
  auto sym = jstr(d, "s");
  auto bid = jstr(d, "b"), ask = jstr(d, "a");
  auto& bk = books_[sym];
  bk.bid = parse_dec(bid).value_or(0);
  bk.ask = parse_dec(ask).value_or(0);
  json::object o{{"type", "md"}, {"symbol", sym}, {"bid", bid}, {"bid_qty", jstr(d, "B")},
                 {"ask", ask}, {"ask_qty", jstr(d, "A")}, {"u", jint(d, "u")}, {"rx_us", rx_us}};
  bk.last_obj = o;
  bk.last = std::make_shared<const std::string>(json::serialize(o));  // serialised once, shared by all
  auto it = by_sym_.find(sym);
  if (it == by_sym_.end()) return;
  for (auto* s : it->second) {
    s->send_md(sym, bk.last);
    ++m.md_out;
  }
}

void Hub::on_exec(const json::object& e) {
  // On a cancel, "c" is the cancel request's own id and "C" the original.
  std::string x = jstr(e, "x");
  std::string cid = jstr(e, "c");
  if (x == "CANCELED" && !jstr(e, "C").empty()) cid = jstr(e, "C");
  auto acct = account_from_cid(cid);
  if (!acct) {  // placed outside this gateway (e.g. the testnet web UI)
    ++m.foreign_exec;
    return;
  }
  ++m.exec_events;
  auto& r = orders_[cid];  // may be new: another gateway instance placed it
  if (r.cid.empty()) {
    r = OrderRec{cid, jstr(e, "s"), jstr(e, "S"), jstr(e, "o"), "", *acct,
                 parse_dec(jstr(e, "q")).value_or(0), parse_dec(jstr(e, "p")).value_or(0), 0, 0};
  }
  r.status = jstr(e, "X");
  r.cum = parse_dec(jstr(e, "z")).value_or(r.cum);
  r.exch_id = jint(e, "i");
  send_to_account(*acct, json::object{
                             {"type", "exec"}, {"cid", cid}, {"symbol", r.symbol}, {"side", r.side},
                             {"exec_type", x}, {"status", r.status}, {"qty", jstr(e, "q")},
                             {"price", jstr(e, "p")}, {"last_qty", jstr(e, "l")},
                             {"last_price", jstr(e, "L")}, {"cum_qty", jstr(e, "z")},
                             {"exch_order_id", r.exch_id}, {"exch_ts", jint(e, "E")}});
}

void Hub::on_upstream_reject(const std::string& cid, int64_t code, const std::string& msg) {
  ++m.upstream_rejects;
  auto it = orders_.find(cid);
  if (it == orders_.end()) return;
  it->second.status = code == 0 ? "UNKNOWN" : "REJECTED";
  send_to_account(it->second.acct,
                  json::object{{"type", "exec"}, {"cid", cid}, {"symbol", it->second.symbol},
                               {"exec_type", "REJECTED"}, {"status", it->second.status},
                               {"code", code}, {"reason", msg}});
}

void Hub::on_cancel_reject(const std::string& cid, const std::string& msg) {
  auto it = orders_.find(cid);
  if (it == orders_.end()) return;
  send_to_account(it->second.acct,
                  json::object{{"type", "cancel_reject"}, {"cid", cid}, {"reason", msg}});
}

// ---------------------------------------------------------------- introspection

std::string Hub::health() const {
  json::object o{{"status", "ok"},
                 {"gw_id", opt_.gw_id},
                 {"config_version", cfg_ ? cfg_->version : 0},
                 {"pg_connected", pg_ && pg_->connected()},
                 {"upstream_md", up_ && up_->md_connected()},
                 {"upstream_trading", up_ && up_->trading_ready()},
                 {"sessions", sessions_.size()}};
  return json::serialize(o);
}

static long rss_kb() {
  FILE* f = std::fopen("/proc/self/status", "r");
  if (!f) return 0;
  char line[256];
  long kb = 0;
  while (std::fgets(line, sizeof line, f))
    if (std::sscanf(line, "VmRSS: %ld", &kb) == 1) break;
  std::fclose(f);
  return kb;
}

std::string Hub::metrics() const {
  std::string s;
  auto put = [&](const char* k, long long v) { s += std::format("{} {}\n", k, v); };
  put("config_version", cfg_ ? cfg_->version : 0);
  put("config_reloads", m.config_reloads);
  put("config_apply_us_p50", m.cfg_apply_us.pct(50));
  put("config_apply_us_p99", m.cfg_apply_us.pct(99));
  put("config_reload_query_us_p50", m.cfg_reload_us.pct(50));
  put("config_reload_query_us_p99", m.cfg_reload_us.pct(99));
  if (pg_) {
    put("pg_notifies", pg_->notifies);
    put("pg_reloads", pg_->reloads);
    put("pg_reconnects", pg_->reconnects);
  }
  put("sessions", (long long)sessions_.size());
  put("orders_in", m.orders_in);
  put("orders_sent", m.orders_sent);
  put("risk_rejects", m.risk_rejects);
  put("rate_rejects", m.rate_rejects);
  put("upstream_rejects", m.upstream_rejects);
  put("exec_events", m.exec_events);
  put("foreign_exec", m.foreign_exec);
  put("md_in", m.md_in);
  put("md_out", m.md_out);
  put("md_conflated", m.md_conflated);
  put("kicked", m.kicked);
  put("slow_disconnects", m.slow_disconnects);
  put("auth_failures", m.auth_failures);
  if (up_) {
    put("upstream_md_reconnects", up_->md_reconnects);
    put("upstream_trade_reconnects", up_->trade_reconnects);
  }
  put("rss_kb", rss_kb());
  return s;
}

}  // namespace tg
