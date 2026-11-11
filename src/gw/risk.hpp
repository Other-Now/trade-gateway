// Pre-trade checks. Pure functions over a Config snapshot so they unit-test
// without a database or a socket.
#pragma once
#include <algorithm>
#include <string>

#include "config.hpp"

namespace tg {

struct OrderReq {
  std::string symbol, side, type = "LIMIT", tif = "GTC";
  int64_t qty = 0, price = 0;  // 1e-8 units; price unused for MARKET
};

// Returns "" if the order may go upstream, otherwise the reject reason.
// ref_px is the touch on the side the order would hit (ask for BUY, bid for
// SELL); it prices MARKET orders for the notional check.
std::string check_order(const Config& c, int acct, const OrderReq& o, int64_t ref_px);

// Per-account order rate limit. Lives in the gateway process, so with N
// gateways an account can get up to N x the configured rate (see NOTES).
struct TokenBucket {
  double tokens = -1;
  int64_t last_us = 0;
  bool take(double rate, int64_t now) {
    if (tokens < 0) tokens = rate;  // first use: full bucket
    else tokens = std::min(rate, tokens + double(now - last_us) * rate / 1e6);
    last_us = now;
    if (tokens >= 1) {
      tokens -= 1;
      return true;
    }
    return false;
  }
};

}  // namespace tg
