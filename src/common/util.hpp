// Small shared helpers: fixed-point decimals, hashing, URL parsing, latency samples.
#pragma once
#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace tg {

// Wall clock in microseconds. Used for cross-process latency (gateway, bench and
// Postgres all run on one host, so they share this clock).
int64_t now_us();
inline int64_t now_ms() { return now_us() / 1000; }

// Prices and quantities are int64 in units of 1e-8, the precision Binance uses.
// No floating point anywhere near a tick-size or notional check.
constexpr int64_t kScale = 100'000'000;
std::optional<int64_t> parse_dec(std::string_view s);  // "0.001" -> 100000
std::string fmt_dec(int64_t v);                        // 100000 -> "0.001"

std::string sha256_hex(std::string_view in);
std::string hmac_sha256_hex(std::string_view key, std::string_view msg);

struct Url {
  bool tls = false;
  std::string host, port, target;
};
std::optional<Url> parse_url(std::string_view s);  // ws[s]://host[:port]/path

// Client order ids look like "tg-<account>-<gw><seq>". The account is encoded in
// the id because Binance only knows ONE account (the gateway's); this is how an
// execution report finds its way back to the right client.
std::optional<int> account_from_cid(std::string_view cid);

struct Samples {
  std::vector<int64_t> v;
  size_t cap = 200000;
  void add(int64_t x) {
    if (v.size() < cap) v.push_back(x);
  }
  int64_t pct(double p) const {
    if (v.empty()) return 0;
    auto c = v;
    size_t i = std::min(c.size() - 1, static_cast<size_t>(p / 100.0 * c.size()));
    std::nth_element(c.begin(), c.begin() + i, c.end());
    return c[i];
  }
  int64_t max() const { return v.empty() ? 0 : *std::max_element(v.begin(), v.end()); }
};

}  // namespace tg
