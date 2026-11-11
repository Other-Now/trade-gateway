// Immutable configuration snapshot loaded from Postgres. The gateway holds a
// shared_ptr<const Config>; a reload builds a whole new snapshot and swaps the
// pointer, so a request never sees half of one config and half of another.
#pragma once
#include <cstdint>
#include <map>
#include <optional>
#include <set>
#include <string>
#include <unordered_map>

namespace tg {

struct Instrument {
  std::string symbol;
  bool enabled = true;
  int64_t tick = 0, lot = 0;  // 1e-8 units
};

struct Limit {
  int64_t max_qty = 0, max_notional = 0;
};

struct Account {
  int id = 0;
  std::string name;
  bool enabled = true;
  int max_orders_per_sec = 10;
  std::map<std::string, Limit> limits;  // a row here == permission to trade/see the symbol
};

struct Config {
  int64_t version = 0;
  std::map<std::string, Instrument> instruments;
  std::map<int, Account> accounts;
  std::unordered_map<std::string, int> keys;  // sha256(api key) -> account, unrevoked only

  const Account* account(int id) const {
    auto it = accounts.find(id);
    return it == accounts.end() ? nullptr : &it->second;
  }

  // A key authenticates only if it is unrevoked AND its account is enabled.
  std::optional<int> auth(const std::string& key_hash) const {
    auto it = keys.find(key_hash);
    if (it == keys.end()) return std::nullopt;
    auto a = account(it->second);
    if (!a || !a->enabled) return std::nullopt;
    return a->id;
  }

  bool permitted(int acct, const std::string& sym) const {
    auto a = account(acct);
    if (!a || !a->enabled) return false;
    auto i = instruments.find(sym);
    if (i == instruments.end() || !i->second.enabled) return false;
    return a->limits.count(sym) > 0;
  }

  std::set<std::string> md_symbols() const {
    std::set<std::string> s;
    for (auto& [sym, i] : instruments)
      if (i.enabled) s.insert(sym);
    return s;
  }
};

}  // namespace tg
