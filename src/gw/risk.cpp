#include "risk.hpp"

#include "../common/util.hpp"

namespace tg {

std::string check_order(const Config& c, int acct, const OrderReq& o, int64_t ref_px) {
  if (o.side != "BUY" && o.side != "SELL") return "bad side";
  if (o.type != "LIMIT" && o.type != "MARKET") return "bad type";
  auto a = c.account(acct);
  if (!a || !a->enabled) return "account disabled";
  auto ins = c.instruments.find(o.symbol);
  if (ins == c.instruments.end()) return "unknown instrument";
  if (!ins->second.enabled) return "instrument disabled";
  auto lim = a->limits.find(o.symbol);
  if (lim == a->limits.end()) return "instrument not permitted for account";

  const Instrument& i = ins->second;
  if (o.qty <= 0) return "qty must be > 0";
  if (i.lot > 0 && o.qty % i.lot != 0) return "qty not a multiple of lot size";
  if (o.qty > lim->second.max_qty) return "qty exceeds max_qty";

  int64_t px = o.price;
  if (o.type == "LIMIT") {
    if (o.price <= 0) return "price must be > 0";
    if (i.tick > 0 && o.price % i.tick != 0) return "price not a multiple of tick size";
  } else {
    if (ref_px <= 0) return "no reference price for market order";
    px = ref_px;
  }
  // qty * px can exceed int64 (both are 1e-8 scaled), so multiply in 128 bits.
  __int128 notional = static_cast<__int128>(o.qty) * px / kScale;
  if (notional > lim->second.max_notional) return "notional exceeds max_notional";
  return "";
}

}  // namespace tg
