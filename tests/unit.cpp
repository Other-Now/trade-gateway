// Unit tests for the pure parts: decimals, signing, URL/cid parsing, risk
// checks, auth rules, rate limiter. No framework, just CHECK.
#include <cstdio>

#include "../src/common/util.hpp"
#include "../src/gw/config.hpp"
#include "../src/gw/risk.hpp"
#include "../src/gw/upstream.hpp"

using namespace tg;

static int fails = 0, checks = 0;
#define CHECK(c)                                                    \
  do {                                                              \
    ++checks;                                                       \
    if (!(c)) {                                                     \
      ++fails;                                                      \
      std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #c);      \
    }                                                               \
  } while (0)

static Config sample() {
  Config c;
  c.version = 7;
  c.instruments["BTCUSDT"] = {"BTCUSDT", true, *parse_dec("0.01"), *parse_dec("0.00001")};
  c.instruments["ETHUSDT"] = {"ETHUSDT", false, *parse_dec("0.01"), *parse_dec("0.0001")};
  Account a{1, "alice", true, 5, {}};
  a.limits["BTCUSDT"] = {*parse_dec("2"), *parse_dec("100000")};
  a.limits["ETHUSDT"] = {*parse_dec("10"), *parse_dec("100000")};
  c.accounts[1] = a;
  c.accounts[2] = Account{2, "bob", false, 5, {}};
  c.keys[sha256_hex("alice-key")] = 1;
  c.keys[sha256_hex("bob-key")] = 2;
  return c;
}

int main() {
  // decimals
  CHECK(parse_dec("0.001") == 100000);
  CHECK(parse_dec("1") == kScale);
  CHECK(parse_dec("60000.12") == 6000012000000LL);
  CHECK(parse_dec("0.123456789") == std::nullopt);      // 9 dp: too precise
  CHECK(parse_dec("0.123456780") == 12345678);          // trailing zero is fine
  CHECK(parse_dec("-1") == std::nullopt);
  CHECK(parse_dec("1e5") == std::nullopt);
  CHECK(parse_dec("") == std::nullopt);
  CHECK(parse_dec(".") == std::nullopt);
  CHECK(parse_dec("1.2.3") == std::nullopt);
  CHECK(parse_dec("99999999999") == std::nullopt);      // 11 integer digits would overflow
  CHECK(parse_dec("9999999999.99999999").has_value());
  CHECK(fmt_dec(100000) == "0.001");
  CHECK(fmt_dec(6000012000000LL) == "60000.12");
  CHECK(fmt_dec(kScale) == "1");
  CHECK(fmt_dec(-150000000) == "-1.5");
  CHECK(fmt_dec(*parse_dec("123.45678901")) == "123.45678901");

  // hashing: FIPS 180-2 "abc" vector, and Binance's documented HMAC example
  CHECK(sha256_hex("abc") == "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
  CHECK(hmac_sha256_hex("NhqPtmdSJYdKjVHjA7PZj4Mge3R5YNiP1e3UZjInClVN65XAbvqqM6A7H5fATj0j",
                        "symbol=LTCBTC&side=BUY&type=LIMIT&timeInForce=GTC&quantity=1&price=0.1&"
                        "recvWindow=5000&timestamp=1499827319559") ==
        "c8db56825ae71d6d79447849e617115f4a920fa2acdcab2b053c4b2838bd6b71");
  CHECK(Upstream::signing_payload({{"timestamp", "1"}, {"apiKey", "k"}, {"symbol", "BTCUSDT"}}) ==
        "apiKey=k&symbol=BTCUSDT&timestamp=1");

  // urls and client order ids
  auto u = parse_url("wss://stream.testnet.binance.vision/ws");
  CHECK(u && u->tls && u->host == "stream.testnet.binance.vision" && u->port == "443" && u->target == "/ws");
  auto u2 = parse_url("wss://127.0.0.1:9443/ws-api/v3");
  CHECK(u2 && u2->port == "9443" && u2->target == "/ws-api/v3");
  CHECK(!parse_url("https://x/"));
  CHECK(account_from_cid("tg-42-a1731000000000") == 42);
  CHECK(account_from_cid("tg-x-a1") == std::nullopt);
  CHECK(account_from_cid("web_8f3a2") == std::nullopt);
  CHECK(account_from_cid("tg-7") == std::nullopt);

  // auth: unrevoked key + enabled account
  Config c = sample();
  CHECK(c.auth(sha256_hex("alice-key")) == 1);
  CHECK(c.auth(sha256_hex("bob-key")) == std::nullopt);  // account disabled
  CHECK(c.auth(sha256_hex("nobody")) == std::nullopt);
  CHECK(c.permitted(1, "BTCUSDT"));
  CHECK(!c.permitted(1, "ETHUSDT"));  // instrument disabled
  CHECK(!c.permitted(2, "BTCUSDT"));
  CHECK(c.md_symbols() == std::set<std::string>{"BTCUSDT"});

  // risk
  OrderReq o{"BTCUSDT", "BUY", "LIMIT", "GTC", *parse_dec("0.5"), *parse_dec("60000.01")};
  CHECK(check_order(c, 1, o, 0) == "");
  auto r = [&](auto f, int64_t ref = 0, int acct = 1) {
    OrderReq x = o;
    f(x);
    return check_order(c, acct, x, ref);
  };
  CHECK(r([](OrderReq& x) { x.side = "HOLD"; }) == "bad side");
  CHECK(r([](OrderReq& x) { x.symbol = "DOGEUSDT"; }) == "unknown instrument");
  CHECK(r([](OrderReq& x) { x.symbol = "ETHUSDT"; }) == "instrument disabled");
  CHECK(r([](OrderReq&) {}, 0, 2) == "account disabled");
  CHECK(r([](OrderReq& x) { x.qty = *parse_dec("0.000015"); }) == "qty not a multiple of lot size");
  CHECK(r([](OrderReq& x) { x.price = *parse_dec("60000.005"); }) == "price not a multiple of tick size");
  CHECK(r([](OrderReq& x) { x.qty = *parse_dec("2.00001"); }) == "qty exceeds max_qty");
  CHECK(r([](OrderReq& x) { x.qty = *parse_dec("2"); }) == "notional exceeds max_notional");  // 120k > 100k
  CHECK(r([](OrderReq& x) { x.qty = 0; }) == "qty must be > 0");
  CHECK(r([](OrderReq& x) { x.type = "MARKET"; }) == "no reference price for market order");
  CHECK(r([](OrderReq& x) { x.type = "MARKET"; x.price = 0; }, *parse_dec("60000")) == "");
  CHECK(r([](OrderReq& x) { x.type = "MARKET"; x.qty = *parse_dec("1.7"); }, *parse_dec("60000")) ==
        "notional exceeds max_notional");
  // a notional that would overflow int64 in 1e-8 units must still be caught
  c.accounts[1].limits["BTCUSDT"].max_qty = *parse_dec("9999999999");
  CHECK(r([](OrderReq& x) { x.qty = *parse_dec("9999999"); x.price = *parse_dec("9999999"); }) ==
        "notional exceeds max_notional");

  // rate limiter: burst of `rate`, then refill at `rate`/s
  TokenBucket b;
  int ok = 0;
  for (int i = 0; i < 10; i++) ok += b.take(5, 1'000'000);
  CHECK(ok == 5);
  CHECK(!b.take(5, 1'100'000));  // +0.5 token
  CHECK(b.take(5, 1'300'000));   // +1.5 total
  CHECK(b.take(5, 3'000'000) && b.take(5, 3'000'000));

  std::printf("%d checks, %d failures\n", checks, fails);
  return fails ? 1 : 0;
}
