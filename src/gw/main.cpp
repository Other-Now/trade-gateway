// tgw: the client gateway.
//
//   tgw --port 9001 --id a --pg "host=127.0.0.1 port=5544 user=tg dbname=tg"
//       [--md-url wss://...] [--api-url wss://...] [--insecure-upstream]
//       [--no-resync] [--no-conflate] [--max-queue 10000]
//   TG_API_KEY / TG_API_SECRET in the environment enable trading upstream.
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <map>

#include "hub.hpp"
#include "pgwatch.hpp"
#include "session.hpp"
#include "upstream.hpp"

using namespace tg;

int main(int argc, char** argv) {
  std::map<std::string, std::string> a;
  for (int i = 1; i < argc; i++) {
    std::string k = argv[i];
    if (!k.starts_with("--")) continue;
    k = k.substr(2);
    if (i + 1 < argc && std::string_view(argv[i + 1]).substr(0, 2) != "--") a[k] = argv[++i];
    else a[k] = "1";
  }
  auto get = [&](const char* k, std::string d) { return a.count(k) ? a[k] : d; };

  HubOptions ho;
  ho.gw_id = get("id", "a");
  ho.no_conflate = a.count("no-conflate");
  ho.max_queue = std::stoul(get("max-queue", "10000"));

  Upstream::Options uo;
  uo.md_url = get("md-url", uo.md_url);
  uo.api_url = get("api-url", uo.api_url);
  uo.insecure = a.count("insecure-upstream");
  if (auto k = std::getenv("TG_API_KEY")) uo.api_key = k;
  if (auto s = std::getenv("TG_API_SECRET")) uo.api_secret = s;

  std::string conninfo = get("pg", "host=127.0.0.1 port=5544 user=tg dbname=tg") +
                         " application_name=tgw-" + ho.gw_id + " connect_timeout=2";
  unsigned short port = static_cast<unsigned short>(std::stoi(get("port", "9001")));

  net::io_context ioc{1};
  Hub hub(ho);
  Upstream up(ioc, uo, hub);
  PgWatcher pg(
      ioc.get_executor(), conninfo,
      [&](std::shared_ptr<const Config> c, const ReloadInfo& i) { hub.apply_config(std::move(c), i); },
      !a.count("no-resync"));
  hub.attach(&up, &pg);

  pg.start();
  up.start();
  net::co_spawn(ioc, listen(hub, tcp::endpoint(tcp::v4(), port)), [](std::exception_ptr e) {
    if (e) std::rethrow_exception(e);
  });

  net::signal_set sigs(ioc, SIGINT, SIGTERM);
  sigs.async_wait([&](auto, auto) { ioc.stop(); });
  std::fprintf(stderr, "[tgw %s] listening on :%u  md=%s trading=%s%s%s\n", ho.gw_id.c_str(), port,
               uo.md_url.c_str(), up.trading_configured() ? uo.api_url.c_str() : "off",
               a.count("no-resync") ? "  [CONTROL: no-resync]" : "",
               ho.no_conflate ? "  [CONTROL: no-conflate]" : "");
  ioc.run();
}
