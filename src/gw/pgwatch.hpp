// Live configuration from Postgres: LISTEN tg_config + snapshot reload, driven
// by the same Asio loop as everything else (libpq in non-blocking mode, its
// socket wrapped in a posix::stream_descriptor). No extra thread.
#pragma once
#include <libpq-fe.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "../common/net.hpp"
#include "config.hpp"

namespace tg {

struct ReloadInfo {
  int64_t version = 0;
  int64_t notify_ts_us = 0;  // trigger's clock_timestamp(); 0 if not caused by a NOTIFY
  int64_t reload_us = 0;     // time spent running the snapshot query
  const char* cause = "";    // "initial" | "notify" | "resync"
};

class PgWatcher {
 public:
  using Callback = std::function<void(std::shared_ptr<const Config>, const ReloadInfo&)>;

  // resync=false is the deliberately broken control: after a reconnect it only
  // re-LISTENs and does not reload, so any change committed while the
  // connection was down is never seen (NOTIFY is not queued for absent listeners).
  PgWatcher(net::any_io_executor ex, std::string conninfo, Callback cb, bool resync = true);
  ~PgWatcher();
  void start();

  bool connected() const { return connected_; }
  int64_t applied_version() const { return applied_; }
  int64_t notifies = 0, reloads = 0, reconnects = 0;

 private:
  using Res = std::unique_ptr<PGresult, void (*)(PGresult*)>;
  awaitable<void> run();
  awaitable<void> session();
  awaitable<std::vector<Res>> exec(const char* sql);
  awaitable<void> reload(const char* cause, int64_t notify_ts);
  void close();

  net::any_io_executor ex_;
  std::string conninfo_;
  Callback cb_;
  bool resync_;
  PGconn* conn_ = nullptr;
  std::optional<net::posix::stream_descriptor> sock_;
  bool connected_ = false, loaded_once_ = false;
  int64_t applied_ = 0;
};

// Exposed for tests: builds a snapshot from the five result sets of the load query.
std::shared_ptr<Config> build_config(PGresult* ver, PGresult* inst, PGresult* acc,
                                     PGresult* lim, PGresult* keys);

}  // namespace tg
