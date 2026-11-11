#include "pgwatch.hpp"

#include <unistd.h>

#include <cstdio>
#include <stdexcept>

#include "../common/util.hpp"

namespace tg {

// One REPEATABLE READ transaction, so the five SELECTs see the same snapshot
// and the version we record is exactly the version of what we loaded.
static const char* kLoadSql =
    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY;"
    "SELECT version FROM config_version WHERE id = 1;"
    "SELECT symbol, enabled, tick_size::text, lot_size::text FROM instruments;"
    "SELECT id, name, enabled, max_orders_per_sec FROM accounts;"
    "SELECT account_id, symbol, max_qty::text, max_notional::text FROM account_limits;"
    "SELECT key_hash, account_id FROM api_keys WHERE NOT revoked;"
    "COMMIT;";

static int64_t dec(PGresult* r, int row, int col) {
  return parse_dec(PQgetvalue(r, row, col)).value_or(0);
}
static bool boolean(PGresult* r, int row, int col) { return PQgetvalue(r, row, col)[0] == 't'; }

std::shared_ptr<Config> build_config(PGresult* ver, PGresult* inst, PGresult* acc,
                                     PGresult* lim, PGresult* keys) {
  auto c = std::make_shared<Config>();
  if (PQntuples(ver) == 1) c->version = std::stoll(PQgetvalue(ver, 0, 0));
  for (int i = 0; i < PQntuples(inst); i++) {
    Instrument in{PQgetvalue(inst, i, 0), boolean(inst, i, 1), dec(inst, i, 2), dec(inst, i, 3)};
    c->instruments[in.symbol] = in;
  }
  for (int i = 0; i < PQntuples(acc); i++) {
    Account a;
    a.id = std::stoi(PQgetvalue(acc, i, 0));
    a.name = PQgetvalue(acc, i, 1);
    a.enabled = boolean(acc, i, 2);
    a.max_orders_per_sec = std::stoi(PQgetvalue(acc, i, 3));
    c->accounts[a.id] = a;
  }
  for (int i = 0; i < PQntuples(lim); i++) {
    int id = std::stoi(PQgetvalue(lim, i, 0));
    auto it = c->accounts.find(id);
    if (it != c->accounts.end())
      it->second.limits[PQgetvalue(lim, i, 1)] = Limit{dec(lim, i, 2), dec(lim, i, 3)};
  }
  for (int i = 0; i < PQntuples(keys); i++)
    c->keys[PQgetvalue(keys, i, 0)] = std::stoi(PQgetvalue(keys, i, 1));
  return c;
}

PgWatcher::PgWatcher(net::any_io_executor ex, std::string conninfo, Callback cb, bool resync)
    : ex_(ex), conninfo_(std::move(conninfo)), cb_(std::move(cb)), resync_(resync) {}

PgWatcher::~PgWatcher() { close(); }

void PgWatcher::start() { net::co_spawn(ex_, run(), net::detached); }

void PgWatcher::close() {
  sock_.reset();  // closes our dup() of the socket, not libpq's
  if (conn_) PQfinish(conn_);
  conn_ = nullptr;
  connected_ = false;
}

awaitable<void> PgWatcher::run() {
  net::steady_timer t(ex_);
  int backoff_ms = 100;
  for (;;) {
    try {
      co_await session();
    } catch (const std::exception& e) {
      std::fprintf(stderr, "[pg] connection lost: %s", e.what());
    }
    close();
    ++reconnects;
    t.expires_after(std::chrono::milliseconds(backoff_ms));
    co_await t.async_wait(use_awaitable);
    backoff_ms = std::min(backoff_ms * 2, 2000);
    if (loaded_once_) backoff_ms = std::min(backoff_ms, 400);
  }
}

awaitable<void> PgWatcher::session() {
  // Connect is blocking but local and rare; connect_timeout bounds it.
  conn_ = PQconnectdb(conninfo_.c_str());
  if (PQstatus(conn_) != CONNECTION_OK) throw std::runtime_error(PQerrorMessage(conn_));
  PQsetnonblocking(conn_, 1);
  // dup() so the descriptor and libpq each own a fd: PQfinish closes libpq's,
  // the stream_descriptor closes ours. Without it we would double-close.
  sock_.emplace(ex_, ::dup(PQsocket(conn_)));

  // LISTEN *before* loading. If we loaded first, a change committed between
  // the SELECT and the LISTEN would be in neither.
  co_await exec("LISTEN tg_config");
  connected_ = true;
  std::fprintf(stderr, "[pg] connected, listening (backend pid %d)\n", PQbackendPID(conn_));

  if (!loaded_once_) co_await reload("initial", 0);
  else if (resync_) co_await reload("resync", 0);
  loaded_once_ = true;

  for (;;) {
    // Drain everything libpq has buffered (some may have arrived during a
    // reload). Many NOTIFYs collapse into one reload: we only need to know the
    // highest version, because a reload reads the whole current snapshot.
    int64_t max_v = 0, ts = 0;
    while (PGnotify* n = PQnotifies(conn_)) {
      ++notifies;
      boost::system::error_code ec;
      auto v = json::parse(n->extra, ec);
      if (!ec && v.is_object()) {
        int64_t ver = jint(v.as_object(), "v");
        if (ver > max_v) max_v = ver, ts = jint(v.as_object(), "ts");
      }
      PQfreemem(n);
    }
    if (max_v > applied_) {
      co_await reload("notify", ts);
      continue;
    }
    co_await sock_->async_wait(net::posix::stream_descriptor::wait_read, use_awaitable);
    if (!PQconsumeInput(conn_) || PQstatus(conn_) != CONNECTION_OK)
      throw std::runtime_error(PQerrorMessage(conn_));
  }
}

awaitable<std::vector<PgWatcher::Res>> PgWatcher::exec(const char* sql) {
  if (!PQsendQuery(conn_, sql)) throw std::runtime_error(PQerrorMessage(conn_));
  for (;;) {
    int f = PQflush(conn_);
    if (f == 0) break;
    if (f < 0) throw std::runtime_error(PQerrorMessage(conn_));
    co_await sock_->async_wait(net::posix::stream_descriptor::wait_write, use_awaitable);
  }
  std::vector<Res> out;
  std::string err;
  for (;;) {
    while (PQisBusy(conn_)) {
      co_await sock_->async_wait(net::posix::stream_descriptor::wait_read, use_awaitable);
      if (!PQconsumeInput(conn_)) throw std::runtime_error(PQerrorMessage(conn_));
    }
    PGresult* r = PQgetResult(conn_);
    if (!r) break;
    auto st = PQresultStatus(r);
    if (st != PGRES_COMMAND_OK && st != PGRES_TUPLES_OK && err.empty())
      err = PQresultErrorMessage(r);
    out.emplace_back(r, &PQclear);
  }
  if (!err.empty()) throw std::runtime_error(err);
  co_return out;
}

awaitable<void> PgWatcher::reload(const char* cause, int64_t notify_ts) {
  auto t0 = std::chrono::steady_clock::now();
  auto rs = co_await exec(kLoadSql);
  if (rs.size() != 7) throw std::runtime_error("load query: unexpected result count");
  auto cfg = build_config(rs[1].get(), rs[2].get(), rs[3].get(), rs[4].get(), rs[5].get());
  ReloadInfo info;
  info.version = cfg->version;
  info.notify_ts_us = notify_ts;
  info.reload_us = std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::steady_clock::now() - t0)
                       .count();
  info.cause = cause;
  applied_ = cfg->version;
  ++reloads;
  cb_(std::move(cfg), info);
}

}  // namespace tg
