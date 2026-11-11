// One place for the Boost includes and namespace aliases.
#pragma once
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/beast.hpp>
#include <boost/beast/ssl.hpp>
#include <boost/beast/websocket/ssl.hpp>
#include <boost/json.hpp>

#include <deque>
#include <memory>
#include <string>

namespace tg {
namespace net = boost::asio;
namespace beast = boost::beast;
namespace http = beast::http;
namespace websocket = beast::websocket;
namespace json = boost::json;
namespace ssl = net::ssl;
using tcp = net::ip::tcp;
using net::awaitable;
using net::use_awaitable;

inline std::string jstr(const json::object& o, std::string_view k) {
  if (auto v = o.if_contains(k)) {
    if (auto s = v->if_string()) return std::string(*s);
    if (v->is_int64()) return std::to_string(v->get_int64());
    if (v->is_uint64()) return std::to_string(v->get_uint64());
  }
  return {};
}

inline int64_t jint(const json::object& o, std::string_view k, int64_t d = 0) {
  if (auto v = o.if_contains(k)) {
    if (v->is_int64()) return v->get_int64();
    if (v->is_uint64()) return static_cast<int64_t>(v->get_uint64());
    if (auto s = v->if_string()) {
      try {
        return std::stoll(std::string(*s));
      } catch (...) {
      }
    }
  }
  return d;
}

// Serialises writes on one websocket. Beast allows only one async_write in
// flight per stream, so every producer pushes here and a single coroutine
// drains the queue. Single-threaded io_context, so no locks.
class Outbox {
 public:
  explicit Outbox(net::any_io_executor ex) : sig_(ex) {}
  void push(std::string s) {
    q_.push_back(std::move(s));
    sig_.cancel();
  }
  void stop() {
    stopped_ = true;
    sig_.cancel();
  }
  size_t size() const { return q_.size(); }

  // Static, taking shared_ptrs by value: the coroutine frame owns both, so a
  // connection can be torn down while a write is still pending.
  template <class WS>
  static awaitable<void> run(std::shared_ptr<Outbox> self, std::shared_ptr<WS> ws) {
    try {
      while (!self->stopped_) {
        if (self->q_.empty()) {
          self->sig_.expires_at(net::steady_timer::time_point::max());
          boost::system::error_code ec;
          co_await self->sig_.async_wait(net::redirect_error(use_awaitable, ec));
          continue;
        }
        std::string s = std::move(self->q_.front());
        self->q_.pop_front();
        co_await ws->async_write(net::buffer(s), use_awaitable);
      }
    } catch (const std::exception&) {
    }
  }

 private:
  std::deque<std::string> q_;
  net::steady_timer sig_;
  bool stopped_ = false;
};

}  // namespace tg
