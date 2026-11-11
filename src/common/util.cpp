#include "util.hpp"

#include <openssl/evp.h>
#include <openssl/hmac.h>

#include <cstdio>
#include <ctime>

namespace tg {

int64_t now_us() {
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return int64_t(ts.tv_sec) * 1'000'000 + ts.tv_nsec / 1000;
}

std::optional<int64_t> parse_dec(std::string_view s) {
  if (s.empty() || s.size() > 32) return std::nullopt;
  int64_t ip = 0, fp = 0;
  int idigits = 0, fdigits = 0;
  bool dot = false, any = false;
  for (char c : s) {
    if (c == '.') {
      if (dot) return std::nullopt;
      dot = true;
      continue;
    }
    if (c < '0' || c > '9') return std::nullopt;
    any = true;
    if (!dot) {
      if (++idigits > 10) return std::nullopt;  // keeps ip * kScale < 2^63
      ip = ip * 10 + (c - '0');
    } else {
      if (fdigits == 8) {
        if (c != '0') return std::nullopt;  // more precision than we can hold
        continue;
      }
      fp = fp * 10 + (c - '0');
      ++fdigits;
    }
  }
  if (!any) return std::nullopt;
  while (fdigits < 8) fp *= 10, ++fdigits;
  return ip * kScale + fp;
}

std::string fmt_dec(int64_t v) {
  bool neg = v < 0;
  uint64_t u = neg ? uint64_t(0) - uint64_t(v) : uint64_t(v);
  std::string s = std::to_string(u / kScale);
  uint64_t f = u % kScale;
  if (f) {
    char b[16];
    std::snprintf(b, sizeof b, "%08llu", static_cast<unsigned long long>(f));
    std::string fs(b);
    while (fs.back() == '0') fs.pop_back();
    s += "." + fs;
  }
  return neg ? "-" + s : s;
}

static std::string hex(const unsigned char* p, size_t n) {
  static const char* d = "0123456789abcdef";
  std::string s(n * 2, '0');
  for (size_t i = 0; i < n; i++) {
    s[2 * i] = d[p[i] >> 4];
    s[2 * i + 1] = d[p[i] & 15];
  }
  return s;
}

std::string sha256_hex(std::string_view in) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int n = 0;
  EVP_Digest(in.data(), in.size(), md, &n, EVP_sha256(), nullptr);
  return hex(md, n);
}

std::string hmac_sha256_hex(std::string_view key, std::string_view msg) {
  unsigned char md[EVP_MAX_MD_SIZE];
  unsigned int n = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(msg.data()), msg.size(), md, &n);
  return hex(md, n);
}

std::optional<Url> parse_url(std::string_view s) {
  Url u;
  if (s.starts_with("wss://")) {
    u.tls = true;
    s.remove_prefix(6);
  } else if (s.starts_with("ws://")) {
    s.remove_prefix(5);
  } else {
    return std::nullopt;
  }
  auto slash = s.find('/');
  std::string_view hostport = s.substr(0, slash);
  u.target = slash == std::string_view::npos ? "/" : std::string(s.substr(slash));
  auto colon = hostport.find(':');
  if (colon == std::string_view::npos) {
    u.host = std::string(hostport);
    u.port = u.tls ? "443" : "80";
  } else {
    u.host = std::string(hostport.substr(0, colon));
    u.port = std::string(hostport.substr(colon + 1));
  }
  if (u.host.empty()) return std::nullopt;
  return u;
}

std::optional<int> account_from_cid(std::string_view cid) {
  if (!cid.starts_with("tg-")) return std::nullopt;
  cid.remove_prefix(3);
  int acct = 0, digits = 0;
  while (!cid.empty() && cid.front() >= '0' && cid.front() <= '9') {
    if (++digits > 9) return std::nullopt;
    acct = acct * 10 + (cid.front() - '0');
    cid.remove_prefix(1);
  }
  if (digits == 0 || cid.empty() || cid.front() != '-') return std::nullopt;
  return acct;
}

}  // namespace tg
