-- Demo/bench configuration. Plain-text keys appear only here, for the demo;
-- the table stores sha256(key).
INSERT INTO instruments(symbol, tick_size, lot_size) VALUES
  ('BTCUSDT', 0.01, 0.00001),
  ('ETHUSDT', 0.01, 0.0001),
  ('BNBUSDT', 0.01, 0.001);

INSERT INTO accounts(id, name, max_orders_per_sec) VALUES
  (1, 'alice', 5000),
  (2, 'bob',   5000),
  (3, 'carol', 5000),
  (4, 'dave',  5000),
  (5, 'erin',  5000),
  (6, 'trickle', 5);   -- low rate limit, used by tests

-- bob may only trade BTCUSDT; everyone else gets BTC and ETH.
INSERT INTO account_limits(account_id, symbol, max_qty, max_notional)
SELECT a, s, 100, 10000000 FROM generate_series(1, 6) a, unnest(ARRAY['BTCUSDT', 'ETHUSDT']) s
WHERE NOT (a = 2 AND s = 'ETHUSDT');

INSERT INTO api_keys(key_hash, account_id, label)
SELECT encode(sha256(convert_to(name || '-key', 'UTF8')), 'hex'), id, 'seed' FROM accounts;
