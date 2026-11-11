-- Gateway configuration. Every change to these tables bumps config_version and
-- sends one NOTIFY tg_config, so running gateways reload without a restart.
DROP TABLE IF EXISTS api_keys, account_limits, accounts, instruments, config_version CASCADE;

CREATE TABLE instruments (
  symbol     text PRIMARY KEY,
  enabled    boolean NOT NULL DEFAULT true,
  tick_size  numeric(20,8) NOT NULL CHECK (tick_size > 0),
  lot_size   numeric(20,8) NOT NULL CHECK (lot_size > 0)
);

CREATE TABLE accounts (
  id                 int PRIMARY KEY,
  name               text NOT NULL UNIQUE,
  enabled            boolean NOT NULL DEFAULT true,
  max_orders_per_sec int NOT NULL DEFAULT 10 CHECK (max_orders_per_sec > 0)
);

-- A row here is both the permission to trade/see a symbol and its limits.
CREATE TABLE account_limits (
  account_id   int  NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  symbol       text NOT NULL REFERENCES instruments(symbol) ON DELETE CASCADE,
  max_qty      numeric(20,8) NOT NULL,
  max_notional numeric(20,8) NOT NULL,
  PRIMARY KEY (account_id, symbol)
);

-- Only the SHA-256 of a key is stored; the gateway hashes what the client sends.
CREATE TABLE api_keys (
  key_hash   text PRIMARY KEY,
  account_id int NOT NULL REFERENCES accounts(id) ON DELETE CASCADE,
  label      text,
  revoked    boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE config_version (
  id      int PRIMARY KEY DEFAULT 1 CHECK (id = 1),
  version bigint NOT NULL DEFAULT 0
);
INSERT INTO config_version VALUES (1, 0);

-- Statement-level: an UPDATE touching 500 rows is one bump and one NOTIFY.
-- The version row is locked by the UPDATE, so concurrent writers serialise
-- here and commit order == version order. NOTIFY is delivered at COMMIT.
-- ts is the trigger's clock (before commit), used to measure propagation.
CREATE OR REPLACE FUNCTION tg_bump_config() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE v bigint;
BEGIN
  UPDATE config_version SET version = version + 1 WHERE id = 1 RETURNING version INTO v;
  PERFORM pg_notify('tg_config', json_build_object(
      'v', v, 'table', TG_TABLE_NAME,
      'ts', (extract(epoch FROM clock_timestamp()) * 1000000)::bigint)::text);
  RETURN NULL;
END $$;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['instruments', 'accounts', 'account_limits', 'api_keys'] LOOP
    EXECUTE format('CREATE TRIGGER %I_cfg AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON %I
                    FOR EACH STATEMENT EXECUTE FUNCTION tg_bump_config()', t, t);
  END LOOP;
END $$;
