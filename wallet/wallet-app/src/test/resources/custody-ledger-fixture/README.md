# Native custody ledger fixture

This test-only fixture runs the production `CustodyLedgerService`, wallet managers,
`TransferManager`, and PostgreSQL repositories behind a small JDK `HttpServer` that
defaults to `127.0.0.1`. It is not packaged in the wallet application and starts no Kafka
consumers.

The legacy wallet migrations use PostgreSQL's three-part `opex.public.*` qualifiers,
so the database must be named `opex`. Use a fresh, disposable PostgreSQL container
reserved for this fixture. Never point it at a live exchange store, `opex_test`, or a
database that contains data you need. The fixture refuses to start unless the database
name, database comment, and marker row all match these values:

```sql
COMMENT ON DATABASE opex IS 'cex_custody_ledger_fixture_20261017';
CREATE TABLE public.custody_fixture_marker (marker TEXT PRIMARY KEY);
INSERT INTO public.custody_fixture_marker (marker)
VALUES ('cex_custody_ledger_fixture_20261017');
```

For example, create a disposable container and add the marker:

```sh
docker run -d --name cex-custody-ledger-tests-20261017 \
  -e POSTGRES_DB=opex -e POSTGRES_USER=opex \
  -e POSTGRES_PASSWORD="$CUSTODY_FIXTURE_DB_PASSWORD" postgres:16.11-alpine
docker exec -i cex-custody-ledger-tests-20261017 psql -U opex -d opex -v ON_ERROR_STOP=1 <<'SQL'
COMMENT ON DATABASE opex IS 'cex_custody_ledger_fixture_20261017';
CREATE TABLE public.custody_fixture_marker (marker TEXT PRIMARY KEY);
INSERT INTO public.custody_fixture_marker (marker)
VALUES ('cex_custody_ledger_fixture_20261017');
SQL
```

The harness applies the wallet Flyway migrations and records a one-time test opening
state of `100 LOCAL_EVM_ETH` by default for its dedicated treasury owner. Set
`OPEX_CUSTODY_FIXTURE_OPENING_TREASURY_AMOUNT` to a positive decimal before first
initialization to choose a different fixture amount. The persisted opening-state row
includes that amount, and startup refuses to continue if a later run requests a
different amount. The opening-state row
records the fixture ID, owner, asset, amount, and provenance. It is not a deposit or
user-fund credit. Restarts never replenish the balance; if recording the opening state
is interrupted, startup stops for manual review. The fixture user starts with a
zero-balance MAIN wallet, which the harness never resets.

Compile the harness with Java 17 and Maven, then launch it:

```sh
mvn -q -pl wallet/wallet-app -am -Dskip.unit.tests=true -DincludeScope=test \
  -Dmdep.outputFile="$(pwd)/wallet/wallet-app/target/custody-ledger-fixture.classpath" \
  test-compile dependency:build-classpath
java -cp "wallet/wallet-app/target/classes:wallet/wallet-app/target/test-classes:$(cat wallet/wallet-app/target/custody-ledger-fixture.classpath)" \
  co.nilin.opex.wallet.app.CustodyLedgerFixtureMainKt
```

Set `OPEX_CUSTODY_FIXTURE_ENABLED=true`, `OPEX_TEST_POSTGRES` to a reachable R2DBC URL
for the marked `opex` database, and `OPEX_CUSTODY_FIXTURE_LEDGER_KEY` to the key expected
in `X-Custody-Key`. `OPEX_CUSTODY_FIXTURE_PORT` defaults to `18447`. The server binds
to `127.0.0.1` by default; set `OPEX_CUSTODY_FIXTURE_BIND=0.0.0.0` only when the
container's published port is restricted to host loopback.
The HTTP adapter exposes the production custody controller, including its
insufficient-funds response. Its finite opening balance is consumed by actions and
never replenished. To repeat a scenario from a clean state, create a new disposable
PostgreSQL container; the harness has no database-reset operation.
