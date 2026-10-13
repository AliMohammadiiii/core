# Source build and verification

The repository uses Maven. The implementation plan's Gradle commands do not apply. API overrides Kotlin2.1.0/SpringBoot3.4.2 in its own POM; the other selected services inherit Kotlin1.9.0/SpringBoot2.7.6. The actual reactor compiled successfully with the versions declared by source.

Build with Java17+ and Maven3.9.9, one reactor at a time. `scripts/build-order-foundation.sh package` builds API, matching gateway, matching engine, accountant, market and eventlog with the no-vault source profile. `scripts/build-order-foundation.sh test` runs the affected broad suites; set `OPEX_TEST_POSTGRES` to an isolated test PostgreSQL URL to enable durable-intent database tests. Never aim tests at the original demo database. Example test-only URL: `r2dbc:postgresql://opex_test:opex_test@localhost:18033/opex_test`.

`OPEX_MAVEN_BIN`, `OPEX_MAVEN_REPO` and `OPEX_MAVEN_SETTINGS` can select task-local tools/cache/settings. The tested native tools were Corretto17.0.20.1 and ApacheMaven3.9.9. Native JVM heaps were limited to768MiB, tests384MiB, and two active CPUs. The Maven ARM container was3.9.9-eclipse-temurin-17 manifest sha256:f58d59b6273e785ac0a4477f6e9b5ba1d7731c75b906c0f7b34076f1851318cc.

The nilin Maven utility repository returned404. The task-local settings mirror nilin to MavenCentral for normal public dependencies. Exact external utility jars and embedded POMs were recovered from the published API image `ghcr.io/opexdev/api@sha256:f73c0c17f6ac99a16ee752596feb7694c24af6004aa0b87a775c1fbec7c87d56`, then installed in the isolated cache with `mvn install:install-file -Dfile=... -DpomFile=...`. No artifact was relabeled to a different version. These are external dependencies, not replacements for compiled service source.

Recovered utility jar SHA256 values:

- error-handler1.2.27: `8f31e9dc7eaf417502a984503088807ab7b8407c75a0ddcc743bbb9765733a77`
- interceptors1.0.8: `4e2cfae6b2c0dbfa6b96098a30105f30ad4aea6d26c3d6f195ec360957c721d8`

Output paths are `api/api-app/target/api-app.jar`, `matching-gateway/matching-gateway-app/target/matching-gateway-app.jar`, `matching-engine/matching-engine-app/target/matching-engine-app.jar`, `accountant/accountant-app/target/accountant-app.jar`, `market/market-app/target/market-app.jar`, and `eventlog/eventlog-app/target/eventlog-app.jar`. Deploy all readers of the changed matching protocol together; an old eventlog RejectReason enum cannot decode new rejection names.

The new schemas are additive in each port's existing schema.sql initializer. Gateway adds intent/outbox/cursor tables; accountant adds admission and inbox tables plus nullable client ID and post-only columns/index. Existing order or ledger rows are not rewritten. Before using a fresh source runtime, retain the actual source commit and jar/image hashes, preserve the original demo snapshot, use separate database/Redis/Kafka state, and verify the full submit/reserve/match/settle/cancel/restart loop. Source/test success alone is not proof of a deployed jar.

Review round 1 adds the command-publication, admission-progress, and concurrent cursor regressions to `scripts/build-order-foundation.sh test`. At review round 1, with the isolated PostgreSQL URL set, the selected suites contained 78 tests; omitting it skips the database-backed evidence. Run only one Maven reactor at a time. Round 1 was verified with a 512 MiB Maven heap, a 256 MiB test heap, and two active CPUs.

Package only the three services changed by this review round:

```sh
mvn -B -Pno-vault \
  -pl matching-engine/matching-engine-app,accountant/accountant-app,matching-gateway/matching-gateway-app \
  -am -Dskip.unit.tests=true -Dmaven.test.skip=true package
```

The accountant schema initializer applies the additive migration `ALTER TABLE order_admission ADD COLUMN IF NOT EXISTS reconciliation_required BOOLEAN NOT NULL DEFAULT FALSE;`. Apply it before the revised admission scheduler is enabled. The gateway cursor fix has no schema migration. Preserve the existing command offsets and Redis snapshots when restarting for replay; do not create replacement orders, reservation references, or wallet credits as a recovery step. The source regressions do not prove runtime recovery until the existing command backlog and durable intent state are checked in the deployed system.

The database-backed tests expect both service schemas to exist in the isolated test database. For the owned PostgreSQL container used in this run, initialize them before running the test script:

```sh
docker exec -i opex-order-test-postgres psql -v ON_ERROR_STOP=1 -U opex_test -d opex_test \
  < accountant/accountant-ports/accountant-persister-postgres/src/main/resources/schema.sql
docker exec -i opex-order-test-postgres psql -v ON_ERROR_STOP=1 -U opex_test -d opex_test \
  < matching-gateway/matching-gateway-port/matching-gateway-persister-postgres/src/main/resources/schema.sql
```

Use a dedicated empty test database: the admission and intent fixtures delete their test rows before each case. Set OPEX_TEST_POSTGRES to that database's R2DBC URL, then run the test script.

Review round 2 adds `UserTradeHistoryPostgresTest` to the complete affected selection (80 tests). Its real PostgreSQL fixture creates a unique schema, initializes the Market schema, and drops only that test schema afterward; no existing Market rows are changed. The two cases cover owner isolation, maker/taker and BUY/SELL attribution, distinct matching/database IDs, multiple partial fills, differing order limits and execution prices, executed notionals, commissions/assets, and unchanged timestamps. The shared legacy Trade DTO consumer also receives its actual matching order ID. API already declares and decodes orderId; this round changes only the Market source and needs no API jar or database migration.

Run the focused Market regression selection with the isolated OPEX_TEST_POSTGRES URL set:

```sh
mvn -B -pl market/market-ports/market-persister-postgres -am \
  -Dskip.unit.tests=false -Dsurefire.failIfNoSpecifiedTests=false \
  -Dtest=UserTradeHistoryPostgresTest,MarketQueryHandlerTest,TradePersisterTest,OrderPersisterTest,UserQueryHandlerTest test
```

Package only Market with `mvn -B -Pno-vault -pl market/market-app -am -Dskip.unit.tests=true -Dmaven.test.skip=true package`. The parent task verifies the authenticated public HTTP response and SDK after deploying that jar; repository/serialized DTO tests alone are not deployed wire proof.

Review round 3 completes the adjacent legacy `allTrades` execution projection. The same two real PostgreSQL cases now assert maker/taker prices, individual partial-fill notionals, native trade/order IDs, commissions/assets, owner isolation, direction flags, and execution timestamps on both owner-history paths. Fixtures persist each fill five minutes after its `trade_date` to prove that legacy time comes from execution. The focused Market selection remains 19 tests and uses the round-2 reproduction/package commands. No API change or migration is required; runtime and financial state are managed by the parent task.
