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
