#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."

# Use Java17+ and Maven3.9.9. Native builds avoid placing compiler heaps in the demo Docker VM.
TASK_MAVEN_BIN="${OPEX_MAVEN_BIN:-mvn}"
TASK_MAVEN_REPO="${OPEX_MAVEN_REPO:-}"
TASK_MAVEN_SETTINGS="${OPEX_MAVEN_SETTINGS:-}"
TASK_MAVEN_ARGS=()
if [[ -n "$TASK_MAVEN_REPO" ]]; then TASK_MAVEN_ARGS+=("-Dmaven.repo.local=$TASK_MAVEN_REPO"); fi
if [[ -n "$TASK_MAVEN_SETTINGS" ]]; then TASK_MAVEN_ARGS+=("-s" "$TASK_MAVEN_SETTINGS"); fi
export MAVEN_OPTS="${MAVEN_OPTS:--Xmx768m -XX:ActiveProcessorCount=2}"

if [[ "${1:-package}" == "test" ]]; then
  "$TASK_MAVEN_BIN" "${TASK_MAVEN_ARGS[@]}" -B \
    -pl matching-engine/matching-engine-app,accountant/accountant-app,matching-gateway/matching-gateway-app,market/market-ports/market-persister-postgres \
    -am -Pno-vault -Dskip.unit.tests=false -Dsurefire.failIfNoSpecifiedTests=false \
    -Dtest=CommandPublicationProgressTest,OrderAdmissionProgressTest,OrderIntentStoreTest,OrderSafetyTest,SimpleOrderBookUnitTest,OrderRefundSafetyTest,OrderManagerImplTest,TradeManagerImplTest,FeeCalculatorImplTest,TradePersisterTest,OrderPersisterTest,MarketQueryHandlerTest,UserQueryHandlerTest,UserTradeHistoryPostgresTest,OrderIntentHttpTest \
    '-DargLine=-Xmx384m -XX:ActiveProcessorCount=2' test
else
  "$TASK_MAVEN_BIN" "${TASK_MAVEN_ARGS[@]}" -B -Pno-vault \
    -pl api/api-app,matching-gateway/matching-gateway-app,matching-engine/matching-engine-app,accountant/accountant-app,market/market-app,eventlog/eventlog-app \
    -am -Dskip.unit.tests=true -Dmaven.test.skip=true package
fi
