package co.nilin.opex.matching.gateway.ports.postgres

import co.nilin.opex.matching.gateway.ports.postgres.dto.*
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.*
import kotlinx.coroutines.reactor.awaitSingle
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.math.BigDecimal
import java.util.UUID

@EnabledIfEnvironmentVariable(named = "OPEX_TEST_POSTGRES", matches = ".+")
class OrderIntentStoreTest {
    private val factory = ConnectionFactories.get(System.getenv("OPEX_TEST_POSTGRES") ?: "r2dbc:postgresql://opex_test:opex_test@localhost/opex_test")
    private val db = DatabaseClient.create(factory)
    private val tx = TransactionalOperator.create(R2dbcTransactionManager(factory))
    private fun store() = OrderIntentStore(db, tx)
    private fun candidate(key: String, hash: String = "hash", owner: String = "owner") = OrderIntent(
        owner, key, UUID.randomUUID().toString(), hash, "USDT_IRT", "LIMIT", "BUY",
        BigDecimal("10"), BigDecimal("100"), true, BigDecimal("0.01"), "{}")

    @BeforeEach
    fun clean() = runBlocking {
        val database = db.sql("SELECT current_database() AS name")
            .map { row, _ -> row.get("name", String::class.java)!! }.one().awaitSingle()
        check(database == "opex_test") { "Refusing destructive fixture outside disposable opex_test database" }
        db.sql("DELETE FROM order_intent_outbox").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM order_intent_event").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM order_intent").fetch().rowsUpdated().awaitSingle()
        Unit
    }

    @Test
    fun concurrentSameKeyHasOneStableIdentityAndOneOutbox() = runBlocking {
        val receipts = (1..24).map { async(Dispatchers.IO) { store().accept(candidate("same-key")) } }.awaitAll()
        assertEquals(1, receipts.map { it.ouid }.toSet().size)
        assertEquals("ACCEPTED_PENDING", receipts.first().status)
        val count = db.sql("SELECT count(*) AS n FROM order_intent_outbox").map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }.one().awaitSingle()
        assertEquals(1L, count)
    }

    @Test
    fun lostAcceptanceResponseAndRestartReturnSameIdentity() = runBlocking {
        val accepted = store().accept(candidate("lost"))
        val replay = store().accept(candidate("lost"))
        assertEquals(accepted.ouid, replay.ouid)
        assertEquals(accepted.ouid, store().find("owner", "lost", null)!!.ouid)
    }

    @Test
    fun sameKeyDifferentPayloadIsConflictAndOtherOwnerCanReuseKey() = runBlocking {
        val first = store().accept(candidate("key"))
        val failure = runCatching { store().accept(candidate("key", "different")) }.exceptionOrNull()
        assertTrue(failure is OrderIntentConflict)
        assertNotEquals(first.ouid, store().accept(candidate("key", owner = "other")).ouid)
        assertNull(store().find("attacker", null, first.ouid))
        assertNull(store().cancel("attacker", null, first.ouid, "{}"))
    }

@Test
fun sameHashReplayOfTerminalTombstoneDoesNotRecreateSubmitOutbox() = runBlocking {
    val request = candidate("terminal-replay")
    val tombstone = store().accept(request)
    db.sql("DELETE FROM order_intent_outbox WHERE ouid=:ouid")
        .bind("ouid", tombstone.ouid).fetch().rowsUpdated().awaitSingle()
    db.sql("UPDATE order_intent SET status='CANCELED',cancel_requested=true WHERE ouid=:ouid")
        .bind("ouid", tombstone.ouid).fetch().rowsUpdated().awaitSingle()

    val replay = store().accept(request.copy(ouid = UUID.randomUUID().toString()))
    val submitRows = db.sql("SELECT count(*) AS n FROM order_intent_outbox WHERE ouid=:ouid AND kind='SUBMIT'")
        .bind("ouid", tombstone.ouid)
        .map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }
        .one().awaitSingle()

    assertEquals(tombstone.ouid, replay.ouid)
    assertEquals("CANCELED", replay.status)
    assertEquals(0L, submitRows)
}

    @Test
    fun metadataIsOwnerScopedIncludesTerminalConditionalTermsAndIsLimitedTo500() = runBlocking {
        repeat(500) { index -> store().accept(candidate("metadata-$index")) }
        val terminalStop = store().accept(candidate("terminal-stop").copy(
            type = "STOP_LIMIT",
            stopPrice = BigDecimal("10.2500"),
        ))
        db.sql("UPDATE order_intent SET status='FILLED', created_at=now() + INTERVAL '1 day' WHERE ouid=:ouid")
            .bind("ouid", terminalStop.ouid).fetch().rowsUpdated().awaitSingle()
        store().accept(candidate("other-owner", owner = "other"))

        val metadata = store().metadata("owner")

        assertEquals(500, metadata.size)
        assertEquals("terminal-stop", metadata.first().clientOrderId)
        assertEquals("FILLED", metadata.first().status)
        assertEquals("STOP_LIMIT", metadata.first().type)
        assertEquals("10.2500", metadata.first().stopPrice?.toPlainString())
        assertFalse(metadata.any { it.clientOrderId == "other-owner" })
    }

    @Test
    fun cancelReceiptRemainsPendingAndIsIdempotent() = runBlocking {
        store().accept(candidate("cancel"))
        assertTrue(store().cancel("owner", "cancel", null, "{}")!!.cancelRequested)
        assertTrue(store().cancel("owner", "cancel", null, "{}")!!.cancelRequested)
        val count = db.sql("SELECT count(*) AS n FROM order_intent_outbox WHERE kind='CANCEL'").map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }.one().awaitSingle()
        assertEquals(1L, count)
    }

    @Test
    fun failedPublicationKeepsDurableOutboxForRestart() = runBlocking {
        val accepted = store().accept(candidate("broker-down"))
        val error = runCatching { store().publishNext { _, _ -> throw java.io.IOException("broker unavailable") } }.exceptionOrNull()
        assertTrue(error is java.io.IOException)
        assertEquals(accepted.ouid, store().find("owner", "broker-down", null)!!.ouid)
        assertTrue(store().publishNext { _, _ -> Unit })
        assertFalse(store().publishNext { _, _ -> fail("No duplicate outbox should remain") })
    }

    @Test
    fun duplicateFillAndLateCreateCannotReduceExecutedQuantity() = runBlocking {
        val accepted = store().accept(candidate("fill"))
        store().outcome("owner", accepted.ouid, "execution-1", "{}", "PARTIALLY_FILLED", 7, 600, null)
        store().outcome("owner", accepted.ouid, "execution-1", "{}", "PARTIALLY_FILLED", 7, 600, null)
        store().outcome("owner", accepted.ouid, "late-create", "{}", "OPEN", 7, 1000, null)
        val receipt = store().find("owner", "fill", null)!!
        assertEquals(0, BigDecimal("4").compareTo(receipt.filledQuantity))
        assertEquals("PARTIALLY_FILLED", receipt.status)
        assertEquals(2, store().events("owner", 0).size)
        assertTrue(store().events("attacker", 0).isEmpty())
    }

    @Test
fun lateConditionalAdmissionAcknowledgementsPreserveMatcherEvidence() = runBlocking {
    for (evidence in listOf("OPEN", "PARTIALLY_FILLED", "FILLED", "CANCELED", "REJECTED")) {
        val accepted = store().accept(candidate("late-admission-$evidence").copy(
            type = "STOP_LIMIT",
            stopPrice = BigDecimal("101"),
        ))
        val remaining = if (evidence == "FILLED") 0L else 500L
        store().outcome("owner", accepted.ouid, "engine-$evidence", "{}", evidence, 71, remaining, null)

        store().outcome("owner", accepted.ouid, "admission-waiting-$evidence", "{}", "WAITING_TRIGGER", null, null, null)
        store().outcome("owner", accepted.ouid, "admission-triggered-$evidence", "{}", "TRIGGERED", null, null, null)

        val current = store().find("owner", null, accepted.ouid)!!
        assertEquals(evidence, current.status)
        if (evidence == "PARTIALLY_FILLED" || evidence == "FILLED") {
            assertTrue(current.filledQuantity > BigDecimal.ZERO)
        }
    }
}

@Test
fun lateWaitingAdmissionAcknowledgementCannotRegressTriggered() = runBlocking {
    val accepted = store().accept(candidate("waiting-after-trigger").copy(
        type = "STOP_LIMIT",
        stopPrice = BigDecimal("101"),
    ))
    store().outcome("owner", accepted.ouid, "admission-triggered", "{}", "TRIGGERED", null, null, null)
    store().outcome("owner", accepted.ouid, "admission-waiting", "{}", "WAITING_TRIGGER", null, null, null)

    assertEquals("TRIGGERED", store().find("owner", null, accepted.ouid)!!.status)
}

@Test
fun conditionalAdmissionStatusIsOwnerScopedAndDeduplicated() = runBlocking {
    val accepted = store().accept(candidate("scoped-admission-status").copy(
        type = "STOP_LIMIT",
        stopPrice = BigDecimal("101"),
    ))
    assertFalse(store().conditionalStatus("attacker", accepted.ouid, "admission-status", "{}", "TRIGGERED"))
    assertEquals("ACCEPTED_PENDING", store().find("owner", null, accepted.ouid)!!.status)

    store().conditionalStatus("owner", accepted.ouid, "admission-status", "{}", "TRIGGERED")
    store().conditionalStatus("owner", accepted.ouid, "admission-status", "{}", "WAITING_TRIGGER")

    assertEquals("TRIGGERED", store().find("owner", null, accepted.ouid)!!.status)
    assertTrue(store().events("attacker", 0).isEmpty())
    }

    @Test
    fun forwardCursorCannotSkipEarlierCommitAcrossPairsForOneOwner() = runBlocking {
        val first = store().accept(candidate("pair-one").copy(symbol = "BTC_USDT"))
        val second = store().accept(candidate("pair-two").copy(symbol = "USDT_IRT"))
        val locked = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val holder = async(Dispatchers.IO) {
            tx.executeAndAwait {
                db.sql("SELECT ouid FROM order_intent WHERE ouid=:id FOR UPDATE").bind("id", first.ouid)
                    .map { row, _ -> row.get("ouid", String::class.java)!! }.one().awaitSingle()
                locked.complete(Unit)
                release.await()
            }
        }
        withTimeout(5000) { locked.await() }
        suspend fun allocatedCursor() = db.sql("SELECT CASE WHEN is_called THEN last_value ELSE 0 END AS n FROM order_intent_event_cursor_seq")
            .map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }.one().awaitSingle()
        val before = allocatedCursor()
        val earlyTransaction = async(Dispatchers.IO) {
            store().outcome("owner", first.ouid, "commit-first", "{}", "OPEN", 1, 1000, null)
        }
        var laterTransaction: Deferred<*>? = null
        try {
            withTimeout(5000) { while (allocatedCursor() <= before) delay(10) }
            val later = async(Dispatchers.IO) {
                store().outcome("owner", second.ouid, "commit-later", "{}", "OPEN", 2, 1000, null)
            }
            laterTransaction = later
            withTimeoutOrNull(1000) { later.await() }
            val visible = store().events("owner", 0)
            val lastSeen = visible.lastOrNull()?.get("cursor") as Long? ?: 0L
            release.complete(Unit)
            holder.await(); earlyTransaction.await(); later.await()
            val nextPage = store().events("owner", lastSeen)
            val observed = (visible + nextPage).map { it["ouid"] }.toSet()
            assertEquals(setOf(first.ouid, second.ouid), observed)
        } finally {
            release.complete(Unit)
            holder.await(); earlyTransaction.await(); laterTransaction?.await()
        }
    }
}
