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
}
