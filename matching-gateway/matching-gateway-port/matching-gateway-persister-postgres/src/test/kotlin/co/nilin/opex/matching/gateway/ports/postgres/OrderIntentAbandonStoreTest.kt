package co.nilin.opex.matching.gateway.ports.postgres

import co.nilin.opex.matching.gateway.ports.postgres.dto.OrderIntent
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentAbandonStore
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.reactor.awaitSingle
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import java.math.BigDecimal
import java.util.UUID

@EnabledIfEnvironmentVariable(named = "OPEX_TEST_POSTGRES", matches = ".+")
class OrderIntentAbandonStoreTest {
    private val factory = ConnectionFactories.get(
        System.getenv("OPEX_TEST_POSTGRES") ?: "r2dbc:postgresql://opex_test:opex_test@localhost/opex_test"
    )
    private val db = DatabaseClient.create(factory)
    private val tx = TransactionalOperator.create(R2dbcTransactionManager(factory))
    private val orderStore = OrderIntentStore(db, tx)
    private val abandonStore = OrderIntentAbandonStore(db, tx, orderStore)

    private fun candidate(key: String, hash: String = "a".repeat(64), owner: String = "owner") = OrderIntent(
        owner = owner,
        clientOrderId = key,
        ouid = UUID.randomUUID().toString(),
        payloadHash = hash,
        symbol = "USDT_IRT",
        type = "LIMIT",
        side = "BUY",
        quantity = BigDecimal("10"),
        price = BigDecimal("100"),
        postOnly = true,
        leftFraction = BigDecimal("0.01"),
        payload = "{}",
    )

    private suspend fun outboxCount(): Long = db.sql("SELECT count(*) AS n FROM order_intent_outbox")
        .map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }
        .one()
        .awaitSingle()

    @BeforeEach
    fun clean() = runBlocking {
        val database = db.sql("SELECT current_database() AS name")
            .map { row, _ -> row.get("name", String::class.java)!! }
            .one()
            .awaitSingle()
        check(database == "opex_test") {
            "Refusing destructive fixture outside disposable opex_test database"
        }
        db.sql("DELETE FROM order_intent_outbox").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM order_intent_event").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM order_intent").fetch().rowsUpdated().awaitSingle()
        Unit
    }

    @Test
    fun concurrentAbandonsCreateOneTerminalRowWithoutOutbox() = runBlocking {
        val tombstones = (1..24).map {
            async(Dispatchers.IO) {
                abandonStore.insertOrRead(candidate("same-key").copy(status = "CANCELED", cancelRequested = true))
            }
        }.awaitAll()

        assertEquals(1, tombstones.map { it.ouid }.toSet().size)
        assertTrue(tombstones.all { it.status == "CANCELED" && it.cancelRequested })
        assertEquals(0L, outboxCount())
    }

    @Test
    fun tombstoneWinsAgainstLaterSubmitAndIsScopedByOwner() = runBlocking {
        val ownerTombstone = abandonStore.insertOrRead(
            candidate("same-key").copy(status = "CANCELED", cancelRequested = true)
        )
        val replay = orderStore.accept(candidate("same-key"))
        val otherOwner = abandonStore.insertOrRead(
            candidate("same-key", owner = "other-owner").copy(status = "CANCELED", cancelRequested = true)
        )

        assertEquals(ownerTombstone.ouid, replay.ouid)
        assertEquals("CANCELED", replay.status)
        assertNotEquals(ownerTombstone.ouid, otherOwner.ouid)
        assertNull(orderStore.find("attacker", "same-key", null))
        assertEquals(0L, outboxCount())
    }

    @Test
    fun existingSubmitIsReturnedForNormalCancellation() = runBlocking {
        val accepted = orderStore.accept(candidate("submit-first"))
        val existing = abandonStore.insertOrRead(
            candidate("submit-first").copy(status = "CANCELED", cancelRequested = true)
        )

        assertEquals(accepted.ouid, existing.ouid)
        assertEquals("ACCEPTED_PENDING", existing.status)
        assertEquals(1L, outboxCount())
    }
}
