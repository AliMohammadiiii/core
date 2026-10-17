package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.gateway.app.inout.OrderIntentRequest
import co.nilin.opex.matching.gateway.app.spi.AccountantReadinessProxy
import co.nilin.opex.matching.gateway.app.spi.PairConfigLoader
import co.nilin.opex.matching.gateway.ports.postgres.dto.PairSetting
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentAbandonStore
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import co.nilin.opex.matching.gateway.ports.postgres.service.PairSettingService
import com.fasterxml.jackson.databind.ObjectMapper
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import java.math.BigDecimal

@EnabledIfEnvironmentVariable(named = "OPEX_TEST_POSTGRES", matches = ".+")
class OrderIntentAbandonServiceTest {
    private val factory = ConnectionFactories.get(
        System.getenv("OPEX_TEST_POSTGRES") ?: "r2dbc:postgresql://opex_test:opex_test@localhost/opex_test"
    )
    private val db = DatabaseClient.create(factory)
    private val tx = TransactionalOperator.create(R2dbcTransactionManager(factory))
    private val orderStore = OrderIntentStore(db, tx)
    private val abandonStore = OrderIntentAbandonStore(db, tx, orderStore)
    private val service = OrderIntentService(
        store = orderStore,
        abandonStore = abandonStore,
        pairs = object : PairSettingService {
            override suspend fun load(pair: String): PairSetting = error("abandon must not load pair settings")
            override suspend fun loadAll(): List<PairSetting> = error("not used by this test")
            override suspend fun update(pairSetting: PairSetting): PairSetting = error("not used by this test")
        },
        config = object : PairConfigLoader {
            override suspend fun load(pair: String, direction: OrderDirection) =
                error("abandon must not load pair config")
        },
        mapper = ObjectMapper(),
        accountantReadiness = object : AccountantReadinessProxy {
            override suspend fun conditionalOrdersReady() = error("abandon must not check submit readiness")
        },
    )

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
    fun stopLimitTombstoneCanBeAbandonedAndDeletedRepeatedly() = runBlocking {
        val request = OrderIntentRequest(
            clientOrderId = "stop-limit-abandon",
            symbol = "BTC_USDT",
            type = "STOP_LIMIT",
            side = "BUY",
            quantity = BigDecimal("1"),
            price = BigDecimal("100"),
            stopPrice = BigDecimal("110"),
        )

        val tombstone = service.abandon("owner", request)
        val repeatedDelete = service.cancel("owner", request.clientOrderId, null)
        val repeatedAbandon = service.abandon("owner", request)
        val outboxCount = db.sql("SELECT count(*) AS n FROM order_intent_outbox")
            .map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }
            .one()
            .awaitSingle()

        assertEquals("CANCELED", tombstone.status)
        assertEquals(tombstone.ouid, repeatedDelete.ouid)
        assertEquals(tombstone.ouid, repeatedAbandon.ouid)
        assertEquals(0L, outboxCount)
    }
}
