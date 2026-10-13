package co.nilin.opex.market.ports.postgres.impl

import co.nilin.opex.market.core.inout.Trade
import co.nilin.opex.market.core.inout.TradeRequest
import co.nilin.opex.market.ports.postgres.dao.OrderRepository
import co.nilin.opex.market.ports.postgres.dao.OrderStatusRepository
import co.nilin.opex.market.ports.postgres.dao.TradeRepository
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.r2dbc.spi.ConnectionFactories
import io.r2dbc.spi.ConnectionFactoryOptions
import io.r2dbc.spi.Option
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.core.io.ClassPathResource
import org.springframework.data.r2dbc.core.R2dbcEntityTemplate
import org.springframework.data.r2dbc.repository.support.R2dbcRepositoryFactory
import org.springframework.r2dbc.core.DatabaseClient
import java.math.BigDecimal
import java.time.LocalDateTime
import java.util.UUID

@EnabledIfEnvironmentVariable(named = "OPEX_TEST_POSTGRES", matches = ".+")
class UserTradeHistoryPostgresTest {
    private val base = ConnectionFactoryOptions.parse(System.getenv("OPEX_TEST_POSTGRES")
        ?: "r2dbc:postgresql://opex_test:opex_test@localhost:18033/opex_test")
    private val admin = DatabaseClient.create(ConnectionFactories.get(base))
    private val schema = "market_history_${UUID.randomUUID().toString().replace("-", "") }"
    private val factory = ConnectionFactories.get(base.mutate().option(Option.valueOf("schema"), schema).build())
    private val db = DatabaseClient.create(factory)
    private lateinit var handler: UserQueryHandlerImpl
    private val mapper = jacksonObjectMapper().registerModule(JavaTimeModule())
    private val date = LocalDateTime.parse("2026-10-13T18:56:24.212")

    @BeforeEach
    fun initialize(): Unit = runBlocking {
        admin.sql("CREATE SCHEMA $schema").fetch().rowsUpdated().awaitSingle()
        val ddl = ClassPathResource("schema.sql").inputStream.bufferedReader().use { it.readText() }
        db.sql(ddl).then().awaitSingleOrNull()
        val repositories = R2dbcRepositoryFactory(R2dbcEntityTemplate(factory))
        handler = UserQueryHandlerImpl(repositories.getRepository(OrderRepository::class.java),
            repositories.getRepository(TradeRepository::class.java),
            repositories.getRepository(OrderStatusRepository::class.java))
    }

    @AfterEach
    fun cleanup(): Unit = runBlocking {
        admin.sql("DROP SCHEMA IF EXISTS $schema CASCADE").fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun order(ouid: String, owner: String, id: Long, side: String, price: String, quantity: String) {
        db.sql("""INSERT INTO orders(ouid,uuid,symbol,order_id,side,price,quantity,quote_quantity,update_date)
            VALUES(:ouid,:owner,'BTC_USDT',:id,:side,:price,:quantity,:notional,:date)""")
            .bind("ouid", ouid).bind("owner", owner).bind("id", id).bind("side", side)
            .bind("price", BigDecimal(price)).bind("quantity", BigDecimal(quantity))
            .bind("notional", BigDecimal(price).multiply(BigDecimal(quantity))).bind("date", date)
            .fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun fill(rowId: Long, tradeId: Long, makerOuid: String, maker: String, takerOuid: String,
                             taker: String, price: String, quantity: String, makerPrice: String, takerPrice: String,
                             makerFee: String, makerFeeAsset: String, takerFee: String, takerFeeAsset: String,
                             time: LocalDateTime) {
        db.sql("""INSERT INTO trades(id,trade_id,symbol,base_asset,quote_asset,matched_price,matched_quantity,
            maker_price,taker_price,maker_ouid,taker_ouid,maker_uuid,taker_uuid,maker_commission,
            maker_commission_asset,taker_commission,taker_commission_asset,trade_date,create_date)
            VALUES(:rowId,:tradeId,'BTC_USDT','BTC','USDT',:price,:quantity,:makerPrice,:takerPrice,
            :makerOuid,:takerOuid,:maker,:taker,:makerFee,:makerFeeAsset,:takerFee,:takerFeeAsset,:date,:date)""")
            .bind("rowId", rowId).bind("tradeId", tradeId).bind("price", BigDecimal(price))
            .bind("quantity", BigDecimal(quantity)).bind("makerPrice", BigDecimal(makerPrice))
            .bind("takerPrice", BigDecimal(takerPrice)).bind("makerOuid", makerOuid).bind("takerOuid", takerOuid)
            .bind("maker", maker).bind("taker", taker).bind("makerFee", BigDecimal(makerFee))
            .bind("makerFeeAsset", makerFeeAsset).bind("takerFee", BigDecimal(takerFee))
            .bind("takerFeeAsset", takerFeeAsset).bind("date", time).fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun history(owner: String) = handler.getTradeHistory(owner, "BTC_USDT", null, null, null, 100, 0)!!
    private fun decimal(expected: String, actual: BigDecimal) = assertEquals(0, BigDecimal(expected).compareTo(actual))
    private fun json(trade: Trade): JsonNode = mapper.valueToTree(trade)

    @Test
    fun ownerHistoryKeepsNativeIdsAndPartialFillAmountsAcrossMultipleOrders(): Unit = runBlocking {
        order("maker-a", "seller", 101, "ASK", "62500", "2")
        order("taker-b", "buyer", 202, "BID", "63000", "0.2")
        order("maker-c", "seller", 303, "ASK", "62400", "1")
        order("foreign-d", "foreign-maker", 404, "ASK", "61000", "1")
        order("foreign-e", "foreign-taker", 505, "BID", "61200", "0.2")
        fill(901, 31, "maker-a", "seller", "taker-b", "buyer", "62500", "0.075", "62500", "63000",
            "4.6875", "USDT", "0.000075", "BTC", date)
        fill(902, 32, "maker-c", "seller", "taker-b", "buyer", "62400", "0.025", "62400", "63000",
            "1.56", "USDT", "0.000025", "BTC", date.plusSeconds(2))
        fill(903, 33, "foreign-d", "foreign-maker", "foreign-e", "foreign-taker", "61000", "0.01", "61000", "61200",
            "0.61", "USDT", "0.00001", "BTC", date.plusSeconds(3))
        val buyer = history("buyer")
        val seller = history("seller")
        val legacyBuyer = handler.allTrades("buyer", TradeRequest("BTC_USDT", null, null, null, 100))!!
        assertEquals(2, buyer.size)
        assertEquals(2, seller.size)
        assertTrue(history("stranger").isEmpty())
        assertAll(
            { assertEquals(listOf(32L, 31L), buyer.map { it.id }) },
            { assertEquals(listOf(32L, 31L), seller.map { it.id }) },
            { assertEquals(listOf(202L, 202L), buyer.map { json(it)["orderId"]?.asLong() }) },
            { assertEquals(listOf(303L, 101L), seller.map { json(it)["orderId"]?.asLong() }) },
            { assertEquals(listOf(202L, 202L), legacyBuyer.map { json(it)["orderId"]?.asLong() }) },
            { decimal("62400", buyer[0].price) }, { decimal("62500", buyer[1].price) },
            { decimal("0.025", buyer[0].quantity) }, { decimal("0.075", buyer[1].quantity) },
            { decimal("1560", buyer[0].quoteQuantity) }, { decimal("4687.5", buyer[1].quoteQuantity) },
            { decimal("1560", seller[0].quoteQuantity) }, { decimal("4687.5", seller[1].quoteQuantity) },
            { decimal("0.000025", buyer[0].commission) }, { decimal("4.6875", seller[1].commission) },
            { assertEquals("BTC", buyer[0].commissionAsset) }, { assertEquals("USDT", seller[1].commissionAsset) },
            { assertTrue(buyer.all { it.isBuyer && !it.isMaker }) },
            { assertTrue(seller.all { !it.isBuyer && it.isMaker }) },
            { assertEquals(listOf(date.plusSeconds(2), date), buyer.map { it.time }) })
    }

    @Test
    fun makerBuyAndTakerSellUseTheMatchedPriceAndTheirOwnOrderAndCommission(): Unit = runBlocking {
        order("maker-buy", "buyer", 606, "BID", "62400", "0.2")
        order("taker-sell", "seller", 707, "ASK", "62000", "0.5")
        fill(904, 34, "maker-buy", "buyer", "taker-sell", "seller", "62400", "0.04", "62400", "62000",
            "0.00004", "BTC", "2.496", "USDT", date)
        val buyer = history("buyer").single()
        val seller = history("seller").single()
        assertAll(
            { assertEquals(34L, buyer.id) }, { assertEquals(34L, seller.id) },
            { assertEquals(606L, json(buyer)["orderId"]?.asLong()) },
            { assertEquals(707L, json(seller)["orderId"]?.asLong()) },
            { decimal("62400", buyer.price) }, { decimal("62400", seller.price) },
            { decimal("0.04", buyer.quantity) }, { decimal("0.04", seller.quantity) },
            { decimal("2496", buyer.quoteQuantity) }, { decimal("2496", seller.quoteQuantity) },
            { decimal("0.00004", buyer.commission) }, { decimal("2.496", seller.commission) },
            { assertEquals("BTC", buyer.commissionAsset) }, { assertEquals("USDT", seller.commissionAsset) },
            { assertTrue(buyer.isMaker && buyer.isBuyer) },
            { assertTrue(!seller.isMaker && !seller.isBuyer) },
            { assertEquals(date, buyer.time) }, { assertEquals(date, seller.time) })
    }
}
