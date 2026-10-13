package co.nilin.opex.matching.engine.app

import co.nilin.opex.matching.engine.app.bl.ExchangeEventHandler
import co.nilin.opex.matching.engine.app.bl.OrderBooks
import co.nilin.opex.matching.engine.core.engine.SimpleOrderBook
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.inout.*
import co.nilin.opex.matching.engine.core.model.*
import co.nilin.opex.matching.engine.core.spi.OrderBookPersister
import co.nilin.opex.matching.engine.ports.kafka.submitter.service.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class CommandPublicationProgressTest {
    @Test
    fun rapidCreateCancelAndCommandsAcrossPairsFinishAndPublishLatestBooks() {
        val pair = Pair("PROGRESS", UUID.randomUUID().toString())
        val symbol = pair.toString()
        OrderBooks.createOrderBook(symbol)
        val otherPair = Pair("OTHER", UUID.randomUUID().toString())
        val otherSymbol = otherPair.toString()
        OrderBooks.createOrderBook(otherSymbol)
        val coreProducers = mutableListOf<MockProducer<String, CoreEvent>>()
        val bookProducers = mutableListOf<MockProducer<String?, OrderBookUpdateEvent>>()
        val coreFactory = object : ProducerFactory<String, CoreEvent> {
            override fun createProducer(): Producer<String, CoreEvent> =
                MockProducer(true, StringSerializer(), JsonSerializer<CoreEvent>()).also { coreProducers.add(it) }
        }
        val bookFactory = object : ProducerFactory<String?, OrderBookUpdateEvent> {
            override fun createProducer(): Producer<String?, OrderBookUpdateEvent> =
                MockProducer(true, StringSerializer(), JsonSerializer<OrderBookUpdateEvent>()).also { bookProducers.add(it) }
        }
        val snapshots = mutableMapOf<String, PersistentOrderBook>()
        val persister = object : OrderBookPersister {
            override suspend fun storeLastState(orderBook: PersistentOrderBook) { snapshots[symbol] = orderBook }
            override suspend fun loadLastState(symbol: String) = snapshots[symbol]
        }
        val handler = ExchangeEventHandler(EventsSubmitter(KafkaTemplate(coreFactory)), persister,
            OrderBookUpdateSubmitter(KafkaTemplate(bookFactory)), jacksonObjectMapper().registerModule(JavaTimeModule()))
        handler.register()
        val executor = Executors.newSingleThreadExecutor { runnable -> Thread(runnable).apply { isDaemon = true } }
        val completion = executor.submit {
            runBlocking {
                handler.process(symbol) { it.handleNewOrderCommand(OrderCreateCommand("first", "owner", pair,
                    100, 10, OrderDirection.BID, MatchConstraint.GTC, OrderType.LIMIT_ORDER)) }
                handler.process(symbol) { it.handleCancelCommand(OrderCancelCommand("first", "owner", 1, pair)) }
                handler.process(otherSymbol) { it.handleNewOrderCommand(OrderCreateCommand("other-pair", "other-owner", otherPair,
                    50, 3, OrderDirection.BID, MatchConstraint.GTC, OrderType.LIMIT_ORDER)) }
                handler.process(symbol) { it.handleNewOrderCommand(OrderCreateCommand("next", "owner", pair,
                    101, 10, OrderDirection.BID, MatchConstraint.GTC, OrderType.LIMIT_ORDER)) }
            }
        }
        try {
            val result = runCatching { completion.get(3, TimeUnit.SECONDS) }
            assertTrue(result.isSuccess, "Rapid command sequence did not finish: ${result.exceptionOrNull()}")
            val book = OrderBooks.lookupOrderBook(symbol) as SimpleOrderBook
            assertEquals("next", book.bestBidOrder!!.ouid)
            assertEquals(2L, book.orderCounter.get())
            val updates = bookProducers.flatMap { it.history() }
            assertEquals(listOf(1, 0, 1, 1), updates.map { it.value().orders.size })
            assertEquals(listOf(symbol, symbol, otherSymbol, symbol), updates.map { it.key() })
        } finally {
            completion.cancel(true)
            executor.shutdownNow()
        }
    }
}
