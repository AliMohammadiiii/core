package co.nilin.opex.matching.engine.app.config

import co.nilin.opex.matching.engine.app.bl.ExchangeEventHandler
import co.nilin.opex.matching.engine.app.bl.OrderBooks
import co.nilin.opex.matching.engine.app.listener.MatchingEngineEventListener
import co.nilin.opex.matching.engine.app.listener.OrderListener
import co.nilin.opex.matching.engine.core.model.PersistentOrderBook
import co.nilin.opex.matching.engine.core.spi.OrderBookPersister
import co.nilin.opex.matching.engine.ports.kafka.listener.consumer.EventKafkaListener
import co.nilin.opex.matching.engine.ports.kafka.listener.consumer.OrderKafkaListener
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class AppConfig(
) {
    @Autowired
    private lateinit var symbols: List<String>

    @Bean
    @ConditionalOnMissingBean(value = [OrderBookPersister::class])
    fun orderBookPersister(): OrderBookPersister {
        return object : OrderBookPersister {
            override suspend fun storeLastState(orderBook: PersistentOrderBook) {
                error("Durable matching snapshot storage is required")
            }

            override suspend fun loadLastState(symbol: String): PersistentOrderBook? {
                error("Durable matching snapshot storage is required")
            }
        }
    }

    @Autowired
    fun configureOrderBooks(orderBookPersister: OrderBookPersister, handler: ExchangeEventHandler) {
        runBlocking {
            symbols.forEach { symbol ->
                val saved = orderBookPersister.loadLastState(symbol)
                if (saved != null) OrderBooks.reloadOrderBook(saved) else OrderBooks.createOrderBook(symbol)
                handler.recover(symbol)
            }
        }
    }

    @Bean
    fun orderListener(handler: ExchangeEventHandler): OrderListener {
        return OrderListener(handler)
    }

    @Autowired
    fun configureOrderListener(orderKafkaListener: OrderKafkaListener, orderListener: OrderListener) {
        orderKafkaListener.addOrderListener(orderListener)
    }

    @Bean
    fun eventListener(): MatchingEngineEventListener {
        return MatchingEngineEventListener()
    }

    @Autowired
    fun configureEventListener(eventKafkaListener: EventKafkaListener, eventListener: MatchingEngineEventListener) {
        eventKafkaListener.addEventListener(eventListener)
    }

    @Autowired
    fun configureMatchingEngineListener(exchangeEventHandler: ExchangeEventHandler) {
        exchangeEventHandler.register()
    }
}
