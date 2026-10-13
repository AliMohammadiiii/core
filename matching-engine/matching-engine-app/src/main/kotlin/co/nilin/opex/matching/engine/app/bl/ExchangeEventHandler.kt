package co.nilin.opex.matching.engine.app.bl

import co.nilin.opex.matching.engine.core.engine.SimpleOrderBook
import co.nilin.opex.matching.engine.core.eventh.EventDispatcher
import co.nilin.opex.matching.engine.core.eventh.events.*
import co.nilin.opex.matching.engine.core.inout.OrderBookUpdateEvent
import co.nilin.opex.matching.engine.core.spi.OrderBookPersister
import co.nilin.opex.matching.engine.ports.kafka.submitter.service.EventsSubmitter
import co.nilin.opex.matching.engine.ports.kafka.submitter.service.OrderBookUpdateSubmitter
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import org.springframework.stereotype.Component
import java.util.concurrent.ConcurrentHashMap

@Component
class ExchangeEventHandler(private val eventsSubmitter: EventsSubmitter,
    private val orderBookPersister: OrderBookPersister,
    private val orderBookSubmitter: OrderBookUpdateSubmitter, private val mapper: ObjectMapper) {
    private val locks = ConcurrentHashMap<String, Mutex>()
    private val capture = ThreadLocal<MutableList<CoreEvent>?>()

    fun register() {
        EventDispatcher.register(CoreEvent::class.java) { event ->
            if (event !is OrderBookPublishedEvent) capture.get()?.add(event)
        }
    }

    // A command, dedupe marker, resulting book and event outbox share one durable snapshot.
    // Acknowledged events are removed afterwards; ambiguous delivery replays stable identities.
    suspend fun process(symbol: String, command: (SimpleOrderBook) -> Unit) {
        locks.computeIfAbsent(symbol) { Mutex() }.withLock {
            var book = OrderBooks.lookupOrderBook(symbol) as SimpleOrderBook
            drain(book)
            val previous = book.persistent()
            val events = mutableListOf<CoreEvent>()
            capture.set(events)
            try { command(book) }
            catch (e: Exception) { OrderBooks.reloadOrderBook(previous); throw e }
            finally { capture.remove() }
            for (event in events) book.pendingEvents.add(mapper.writeValueAsString(mapOf(
                "type" to event.javaClass.simpleName, "event" to event)))
            try { orderBookPersister.storeLastState(book.persistent()) }
            catch (e: Exception) { OrderBooks.reloadOrderBook(previous); throw e }
            drain(book)
            orderBookSubmitter.submit(OrderBookUpdateEvent(book.pair, System.currentTimeMillis(), book.persistent().orders.orEmpty()))
        }
    }

    suspend fun recover(symbol: String) {
        locks.computeIfAbsent(symbol) { Mutex() }.withLock { drain(OrderBooks.lookupOrderBook(symbol) as SimpleOrderBook) }
    }

    private suspend fun drain(book: SimpleOrderBook) {
        while (book.pendingEvents.isNotEmpty()) {
            val saved = mapper.readTree(book.pendingEvents.first())
            val type = when (saved["type"].asText()) {
                "CreateOrderEvent" -> CreateOrderEvent::class.java
                "CancelOrderEvent" -> CancelOrderEvent::class.java
                "RejectOrderEvent" -> RejectOrderEvent::class.java
                "UpdatedOrderEvent" -> UpdatedOrderEvent::class.java
                "SubmitOrderEvent" -> SubmitOrderEvent::class.java
                "TradeEvent" -> TradeEvent::class.java
                else -> error("Unknown persisted matching event")
            }
            eventsSubmitter.submit(mapper.treeToValue(saved["event"], type))
            val sent = book.pendingEvents.removeAt(0)
            try { orderBookPersister.storeLastState(book.persistent()) }
            catch (e: Exception) { book.pendingEvents.add(0, sent); throw e }
        }
    }
}
