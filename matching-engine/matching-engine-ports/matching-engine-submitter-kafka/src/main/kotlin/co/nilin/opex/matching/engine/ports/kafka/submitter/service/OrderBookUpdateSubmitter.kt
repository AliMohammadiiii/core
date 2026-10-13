package co.nilin.opex.matching.engine.ports.kafka.submitter.service

import co.nilin.opex.matching.engine.core.inout.OrderBookUpdateEvent
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

@Component
class OrderBookUpdateSubmitter(private val kafkaTemplate: KafkaTemplate<String?, OrderBookUpdateEvent>) {
    private val logger = LoggerFactory.getLogger(OrderBookUpdateSubmitter::class.java)

    // Every resulting book is published. Dropping the final update could leave a canceled order visible.
    // Pair keys retain update ordering; a failed/ambiguous acknowledgment is bounded and retried by the command path.
    suspend fun submit(event: OrderBookUpdateEvent): Unit = withTimeout(15000) {
        suspendCancellableCoroutine { cont ->
            kafkaTemplate.send("orderBookUpdate", event.pair.toString(), event).addCallback(
                { logger.info("Orderbook update event sent"); cont.resume(Unit) },
                { logger.warn("Orderbook update event failed", it); cont.resumeWithException(it) })
        }
    }
}
