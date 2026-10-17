package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.*
import co.nilin.opex.matching.gateway.ports.kafka.submitter.service.OrderRequestEventSubmitter
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component

@Component
class OrderIntentOutbox(private val store: OrderIntentStore, private val submitter: OrderRequestEventSubmitter,
    private val mapper: ObjectMapper) {
    @Scheduled(fixedDelay = 500)
    fun publish() = runBlocking {
        repeat(50) {
            val sent = store.publishNext { kind, payload ->
                val event = when (kind) {
                    "SUBMIT" -> mapper.readValue(payload, OrderSubmitRequestEvent::class.java)
                    "ACTIVATE" -> mapper.readValue(payload, OrderSubmitRequestEvent::class.java)
                    "CANCEL" -> if (mapper.readTree(payload).hasNonNull("conditionalAction")) {
                        mapper.readValue(payload, OrderSubmitRequestEvent::class.java)
                    } else mapper.readValue(payload, OrderCancelRequestEvent::class.java)
                    else -> error("Invalid order outbox type")
                }
                withTimeout(15000) { submitter.submit(event) }
            }
            if (!sent) return@runBlocking
        }
    }
}
