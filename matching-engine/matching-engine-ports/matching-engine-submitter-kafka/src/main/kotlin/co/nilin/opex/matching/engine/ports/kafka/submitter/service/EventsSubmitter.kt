package co.nilin.opex.matching.engine.ports.kafka.submitter.service

import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.eventh.events.TradeEvent
import kotlinx.coroutines.withTimeout
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Component
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

@Component
class EventsSubmitter(val kafkaTemplate: KafkaTemplate<String, CoreEvent>) {
    suspend fun submit(event: CoreEvent) = withTimeout(15000) {
        if (event is TradeEvent) send("trades_${event.pair}", event)
        send("events_${event.pair}", event)
    }
    private suspend fun send(topic: String, event: CoreEvent): Unit = suspendCancellableCoroutine { cont ->
        kafkaTemplate.send(topic, event.pair.toString(), event).addCallback(
            { cont.resume(Unit) }, { cont.resumeWithException(it) })
    }
}
