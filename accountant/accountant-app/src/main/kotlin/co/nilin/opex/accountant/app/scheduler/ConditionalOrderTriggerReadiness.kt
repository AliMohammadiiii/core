package co.nilin.opex.accountant.app.scheduler

import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.atomic.AtomicLong

data class ConditionalOrderCapabilities(
    val conditionalReady: Boolean,
    val lastSuccessfulPollMs: Long?,
    val freshnessThresholdMs: Long,
)

@Component
class ConditionalOrderTriggerReadiness(
    @Value("\${app.order-admission.readiness-max-age-ms:5000}")
    private val freshnessThresholdMs: Long,
) {
    private val lastPollMs = AtomicLong(0L)

    fun markPollSucceeded(now: Instant = Instant.now()) {
        lastPollMs.set(now.toEpochMilli())
    }

    fun capabilities(now: Instant = Instant.now()): ConditionalOrderCapabilities {
        val last = lastPollMs.get().takeIf { it > 0L }
        val age = last?.let { now.toEpochMilli() - it }
        return ConditionalOrderCapabilities(
            conditionalReady = age != null && age in 0..freshnessThresholdMs,
            lastSuccessfulPollMs = last,
            freshnessThresholdMs = freshnessThresholdMs,
        )
    }
}
