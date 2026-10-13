package co.nilin.opex.matching.gateway.ports.postgres.dto

import java.math.BigDecimal
import java.time.OffsetDateTime

data class OrderIntent(
    val owner: String, val clientOrderId: String, val ouid: String, val payloadHash: String,
    val symbol: String, val type: String, val side: String, val quantity: BigDecimal,
    val price: BigDecimal, val postOnly: Boolean, val leftFraction: BigDecimal,
    val payload: String, val status: String = "ACCEPTED_PENDING", val orderId: Long? = null,
    val filledQuantity: BigDecimal = BigDecimal.ZERO, val cancelRequested: Boolean = false,
    val reason: String? = null, val createdAt: OffsetDateTime = OffsetDateTime.now()
)

class OrderIntentConflict : RuntimeException("clientOrderId was already used for different order parameters")
