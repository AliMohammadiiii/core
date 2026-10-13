package co.nilin.opex.api.core.inout
import java.math.BigDecimal

data class OrderIntentRequest(val clientOrderId: String, val symbol: String, val type: String,
    val side: String, val quantity: BigDecimal, val price: BigDecimal, val postOnly: Boolean = false)
data class OrderIntentReceipt(val clientOrderId: String, val ouid: String, val symbol: String,
    val status: String, val orderId: Long?, val type: String, val side: String, val postOnly: Boolean,
    val quantity: String, val price: String, val filledQuantity: String, val remainingQuantity: String,
    val reason: String?, val createdAt: String)
data class OrderIntentEvent(val cursor: Long, val ouid: String, val payload: String)
