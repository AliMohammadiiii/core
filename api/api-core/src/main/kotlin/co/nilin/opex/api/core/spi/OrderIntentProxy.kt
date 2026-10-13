package co.nilin.opex.api.core.spi
import co.nilin.opex.api.core.inout.*
interface OrderIntentProxy {
    suspend fun submit(request: OrderIntentRequest, token: String): OrderIntentReceipt
    suspend fun lookup(clientOrderId: String?, ouid: String?, token: String): OrderIntentReceipt
    suspend fun cancel(clientOrderId: String?, ouid: String?, token: String): OrderIntentReceipt
    suspend fun owned(token: String): List<OrderIntentReceipt>
    suspend fun events(cursor: Long, token: String): List<OrderIntentEvent>
}
