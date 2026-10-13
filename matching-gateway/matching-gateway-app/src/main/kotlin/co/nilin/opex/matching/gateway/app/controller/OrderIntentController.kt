package co.nilin.opex.matching.gateway.app.controller

import co.nilin.opex.matching.gateway.app.inout.OrderIntentRequest
import co.nilin.opex.matching.gateway.app.service.OrderIntentService
import co.nilin.opex.matching.gateway.ports.postgres.dto.*
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import org.springframework.web.server.ResponseStatusException
import java.security.Principal

@RestController
@RequestMapping("/v2/order")
class OrderIntentController(private val service: OrderIntentService, private val store: OrderIntentStore) {
    @PostMapping
    suspend fun submit(principal: Principal, @RequestBody request: OrderIntentRequest): Map<String, Any?> =
        receipt(try { service.submit(principal.name, request) } catch (e: OrderIntentConflict) {
            throw ResponseStatusException(HttpStatus.CONFLICT, e.message, e)
        })
    @GetMapping
    suspend fun get(principal: Principal, @RequestParam(required = false) clientOrderId: String?,
        @RequestParam(required = false) ouid: String?) = receipt(service.get(principal.name, clientOrderId, ouid))
    @DeleteMapping
    suspend fun cancel(principal: Principal, @RequestParam(required = false) clientOrderId: String?,
        @RequestParam(required = false) ouid: String?) = receipt(service.cancel(principal.name, clientOrderId, ouid))
    @GetMapping("/owned")
    suspend fun owned(principal: Principal) = store.owned(principal.name).map { receipt(it) }
    @GetMapping("/events")
    suspend fun events(principal: Principal, @RequestParam(defaultValue = "0") cursor: Long) =
        store.events(principal.name, cursor)

    private fun receipt(i: OrderIntent): Map<String, Any?> {
        val terminal = i.status in listOf("FILLED", "CANCELED", "REJECTED")
        return linkedMapOf("clientOrderId" to i.clientOrderId, "ouid" to i.ouid, "symbol" to i.symbol,
            "status" to if (i.cancelRequested && !terminal) "CANCEL_PENDING" else i.status,
            "orderId" to i.orderId, "type" to i.type, "side" to i.side, "postOnly" to i.postOnly,
            "quantity" to i.quantity.toPlainString(), "price" to i.price.toPlainString(),
            "filledQuantity" to i.filledQuantity.toPlainString(),
            "remainingQuantity" to i.quantity.subtract(i.filledQuantity).toPlainString(),
            "reason" to i.reason, "createdAt" to i.createdAt.toInstant().toString())
    }
}
