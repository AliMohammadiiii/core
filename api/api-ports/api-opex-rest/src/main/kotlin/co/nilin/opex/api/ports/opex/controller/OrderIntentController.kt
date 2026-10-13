package co.nilin.opex.api.ports.opex.controller
import co.nilin.opex.api.core.inout.OrderIntentRequest
import co.nilin.opex.api.core.spi.OrderIntentProxy
import co.nilin.opex.common.security.jwtAuthentication
import co.nilin.opex.common.security.tokenValue
import org.springframework.security.core.annotation.CurrentSecurityContext
import org.springframework.security.core.context.SecurityContext
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/opex/v2/order")
class OrderIntentController(private val proxy: OrderIntentProxy) {
    @PostMapping
    suspend fun submit(@RequestBody request: OrderIntentRequest, @CurrentSecurityContext context: SecurityContext) =
        proxy.submit(request, context.jwtAuthentication().tokenValue())
    @GetMapping
    suspend fun get(@RequestParam(required=false) clientOrderId: String?, @RequestParam(required=false) ouid: String?,
        @CurrentSecurityContext context: SecurityContext) = proxy.lookup(clientOrderId, ouid, context.jwtAuthentication().tokenValue())
    @DeleteMapping
    suspend fun cancel(@RequestParam(required=false) clientOrderId: String?, @RequestParam(required=false) ouid: String?,
        @CurrentSecurityContext context: SecurityContext) = proxy.cancel(clientOrderId, ouid, context.jwtAuthentication().tokenValue())
    @GetMapping("/owned")
    suspend fun owned(@CurrentSecurityContext context: SecurityContext) = proxy.owned(context.jwtAuthentication().tokenValue())
    @GetMapping("/events")
    suspend fun events(@RequestParam(defaultValue="0") cursor: Long, @CurrentSecurityContext context: SecurityContext) =
        proxy.events(cursor, context.jwtAuthentication().tokenValue())
}
