package co.nilin.opex.accountant.app.controller

import co.nilin.opex.accountant.app.scheduler.ConditionalOrderCapabilities
import co.nilin.opex.accountant.app.scheduler.ConditionalOrderTriggerReadiness
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/order-admission")
class OrderAdmissionCapabilitiesController(
    private val readiness: ConditionalOrderTriggerReadiness,
) {
    @GetMapping("/capabilities")
    fun capabilities(): ConditionalOrderCapabilities = readiness.capabilities()
}
