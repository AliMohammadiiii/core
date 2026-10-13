package co.nilin.opex.matching.gateway.app

import co.nilin.opex.matching.gateway.app.controller.OrderIntentController
import co.nilin.opex.matching.gateway.app.service.OrderIntentService
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import co.nilin.opex.utility.error.DefaultErrorConfig
import co.nilin.opex.utility.error.controller.ExceptionController
import co.nilin.opex.utility.error.data.DefaultExceptionResponse
import co.nilin.opex.utility.error.spi.ErrorTranslator
import co.nilin.opex.utility.error.spi.ExceptionResponse
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.*
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.WebFilter
import reactor.core.publisher.Mono
import java.security.Principal
import java.util.Date

class OrderIntentHttpTest {
    private val service = mockk<OrderIntentService>()
    private val store = mockk<OrderIntentStore>()
    private val translator = mockk<ErrorTranslator>()
    private fun client(): WebTestClient {
        every { translator.translate(any()) } returns Mono.just<ExceptionResponse>(
            DefaultExceptionResponse("internal", 500, "internal", HttpStatus.INTERNAL_SERVER_ERROR, null, null, Date()))
        val shared = ExceptionController(translator, DefaultErrorConfig(), jacksonObjectMapper())
        return WebTestClient.bindToController(OrderIntentController(service, store)).controllerAdvice(shared)
            .webFilter<WebTestClient.ControllerSpec>(WebFilter { exchange, chain -> chain.filter(exchange.mutate()
                .principal(Mono.just(Principal { "owner" })).build()) }).build()
    }

    @Test
    fun missingOwnedIntentRemains404WithSharedErrorAdvice() {
        coEvery { service.get("owner", "missing", null) } throws ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")
        client().get().uri("/v2/order?clientOrderId=missing").exchange().expectStatus().isNotFound
    }

    @Test
    fun conflictingIntentRemains409WithSharedErrorAdvice() {
        coEvery { service.get("owner", "conflict", null) } throws ResponseStatusException(HttpStatus.CONFLICT, "Payload conflict")
        client().get().uri("/v2/order?clientOrderId=conflict").exchange().expectStatus().isEqualTo(409)
    }
}
