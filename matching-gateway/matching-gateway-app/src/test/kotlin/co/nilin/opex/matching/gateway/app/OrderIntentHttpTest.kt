package co.nilin.opex.matching.gateway.app

import co.nilin.opex.matching.gateway.app.controller.OrderIntentController
import co.nilin.opex.matching.gateway.app.inout.OrderIntentCapabilities
import co.nilin.opex.matching.gateway.app.inout.OrderIntentMetadata
import co.nilin.opex.matching.gateway.app.service.OrderIntentReadService
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
    private val readService = mockk<OrderIntentReadService>()
    private val translator = mockk<ErrorTranslator>()
    private fun client(): WebTestClient {
        every { translator.translate(any()) } returns Mono.just<ExceptionResponse>(
            DefaultExceptionResponse("internal", 500, "internal", HttpStatus.INTERNAL_SERVER_ERROR, null, null, Date()))
        val shared = ExceptionController(translator, DefaultErrorConfig(), jacksonObjectMapper())
        return WebTestClient.bindToController(OrderIntentController(service, store, readService)).controllerAdvice(shared)
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

    @Test
    fun capabilitiesAreServedByReadService() {
        coEvery { readService.capabilities() } returns OrderIntentCapabilities(listOf("LIMIT", "MARKET"))

        client().get().uri("/v2/order/capabilities").exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$.supportedTypes[0]").isEqualTo("LIMIT")
            .jsonPath("$.supportedTypes[1]").isEqualTo("MARKET")
    }

    @Test
    fun metadataUsesAuthenticatedPrincipalAsOwner() {
        coEvery { readService.metadata("owner") } returns listOf(
            OrderIntentMetadata(
                ouid = "order-1",
                clientOrderId = "client-1",
                symbol = "BTC_USDT",
                type = "STOP_LIMIT",
                side = "BUY",
                stopPrice = "10.2500",
                price = "10.5000",
                quantity = "0.1250",
                filledQuantity = "0.1250",
                remainingQuantity = "0",
                createdAt = "2026-10-16T10:00:00Z",
                status = "FILLED",
            )
        )

        client().get().uri("/v2/order/metadata").exchange()
            .expectStatus().isOk
            .expectBody().jsonPath("$[0].clientOrderId").isEqualTo("client-1")
            .jsonPath("$[0].stopPrice").isEqualTo("10.2500")
            .jsonPath("$[0].side").isEqualTo("BUY")
            .jsonPath("$[0].filledQuantity").isEqualTo("0.1250")
            .jsonPath("$[0].remainingQuantity").isEqualTo("0")

        coVerify(exactly = 1) { readService.metadata("owner") }
    }
}
