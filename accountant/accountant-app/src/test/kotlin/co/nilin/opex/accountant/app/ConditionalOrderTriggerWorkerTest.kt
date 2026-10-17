package co.nilin.opex.accountant.app

import co.nilin.opex.accountant.app.scheduler.ConditionalOrderTriggerReadiness
import co.nilin.opex.accountant.app.scheduler.ConditionalOrderTriggerWorker
import co.nilin.opex.accountant.app.service.OrderAdmissionService
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.model.MatchConstraint
import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.engine.core.model.OrderType
import co.nilin.opex.matching.engine.core.model.Pair
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono
import java.net.URI
import java.math.BigDecimal

class ConditionalOrderTriggerWorkerTest {
    @Test
    fun `idle readiness probes recent trades for configured canary`() = runBlocking {
        val requests = mutableListOf<URI>()
        val admission = mockk<OrderAdmissionService>()
        coEvery { admission.pendingConditionalTriggers(null, 500) } returns emptyList()
        val readiness = ConditionalOrderTriggerReadiness(5_000)
        val worker = worker(admission, readiness, requests, "[]")

        worker.pollOnce()

        assertEquals(
            listOf(URI("http://market/v1/market/BTC_USDT/recent-trades?limit=1")),
            requests,
        )
        assertTrue(readiness.capabilities().conditionalReady)
    }

    @Test
    fun `invalid recent trade body does not mark readiness`() = runBlocking {
        val requests = mutableListOf<URI>()
        val admission = mockk<OrderAdmissionService>()
        coEvery { admission.pendingConditionalTriggers(null, 500) } returns emptyList()
        val readiness = ConditionalOrderTriggerReadiness(5_000)
        val worker = worker(admission, readiness, requests, "[{}]")

        worker.pollOnce()

        assertEquals(1, requests.size)
        assertFalse(readiness.capabilities().conditionalReady)
    }

    @Test
    fun `trigger polling advances through unchanged pages and wraps`() = runBlocking {
        val requests = mutableListOf<URI>()
        val admission = mockk<OrderAdmissionService>()
        val firstPage = (1..500).map { index -> candidate("order-${index.toString().padStart(4, '0')}") }
        val tail = candidate("order-0501")
        coEvery { admission.pendingConditionalTriggers(null, 500) } returns firstPage andThen firstPage
        coEvery { admission.pendingConditionalTriggers("order-0500", 500) } returns listOf(tail)
        val worker = worker(admission, ConditionalOrderTriggerReadiness(5_000), requests, "[]")

        worker.pollOnce()
        worker.pollOnce()
        worker.pollOnce()

        coVerify(exactly = 2) { admission.pendingConditionalTriggers(null, 500) }
        coVerify(exactly = 1) { admission.pendingConditionalTriggers("order-0500", 500) }
        coVerify(exactly = 0) {
            admission.activateIfTriggered(any(), any(), any(), any(), any())
        }
    }

    private fun worker(
        admission: OrderAdmissionService,
        readiness: ConditionalOrderTriggerReadiness,
        requests: MutableList<URI>,
        body: String,
    ) = ConditionalOrderTriggerWorker(
        admission = admission,
        readiness = readiness,
        webClient = WebClient.builder()
            .exchangeFunction(ExchangeFunction { request ->
                requests += request.url()
                Mono.just(
                    ClientResponse.create(HttpStatus.OK)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .body(body)
                        .build(),
                )
            })
            .build(),
        marketBaseUrl = "http://market/",
        maxTradeAgeMs = 10_000,
        marketTimeoutMs = 3_000,
        readinessSymbols = "BTC_USDT",
    )

    private fun candidate(ouid: String) = OrderSubmitRequestEvent(
        ouid = ouid,
        uuid = "reserve-$ouid",
        pair = Pair("BTC", "USDT"),
        price = 100,
        quantity = 1,
        direction = OrderDirection.BID,
        matchConstraint = MatchConstraint.GTC,
        orderType = OrderType.LIMIT_ORDER,
        userLevel = "MAIN",
        requiresReservation = true,
        reservationConfirmed = true,
        conditionalAction = "HOLD",
        stopPrice = BigDecimal("105"),
    )
}
