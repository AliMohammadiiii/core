package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.gateway.app.inout.OrderIntentRequest
import co.nilin.opex.matching.gateway.app.spi.AccountantReadinessProxy
import co.nilin.opex.matching.gateway.app.spi.PairConfigLoader
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException

class OrderIntentServiceReadinessTest {
    @Test
    fun `stop limit is rejected before acceptance when accountant reports not ready`() {
        val store = mockk<OrderIntentStore>()
        coEvery { store.find("owner", "client-1", null) } returns null
        val readiness = mockk<AccountantReadinessProxy>()
        coEvery { readiness.conditionalOrdersReady() } returns false
        val service = service(store, readiness)

        val failure = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.submit("owner", stopLimit()) }
        }

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, failure.status)
        coVerify(exactly = 0) { store.accept(any()) }
    }

    @Test
    fun `stop limit is rejected before acceptance when readiness check fails`() {
        val store = mockk<OrderIntentStore>()
        coEvery { store.find("owner", "client-1", null) } returns null
        val readiness = mockk<AccountantReadinessProxy>()
        coEvery { readiness.conditionalOrdersReady() } throws IllegalStateException("accountant unavailable")
        val service = service(store, readiness)

        val failure = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.submit("owner", stopLimit()) }
        }

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, failure.status)
        coVerify(exactly = 0) { store.accept(any()) }
    }

    private fun service(store: OrderIntentStore, readiness: AccountantReadinessProxy) = OrderIntentService(
        store = store,
        abandonStore = mockk(relaxed = true),
        pairs = mockk(relaxed = true),
        config = mockk<PairConfigLoader>(relaxed = true),
        mapper = jacksonObjectMapper(),
        accountantReadiness = readiness,
    )

    private fun stopLimit() = OrderIntentRequest(
        clientOrderId = "client-1",
        symbol = "BTC_USDT",
        type = "STOP_LIMIT",
        side = "BUY",
        quantity = java.math.BigDecimal("1"),
        price = java.math.BigDecimal("100"),
        stopPrice = java.math.BigDecimal("105"),
    )
}
