package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.gateway.app.spi.AccountantReadinessProxy
import co.nilin.opex.matching.gateway.ports.postgres.dto.OrderIntent
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import io.mockk.coEvery
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class OrderIntentReadServiceTest {
    private val store = mockk<OrderIntentStore>()
    private val accountantReadiness = mockk<AccountantReadinessProxy>()
    private val service = OrderIntentReadService(store, accountantReadiness)

    private fun intent(status: String) = OrderIntent(
        owner = "owner",
        clientOrderId = "client-$status",
        ouid = "ouid-$status",
        payloadHash = "hash",
        symbol = "BTC_USDT",
        type = "STOP_LIMIT",
        side = "SELL",
        quantity = BigDecimal("1.2500"),
        price = BigDecimal("10.5000"),
        postOnly = false,
        leftFraction = BigDecimal("0.0001"),
        payload = "{}",
        status = status,
        stopPrice = BigDecimal("10.2500"),
    )

    @Test
    fun omitsConditionalTypeWhenAccountantReadinessIsUnavailable() = runBlocking {
        coEvery { accountantReadiness.conditionalOrdersReady() } throws IllegalStateException("accountant unavailable")

        assertEquals(listOf("LIMIT", "MARKET"), service.capabilities().supportedTypes)
    }

    @Test
    fun includesConditionalTypeOnlyWhenAccountantReportsFreshReadiness() = runBlocking {
        coEvery { accountantReadiness.conditionalOrdersReady() } returns true

        assertEquals(listOf("LIMIT", "MARKET", "STOP_LIMIT"), service.capabilities().supportedTypes)
    }

    @Test
    fun acceptedIntentDoesNotInventExecutionQuantities() = runBlocking {
        coEvery { store.metadata("owner") } returns listOf(intent("ACCEPTED_PENDING"), intent("TRIGGERED"))

        val metadata = service.metadata("owner")

        assertEquals("SELL", metadata.first().side)
        assertNull(metadata.first().filledQuantity)
        assertNull(metadata.first().remainingQuantity)
        assertNull(metadata.last().filledQuantity)
        assertNull(metadata.last().remainingQuantity)
    }

    @Test
    fun waitingTriggerReportsFullQuantityAsRemaining() = runBlocking {
        coEvery { store.metadata("owner") } returns listOf(intent("WAITING_TRIGGER"))

        val metadata = service.metadata("owner").single()

        assertEquals("0", metadata.filledQuantity)
        assertEquals("1.2500", metadata.remainingQuantity)
        assertEquals("WAITING_TRIGGER", metadata.status)
    }

    @Test
    fun outcomeMetadataUsesPersistedFillAndDerivedRemainingQuantities() = runBlocking {
        coEvery { store.metadata("owner") } returns listOf(
            intent("PARTIALLY_FILLED").copy(filledQuantity = BigDecimal("0.2500"))
        )

        val metadata = service.metadata("owner").single()

        assertEquals("0.2500", metadata.filledQuantity)
        assertEquals("1.0000", metadata.remainingQuantity)
    }

    @Test
    fun cancelRequestedNonterminalIntentReportsPendingAndKeepsExecutionQuantities() = runBlocking {
        coEvery { store.metadata("owner") } returns listOf(
            intent("PARTIALLY_FILLED").copy(
                cancelRequested = true,
                filledQuantity = BigDecimal("0.2500"),
            )
        )

        val metadata = service.metadata("owner").single()

        assertEquals("CANCEL_PENDING", metadata.status)
        assertEquals("0.2500", metadata.filledQuantity)
        assertEquals("1.0000", metadata.remainingQuantity)
    }

    @Test
    fun cancelRequestedTerminalIntentKeepsItsFinalStatus() = runBlocking {
        val terminalStatuses = listOf("FILLED", "CANCELED", "REJECTED")
        coEvery { store.metadata("owner") } returns terminalStatuses.map { status ->
            intent(status).copy(cancelRequested = true)
        }

        val metadata = service.metadata("owner")

        assertEquals(terminalStatuses, metadata.map { it.status })
    }
}
