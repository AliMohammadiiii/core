package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.app.dto.SandboxOperationRequest
import co.nilin.opex.wallet.app.service.SandboxProviderDisposition.PENDING
import co.nilin.opex.wallet.app.service.SandboxProviderResult
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.SandboxOperationStatus
import co.nilin.opex.wallet.core.model.SandboxOperationType
import co.nilin.opex.wallet.core.model.SandboxWalletOperation
import co.nilin.opex.wallet.core.model.Wallet
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.spi.*
import co.nilin.opex.wallet.core.service.PrecisionService
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.ObjectProvider
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.time.LocalDateTime

class SandboxWalletServiceTest {
    private val walletOwnerManager = mockk<WalletOwnerManager>(relaxed = true)
    private val walletManager = mockk<WalletManager>(relaxed = true)
    private val currencyManager = mockk<CurrencyServiceManager>(relaxed = true)
    private val precisionService = mockk<PrecisionService>(relaxed = true)
    private val transferManager = mockk<TransferManager>(relaxed = true)
    private val depositPersister = mockk<DepositPersister>(relaxed = true)
    private val withdrawPersister = mockk<WithdrawPersister>(relaxed = true)
    private val operationPersister = mockk<SandboxWalletOperationPersister>(relaxed = true)
    private val provider = mockk<SandboxTestProvider>(relaxed = true)
    private val providerProvider = mockk<ObjectProvider<SandboxTestProvider>>()

    @BeforeEach
    fun defaultToNoExistingSandboxOperation() {
        coEvery { operationPersister.findByOwnerAndIdempotencyKey(any(), any()) } returns null
    }

    private val validRequest = SandboxOperationRequest("key-1", "BTC", BigDecimal("1.25"))

    @Test
    fun `rejects non-positive amount before reading or changing wallet state`() {
        val service = service(enabled = true, fundingOwnerUuid = "funding-owner")

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.createDeposit("user-owner", validRequest.copy(amount = BigDecimal.ZERO)) }
        }

        assertEquals(HttpStatus.BAD_REQUEST, exception.status)
        coVerify(exactly = 0) { operationPersister.findByOwnerAndIdempotencyKey(any(), any()) }
        coVerify(exactly = 0) { transferManager.transfer(any()) }
    }

    @Test
    fun `returns prior receipt for duplicate idempotency request`() = runBlocking {
        val prior = operation()
        coEvery { operationPersister.findByOwnerAndIdempotencyKey("user-owner", "key-1") } returns prior
        val service = service(enabled = false, fundingOwnerUuid = "")

        val response = service.createDeposit("user-owner", validRequest)

        assertEquals(prior.id, response.id)
        assertEquals("TEST_CREDITED", response.status)
        assertEquals("1.25", response.amount)
        assertEquals("key-1", response.idempotencyKey)
        coVerify(exactly = 0) { transferManager.transfer(any()) }
    }

    @Test
    fun `rejects idempotency key reused with a different payload`() {
        coEvery { operationPersister.findByOwnerAndIdempotencyKey("user-owner", "key-1") } returns operation()
        val service = service(enabled = true, fundingOwnerUuid = "funding-owner")

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking {
                service.createDeposit("user-owner", validRequest.copy(amount = BigDecimal("1.26")))
            }
        }

        assertEquals(HttpStatus.CONFLICT, exception.status)
        assertEquals("IDEMPOTENCY_KEY_REUSED", exception.reason)
        coVerify(exactly = 0) { transferManager.transfer(any()) }
    }

    @Test
    fun `returns 404 for operation not owned by requesting user`() {
        coEvery { operationPersister.findByIdAndOwner("somebody-elses-id", "user-owner") } returns null
        val service = service(enabled = true, fundingOwnerUuid = "funding-owner")

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.getOperation("user-owner", "somebody-elses-id") }
        }

        assertEquals(HttpStatus.NOT_FOUND, exception.status)
        assertEquals("SANDBOX_OPERATION_NOT_FOUND", exception.reason)
    }

    @Test
    fun `reports unavailable provider without creating a wallet transfer`() {
        val service = service(enabled = true, fundingOwnerUuid = "funding-owner", availableProvider = null)

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.createDeposit("user-owner", validRequest) }
        }

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.status)
        assertEquals("SANDBOX_PROVIDER_UNAVAILABLE", exception.reason)
        coVerify(exactly = 0) { transferManager.transfer(any()) }
    }

    @Test
    fun `reports missing funding owner configuration without creating a wallet transfer`() {
        val service = service(enabled = true, fundingOwnerUuid = "")

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.createDeposit("user-owner", validRequest) }
        }

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.status)
        assertEquals("FUNDING_OWNER_NOT_CONFIGURED", exception.reason)
        coVerify(exactly = 0) { transferManager.transfer(any()) }
    }

    @Test
    fun `does not create a ledger transfer while provider result is pending`() {
        val fundingOwner = WalletOwner(null, "funding-owner", "Funding", "system", true, true, true)
        val userOwner = WalletOwner(null, "user-owner", "User", "user", true, true, true)
        val currency = CurrencyCommand("BTC", name = "BTC", precision = BigDecimal("8"))
        val fundingWallet = Wallet(1, fundingOwner, Amount(currency, BigDecimal("100")), currency, WalletType.MAIN, null)
        val userWallet = Wallet(2, userOwner, Amount(currency, BigDecimal("10")), currency, WalletType.MAIN, null)
        coEvery { walletOwnerManager.findWalletOwner("funding-owner") } returns fundingOwner
        coEvery { walletOwnerManager.findWalletOwner("user-owner") } returns userOwner
        coEvery { currencyManager.fetchCurrency(any()) } returns currency
        coEvery { walletManager.findWalletByOwnerAndCurrencyAndType(fundingOwner, WalletType.MAIN, currency) } returns fundingWallet
        coEvery { walletManager.findWalletByOwnerAndCurrencyAndType(userOwner, WalletType.MAIN, currency) } returns userWallet
        coEvery { operationPersister.findByOwnerAndIdempotencyKey("user-owner", "key-1") } returns null
        coEvery { provider.submit(any()) } returns SandboxProviderResult("SANDBOX", "TEST", PENDING)
        val service = service(enabled = true, fundingOwnerUuid = "funding-owner")

        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { service.createDeposit("user-owner", validRequest) }
        }

        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, exception.status)
        assertEquals("SANDBOX_PROVIDER_PENDING", exception.reason)
        coVerify(exactly = 0) { transferManager.transfer(any()) }
        coVerify(exactly = 0) { depositPersister.persist(any()) }
        coVerify(exactly = 0) { operationPersister.persist(any()) }
    }

    private fun service(
        enabled: Boolean,
        fundingOwnerUuid: String,
        availableProvider: SandboxTestProvider? = provider,
    ): SandboxWalletService {
        every { providerProvider.getIfAvailable() } returns availableProvider
        return SandboxWalletService(
            walletOwnerManager = walletOwnerManager,
            walletManager = walletManager,
            currencyManager = currencyManager,
            precisionService = precisionService,
            transferManager = transferManager,
            depositPersister = depositPersister,
            withdrawPersister = withdrawPersister,
            operationPersister = operationPersister,
            provider = providerProvider,
            enabled = enabled,
            fundingOwnerUuid = fundingOwnerUuid,
        )
    }

    private fun operation() = SandboxWalletOperation(
        id = "operation-1",
        ownerUuid = "user-owner",
        idempotencyKey = "key-1",
        type = SandboxOperationType.DEPOSIT,
        currency = "BTC",
        amount = BigDecimal("1.25"),
        status = SandboxOperationStatus.COMPLETED,
        ledgerTransactionId = "9007199254740993",
        withdrawalUuid = null,
        createdAt = LocalDateTime.parse("2026-10-16T12:00:00"),
    )
}
