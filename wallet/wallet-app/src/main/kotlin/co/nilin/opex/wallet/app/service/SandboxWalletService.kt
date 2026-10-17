package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.app.dto.SandboxAvailability
import co.nilin.opex.wallet.app.dto.SandboxCapabilitiesResponse
import co.nilin.opex.wallet.app.dto.SandboxOperationRequest
import co.nilin.opex.wallet.app.dto.SandboxOperationResponse
import co.nilin.opex.wallet.core.inout.Deposit
import co.nilin.opex.wallet.core.inout.TransferCommand
import co.nilin.opex.wallet.core.inout.TransferMethod
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.DepositStatus
import co.nilin.opex.wallet.core.model.DepositType
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.model.SandboxOperationStatus
import co.nilin.opex.wallet.core.model.SandboxOperationType
import co.nilin.opex.wallet.core.model.SandboxWalletOperation
import co.nilin.opex.wallet.core.model.TransferCategory
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.model.Withdraw
import co.nilin.opex.wallet.core.model.WithdrawStatus
import co.nilin.opex.wallet.core.model.WithdrawType
import co.nilin.opex.wallet.core.service.PrecisionService
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.DepositPersister
import co.nilin.opex.wallet.core.spi.SandboxWalletOperationPersister
import co.nilin.opex.wallet.core.spi.TransferManager
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import co.nilin.opex.wallet.core.spi.WithdrawPersister
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.Date
import java.util.UUID
import java.nio.charset.StandardCharsets

@Service
class SandboxWalletService(
    private val walletOwnerManager: WalletOwnerManager,
    private val walletManager: WalletManager,
    private val currencyManager: CurrencyServiceManager,
    private val precisionService: PrecisionService,
    private val transferManager: TransferManager,
    private val depositPersister: DepositPersister,
    private val withdrawPersister: WithdrawPersister,
    private val operationPersister: SandboxWalletOperationPersister,
    private val provider: ObjectProvider<SandboxTestProvider>,
    @Value("\${app.wallet.sandbox.enabled:false}") private val enabled: Boolean,
    @Value("\${app.wallet.sandbox.funding-owner-uuid:}") private val fundingOwnerUuid: String,
) {
    suspend fun capabilities(): SandboxCapabilitiesResponse {
        val configured = fundingOwnerUuid.isNotBlank()
        val fundingOwner = if (configured) walletOwnerManager.findWalletOwner(fundingOwnerUuid) else null
        val availableCurrencies = fundingOwner?.let { owner ->
            walletManager.findWalletsByOwnerAndType(owner, WalletType.MAIN)
                .map { it.currency.symbol }
                .distinct()
                .sorted()
        } ?: emptyList()
        val reason = unavailableReason(configured, fundingOwner != null)
        val capability = SandboxAvailability(available = reason == null, reason = reason)
        return SandboxCapabilitiesResponse(
            enabled = enabled,
            deposit = capability,
            withdrawal = capability,
            fundingOwnerConfigured = configured,
            fundingOwnerFound = fundingOwner != null,
            availableCurrencies = availableCurrencies,
        )
    }

    @Transactional
    suspend fun createDeposit(ownerUuid: String, request: SandboxOperationRequest): SandboxOperationResponse {
        validateRequest(request)
        operationPersister.acquireIdempotencyLock(ownerUuid, request.idempotencyKey.trim())
        findReplay(ownerUuid, request, SandboxOperationType.DEPOSIT)?.let { return it }
        val fundingOwner = requireReadyFundingOwner()
        val userOwner = walletOwnerManager.findWalletOwner(ownerUuid)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "USER_WALLET_OWNER_NOT_FOUND")
        if (fundingOwner.uuid == userOwner.uuid) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "FUNDING_OWNER_CANNOT_RECEIVE_SANDBOX_DEPOSIT")
        }

        val currency = currencyManager.fetchCurrency(FetchCurrency(symbol = request.currency))
            ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "CURRENCY_NOT_FOUND")
        precisionService.validatePrecision(request.amount, currency.symbol)
        val fundingWallet = walletManager.findWalletByOwnerAndCurrencyAndType(fundingOwner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "FUNDING_WALLET_NOT_FOUND")
        val userWallet = walletManager.findWalletByOwnerAndCurrencyAndType(userOwner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "USER_WALLET_NOT_FOUND")

        val operationId = stableOperationId(ownerUuid, request.idempotencyKey)
        authorizeProvider(operationId, SandboxOperationType.DEPOSIT, currency.symbol, request.amount)
        val transfer = transferManager.transfer(
            TransferCommand(
                sourceWallet = fundingWallet,
                destWallet = userWallet,
                amount = Amount(currency, request.amount),
                description = "Sandbox test deposit $operationId; no external transfer occurred",
                transferRef = "wallet:sandbox:deposit:$operationId",
                transferCategory = TransferCategory.DEPOSIT_MANUALLY,
            )
        )
        val now = LocalDateTime.now(ZoneOffset.UTC)
        depositPersister.persist(
            Deposit(
                ownerUuid = ownerUuid,
                depositUuid = operationId,
                currency = currency.symbol,
                amount = request.amount,
                network = null,
                sourceAddress = null,
                transactionRef = "wallet:sandbox:deposit:$operationId",
                note = "Sandbox test deposit; no bank or blockchain transfer occurred",
                status = DepositStatus.DONE,
                depositType = DepositType.OFF_CHAIN,
                attachment = null,
                createDate = Date(),
                transferMethod = TransferMethod.MANUALLY,
            )
        )
        return operationPersister.persist(
            SandboxWalletOperation(
                id = operationId,
                ownerUuid = ownerUuid,
                idempotencyKey = request.idempotencyKey.trim(),
                type = SandboxOperationType.DEPOSIT,
                currency = currency.symbol,
                amount = request.amount,
                status = SandboxOperationStatus.COMPLETED,
                ledgerTransactionId = transfer.tx,
                withdrawalUuid = null,
                createdAt = now,
            )
        ).toResponse()
    }

    @Transactional
    suspend fun createWithdrawal(ownerUuid: String, request: SandboxOperationRequest): SandboxOperationResponse {
        validateRequest(request)
        operationPersister.acquireIdempotencyLock(ownerUuid, request.idempotencyKey.trim())
        findReplay(ownerUuid, request, SandboxOperationType.WITHDRAWAL)?.let { return it }
        val fundingOwner = requireReadyFundingOwner()
        val userOwner = walletOwnerManager.findWalletOwner(ownerUuid)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "USER_WALLET_OWNER_NOT_FOUND")
        if (fundingOwner.uuid == userOwner.uuid) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "FUNDING_OWNER_CANNOT_WITHDRAW_TO_ITSELF")
        }

        val currency = currencyManager.fetchCurrency(FetchCurrency(symbol = request.currency))
            ?: throw ResponseStatusException(HttpStatus.BAD_REQUEST, "CURRENCY_NOT_FOUND")
        precisionService.validatePrecision(request.amount, currency.symbol)
        val userMainWallet = walletManager.findWalletByOwnerAndCurrencyAndType(userOwner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.UNPROCESSABLE_ENTITY, "USER_WALLET_NOT_FOUND")
        val fundingWallet = walletManager.findWalletByOwnerAndCurrencyAndType(fundingOwner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "FUNDING_WALLET_NOT_FOUND")

        val operationId = stableOperationId(ownerUuid, request.idempotencyKey)
        authorizeProvider(operationId, SandboxOperationType.WITHDRAWAL, currency.symbol, request.amount)
        val cashoutWallet = walletManager.findWalletByOwnerAndCurrencyAndType(userOwner, WalletType.CASHOUT, currency)
            ?: walletManager.createCashoutWallet(userOwner, currency)
        val reserve = transferManager.transfer(
            TransferCommand(
                sourceWallet = userMainWallet,
                destWallet = cashoutWallet,
                amount = Amount(currency, request.amount),
                description = "Sandbox test withdrawal reservation $operationId",
                transferRef = "wallet:sandbox:withdrawal:$operationId:reserve",
                transferCategory = TransferCategory.WITHDRAW_REQUEST,
            )
        )
        val reservedCashoutWallet = walletManager.findWalletByOwnerAndCurrencyAndType(
            userOwner,
            WalletType.CASHOUT,
            currency,
        ) ?: throw ResponseStatusException(HttpStatus.INTERNAL_SERVER_ERROR, "CASHOUT_WALLET_NOT_FOUND")
        val settlement = transferManager.transfer(
            TransferCommand(
                sourceWallet = reservedCashoutWallet,
                destWallet = fundingWallet,
                amount = Amount(currency, request.amount),
                description = "Sandbox test withdrawal settlement $operationId; no external transfer occurred",
                transferRef = "wallet:sandbox:withdrawal:$operationId:settle",
                transferCategory = TransferCategory.WITHDRAW_ACCEPT,
            )
        )

        val now = LocalDateTime.now(ZoneOffset.UTC)
        val withdrawalUuid = operationId
        withdrawPersister.persist(
            Withdraw(
                withdrawId = null,
                withdrawUuid = withdrawalUuid,
                ownerUuid = ownerUuid,
                currency = currency.symbol,
                wallet = reservedCashoutWallet.id!!,
                amount = request.amount,
                requestTransaction = reserve.tx,
                finalizedTransaction = settlement.tx,
                appliedFee = BigDecimal.ZERO,
                destAmount = request.amount,
                destSymbol = currency.symbol,
                destAddress = null,
                destNetwork = null,
                destNote = "Sandbox test settlement; no external transfer occurred",
                destTransactionRef = null,
                statusReason = null,
                status = WithdrawStatus.DONE,
                applicator = null,
                withdrawType = WithdrawType.OFF_CHAIN,
                attachment = null,
                createDate = now,
                lastUpdateDate = now,
                transferMethod = TransferMethod.MANUALLY,
                otpRequired = 0,
            )
        )
        return operationPersister.persist(
            SandboxWalletOperation(
                id = operationId,
                ownerUuid = ownerUuid,
                idempotencyKey = request.idempotencyKey.trim(),
                type = SandboxOperationType.WITHDRAWAL,
                currency = currency.symbol,
                amount = request.amount,
                status = SandboxOperationStatus.COMPLETED,
                ledgerTransactionId = settlement.tx,
                withdrawalUuid = withdrawalUuid,
                createdAt = now,
            )
        ).toResponse()
    }

    suspend fun listOperations(ownerUuid: String, limit: Int, offset: Int): List<SandboxOperationResponse> {
        if (limit !in 1..500 || offset < 0) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_PAGE")
        }
        return operationPersister.findRecentByOwner(ownerUuid, limit, offset).map { it.toResponse() }
    }

    suspend fun getOperation(ownerUuid: String, id: String): SandboxOperationResponse =
        operationPersister.findByIdAndOwner(id, ownerUuid)?.toResponse()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "SANDBOX_OPERATION_NOT_FOUND")

    private suspend fun validateRequest(request: SandboxOperationRequest) {
        val key = request.idempotencyKey.trim()
        if (key.isEmpty() || key.length > 128) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_IDEMPOTENCY_KEY")
        }
        if (request.currency.isBlank() || request.currency.length > 64) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_CURRENCY")
        }
        val integerDigits = request.amount.precision() - request.amount.scale()
        if (request.amount <= BigDecimal.ZERO || request.amount.scale().coerceAtLeast(0) > 18 || integerDigits > 18) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "INVALID_AMOUNT")
        }
    }

    private suspend fun findReplay(
        ownerUuid: String,
        request: SandboxOperationRequest,
        type: SandboxOperationType,
    ): SandboxOperationResponse? {
        val existing = operationPersister.findByOwnerAndIdempotencyKey(ownerUuid, request.idempotencyKey.trim())
            ?: return null
        if (existing.type != type || !existing.currency.equals(request.currency, ignoreCase = true) ||
            existing.amount.compareTo(request.amount) != 0
        ) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "IDEMPOTENCY_KEY_REUSED")
        }
        return existing.toResponse()
    }

    private suspend fun requireReadyFundingOwner() = when {
        !enabled -> throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_DISABLED")
        provider.getIfAvailable() == null ->
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_PROVIDER_UNAVAILABLE")
        fundingOwnerUuid.isBlank() ->
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "FUNDING_OWNER_NOT_CONFIGURED")
        else -> walletOwnerManager.findWalletOwner(fundingOwnerUuid)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "FUNDING_OWNER_NOT_FOUND")
    }

    private suspend fun authorizeProvider(
        operationId: String,
        type: SandboxOperationType,
        currency: String,
        amount: BigDecimal,
    ) {
        val request = SandboxProviderRequest(operationId, type, currency, amount)
        val result = provider.getIfAvailable()?.submit(request)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_PROVIDER_UNAVAILABLE")
        if (result.provider != "SANDBOX" || result.environment != "TEST") {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_PROVIDER_IDENTITY_INVALID")
        }
        when (result.disposition) {
            SandboxProviderDisposition.CONFIRMED -> Unit
            SandboxProviderDisposition.PENDING ->
                throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_PROVIDER_PENDING")
            SandboxProviderDisposition.REJECTED ->
                throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "SANDBOX_PROVIDER_REJECTED")
        }
    }

    private fun stableOperationId(ownerUuid: String, idempotencyKey: String): String =
        UUID.nameUUIDFromBytes("$ownerUuid:${idempotencyKey.trim()}".toByteArray(StandardCharsets.UTF_8)).toString()

    private suspend fun unavailableReason(configured: Boolean, found: Boolean): String? = when {
        !enabled -> "SANDBOX_DISABLED"
        provider.getIfAvailable() == null -> "SANDBOX_PROVIDER_UNAVAILABLE"
        !configured -> "FUNDING_OWNER_NOT_CONFIGURED"
        !found -> "FUNDING_OWNER_NOT_FOUND"
        else -> null
    }

    private fun SandboxWalletOperation.toResponse() = SandboxOperationResponse(
        id = id,
        operationId = id,
        idempotencyKey = idempotencyKey,
        type = type.name,
        state = status.name,
        status = when (type) {
            SandboxOperationType.DEPOSIT -> "TEST_CREDITED"
            SandboxOperationType.WITHDRAWAL -> "TEST_SETTLED"
        },
        currency = currency,
        amount = amount.stripTrailingZeros().toPlainString(),
        ledgerTransactionId = ledgerTransactionId,
        withdrawalUuid = withdrawalUuid,
        createdAt = createdAt.toInstant(ZoneOffset.UTC).toString(),
    )
}
