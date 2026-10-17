package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.app.dto.CustodyLedgerActionRequest
import co.nilin.opex.wallet.app.dto.CustodyLedgerActionResponse
import co.nilin.opex.wallet.app.dto.CustodyLedgerAssetCapability
import co.nilin.opex.wallet.app.dto.CustodyLedgerBalanceResponse
import co.nilin.opex.wallet.app.dto.CustodyLedgerBootstrapResponse
import co.nilin.opex.wallet.app.dto.CustodyLedgerCapabilitiesResponse
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.inout.TransferCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.model.TransferCategory
import co.nilin.opex.wallet.core.model.Wallet
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.TransferManager
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import co.nilin.opex.wallet.core.service.PrecisionService
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.transaction.annotation.Transactional
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.security.MessageDigest
import java.util.UUID

enum class CustodyLedgerActionKind {
    CREDIT,
    RESERVE,
    RELEASE,
    SETTLE,
}

class CustodyLedgerInsufficientFundsException(
    val operationId: String,
    val ownerUuid: String,
    val asset: String,
    val amount: String,
    val kind: CustodyLedgerActionKind,
    val referenceOperationId: String?,
    val payloadHash: String,
) : RuntimeException("CUSTODY_LEDGER_INSUFFICIENT_FUNDS")

@Service
class CustodyLedgerService(
    private val walletOwnerManager: WalletOwnerManager,
    private val walletManager: WalletManager,
    private val currencyServiceManager: CurrencyServiceManager,
    private val precisionService: PrecisionService,
    private val transferManager: TransferManager,
    private val actionRepository: CustodyLedgerActionRepository,
    @Value("\${app.wallet.custody.ledger.enabled:false}") private val enabled: Boolean,
    @Value("\${app.wallet.custody.ledger.bootstrap-enabled:false}") private val bootstrapEnabled: Boolean,
    @Value("\${app.wallet.custody.ledger.key:}") private val configuredKey: String,
    @Value("\${app.wallet.custody.ledger.treasury-owner-uuid:}") private val treasuryOwnerUuidValue: String,
) {
    fun authorize(candidateKey: String?) {
        if (configuredKey.isBlank()) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_LEDGER_KEY_NOT_CONFIGURED")
        }
        if (candidateKey.isNullOrBlank() || !MessageDigest.isEqual(
                configuredKey.toByteArray(Charsets.UTF_8),
                candidateKey.toByteArray(Charsets.UTF_8),
            )
        ) {
            throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "CUSTODY_LEDGER_UNAUTHORIZED")
        }
    }

    @Transactional
    suspend fun apply(request: CustodyLedgerActionRequest): CustodyLedgerActionResponse {
        requireEnabled()
        val operationId = normalizeUuid(request.operationId, "operationId")
        val ownerUuid = normalizeUuid(request.ownerUuid, "ownerUuid")
        val asset = normalizeAsset(request.asset)
        val amount = parseAmount(request.amount)
        val referenceOperationId = request.referenceOperationId?.let { normalizeUuid(it, "referenceOperationId") }
        validateKindReference(request.kind, referenceOperationId)
        if (ownerUuid == walletOwnerManager.systemUuid) {
            throw badRequest("CUSTODY_LEDGER_SYSTEM_OWNER_FORBIDDEN")
        }

        val payloadHash = CustodyLedgerPayloadHash.calculate(
            operationId,
            ownerUuid,
            asset,
            amount,
            request.kind,
            referenceOperationId,
        )
        if (!HASH_PATTERN.matches(request.payloadHash) || request.payloadHash != payloadHash) {
            throw badRequest("CUSTODY_LEDGER_PAYLOAD_HASH_MISMATCH")
        }

        val insertedOperationId = actionRepository.insertIfAbsent(
            operationId,
            ownerUuid,
            asset,
            amount,
            request.kind.name,
            referenceOperationId,
            payloadHash,
        ).awaitSingleOrNull()

        if (insertedOperationId == null) {
            val existing = actionRepository.findById(operationId).awaitSingleOrNull()
                ?: throw IllegalStateException("Custody idempotency row disappeared after conflict")
            if (existing.payloadHash != payloadHash) {
                throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_OPERATION_ID_CONFLICT")
            }
            if (existing.status != APPLIED_STATUS || existing.transactionId == null) {
                throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_OPERATION_NOT_APPLIED")
            }
            return existing.toResponse()
        }

        val currency = currency(asset)
        precisionService.validatePrecision(amount, asset)
        val owner = walletOwnerManager.findWalletOwner(ownerUuid)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_OWNER_NOT_FOUND")
        val treasuryOwner = treasuryOwner()
        val transfer = when (request.kind) {
            CustodyLedgerActionKind.CREDIT -> credit(
                operationId, owner, treasuryOwner, currency, amount, referenceOperationId, payloadHash,
            )
            CustodyLedgerActionKind.RESERVE -> reserve(
                operationId, owner, currency, amount, referenceOperationId, payloadHash,
            )
            CustodyLedgerActionKind.RELEASE -> releaseOrSettle(
                operationId, owner, treasuryOwner, currency, amount, referenceOperationId!!,
                payloadHash, request.kind,
            )
            CustodyLedgerActionKind.SETTLE -> releaseOrSettle(
                operationId, owner, treasuryOwner, currency, amount, referenceOperationId!!,
                payloadHash, request.kind,
            )
        }

        val transactionId = transfer.toLongOrNull()
            ?: throw IllegalStateException("Native wallet transfer returned a non-numeric transaction id")
        val marked = actionRepository.markApplied(operationId, transactionId).awaitSingle()
        if (marked != 1) throw IllegalStateException("Custody action receipt could not be marked APPLIED")

        return CustodyLedgerActionResponse(
            operationId = operationId,
            ownerUuid = ownerUuid,
            asset = asset,
            amount = canonicalAmount(amount),
            kind = request.kind,
            referenceOperationId = referenceOperationId,
            payloadHash = payloadHash,
            status = APPLIED_STATUS,
            transactionId = transactionId.toString(),
        )
    }

    suspend fun getAction(operationIdValue: String): CustodyLedgerActionResponse {
        requireEnabled()
        val operationId = normalizeUuid(operationIdValue, "operationId")
        val action = actionRepository.findById(operationId).awaitSingleOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_ACTION_NOT_FOUND")
        if (action.status != APPLIED_STATUS || action.transactionId == null) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_ACTION_NOT_FOUND")
        }
        return action.toResponse()
    }

    suspend fun balance(ownerUuidValue: String, assetValue: String): CustodyLedgerBalanceResponse {
        requireEnabled()
        val ownerUuid = normalizeUuid(ownerUuidValue, "ownerUuid")
        val asset = normalizeAsset(assetValue)
        if (ownerUuid == walletOwnerManager.systemUuid) {
            throw badRequest("CUSTODY_LEDGER_SYSTEM_OWNER_FORBIDDEN")
        }
        val owner = walletOwnerManager.findWalletOwner(ownerUuid)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_OWNER_NOT_FOUND")
        val currency = currency(asset)
        val mainWallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
        val cashoutWallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.CASHOUT, currency)
        val available = mainWallet?.balance?.amount ?: BigDecimal.ZERO
        val reserved = cashoutWallet?.balance?.amount ?: BigDecimal.ZERO
        return CustodyLedgerBalanceResponse(
            ownerUuid = ownerUuid,
            asset = asset,
            available = canonicalAmount(available),
            reserved = canonicalAmount(reserved),
            total = canonicalAmount(available.add(reserved)),
        )
    }

    suspend fun capabilities(): CustodyLedgerCapabilitiesResponse {
        val configuredOwnerUuid = treasuryOwnerUuid()
        val treasuryOwner = configuredOwnerUuid?.let { walletOwnerManager.findWalletOwner(it) }
        val ledgerAvailable = enabled && configuredKey.isNotBlank()
        val assets = ASSETS.map { asset ->
            val currency = currencyServiceManager.fetchCurrency(FetchCurrency(symbol = asset))
            val treasuryWallet = if (currency != null && treasuryOwner != null) {
                walletManager.findWalletByOwnerAndCurrencyAndType(treasuryOwner, WalletType.MAIN, currency)
            } else {
                null
            }
            val fundedAmount = treasuryWallet?.balance?.amount ?: BigDecimal.ZERO
            val reason = when {
                !ledgerAvailable -> "CUSTODY_LEDGER_DISABLED"
                currency == null -> "CUSTODY_TEST_ASSET_NOT_PROVISIONED"
                treasuryOwner == null -> "CUSTODY_TREASURY_OWNER_NOT_PROVISIONED"
                treasuryWallet == null -> "CUSTODY_TREASURY_WALLET_NOT_PROVISIONED"
                fundedAmount <= BigDecimal.ZERO -> "CUSTODY_TREASURY_UNFUNDED"
                else -> null
            }
            CustodyLedgerAssetCapability(
                asset = asset,
                currencyConfigured = currency != null,
                treasuryWalletConfigured = treasuryWallet != null,
                fundedAmount = canonicalAmount(fundedAmount),
                creditAvailable = ledgerAvailable && currency != null && treasuryWallet != null && fundedAmount > BigDecimal.ZERO,
                reserveAvailable = ledgerAvailable && currency != null && treasuryOwner != null,
                reason = reason,
            )
        }
        return CustodyLedgerCapabilitiesResponse(
            enabled = enabled && configuredKey.isNotBlank(),
            bootstrapEnabled = bootstrapEnabled,
            treasuryOwnerConfigured = configuredOwnerUuid != null,
            treasuryOwnerFound = treasuryOwner != null,
            assets = assets,
        )
    }

    @Transactional
    suspend fun bootstrapAssets(): CustodyLedgerBootstrapResponse {
        requireEnabled()
        if (!bootstrapEnabled) {
            throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_BOOTSTRAP_DISABLED")
        }
        val ownerUuid = treasuryOwnerUuid()
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_OWNER_NOT_CONFIGURED")
        if (ownerUuid == walletOwnerManager.systemUuid) {
            throw badRequest("CUSTODY_TREASURY_OWNER_CANNOT_BE_SYSTEM")
        }

        val createdCurrencies = mutableListOf<String>()
        val createdWallets = mutableListOf<String>()
        for (asset in ASSETS) {
            val before = currencyServiceManager.fetchCurrency(FetchCurrency(symbol = asset))
            if (before == null) {
                currencyServiceManager.createNewCurrency(
                    CurrencyCommand(
                        symbol = asset,
                        name = asset,
                        title = if (asset == LOCAL_ASSET) "Local EVM Test ETH" else "Sepolia Test ETH",
                        alias = "Test-only custody asset",
                        precision = ETH_PRECISION,
                        isTransitive = false,
                        isActive = true,
                        withdrawAllowed = false,
                        depositAllowed = false,
                    ),
                    ignoreIfExist = true,
                )
                createdCurrencies.add(asset)
            }
            val currency = currency(asset)
            if (currency.precision.compareTo(ETH_PRECISION) != 0 || currency.isTransitive == true) {
                throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_TEST_ASSET_METADATA_CONFLICT:$asset")
            }
        }

        val owner = walletOwnerManager.findWalletOwner(ownerUuid)
            ?: walletOwnerManager.createWalletOwner(
                uuid = ownerUuid,
                title = "Custody EVM Test Treasury",
                userLevel = "1",
                externalIdentifier = "custody-evm-test-treasury",
            )
        for (asset in ASSETS) {
            val currency = currency(asset)
            val existingWallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            if (existingWallet == null) {
                walletManager.createWallet(
                    owner = owner,
                    balance = Amount(currency, BigDecimal.ZERO),
                    currency = currency,
                    type = WalletType.MAIN,
                )
                createdWallets.add(asset)
            }
        }

        return CustodyLedgerBootstrapResponse(
            treasuryOwnerUuid = ownerUuid,
            assets = ASSETS,
            createdCurrencies = createdCurrencies,
            createdWallets = createdWallets,
        )
    }

    private suspend fun credit(
        operationId: String,
        owner: WalletOwner,
        treasuryOwner: WalletOwner,
        currency: CurrencyCommand,
        amount: BigDecimal,
        referenceOperationId: String?,
        payloadHash: String,
    ): String {
        if (owner.uuid == treasuryOwner.uuid) throw badRequest("CUSTODY_TREASURY_CANNOT_CREDIT_ITSELF")
        val source = walletManager.findWalletByOwnerAndCurrencyAndType(treasuryOwner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_WALLET_NOT_PROVISIONED")
        requireFunds(source, operationId, owner.uuid, currency.symbol, amount, CustodyLedgerActionKind.CREDIT, referenceOperationId, payloadHash)
        val destination = mainWallet(owner, currency)
        return nativeTransfer(
            operationId, source, destination, amount, TransferCategory.DEPOSIT_MANUALLY,
            "Custody EVM test deposit credit operation=$operationId",
        )
    }

    private suspend fun reserve(
        operationId: String,
        owner: WalletOwner,
        currency: CurrencyCommand,
        amount: BigDecimal,
        referenceOperationId: String?,
        payloadHash: String,
    ): String {
        val source = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_MAIN_WALLET_NOT_FOUND")
        requireFunds(source, operationId, owner.uuid, currency.symbol, amount, CustodyLedgerActionKind.RESERVE, referenceOperationId, payloadHash)
        val destination = cashoutWallet(owner, currency)
        return nativeTransfer(
            operationId, source, destination, amount, TransferCategory.WITHDRAW_REQUEST,
            "Custody EVM test withdrawal reserve operation=$operationId",
        )
    }

    private suspend fun releaseOrSettle(
        operationId: String,
        owner: WalletOwner,
        treasuryOwner: WalletOwner,
        currency: CurrencyCommand,
        amount: BigDecimal,
        reserveOperationId: String,
        payloadHash: String,
        kind: CustodyLedgerActionKind,
    ): String {
        val reserve = actionRepository.findById(reserveOperationId).awaitSingleOrNull()
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_RESERVE_NOT_FOUND")
        if (reserve.kind != CustodyLedgerActionKind.RESERVE.name || reserve.status != APPLIED_STATUS || reserve.transactionId == null) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_REFERENCE_NOT_RESERVE")
        }
        if (reserve.ownerUuid != owner.uuid || reserve.asset != currency.symbol || reserve.amount.compareTo(amount) != 0) {
            throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_REFERENCE_MISMATCH")
        }
        val closed = actionRepository.closeReserve(reserveOperationId, operationId, kind.name).awaitSingle()
        if (closed != 1) throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_RESERVE_ALREADY_CLOSED")

        val source = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.CASHOUT, currency)
            ?: throw ResponseStatusException(HttpStatus.CONFLICT, "CUSTODY_LEDGER_RESERVED_WALLET_NOT_FOUND")
        requireFunds(source, operationId, owner.uuid, currency.symbol, amount, kind, reserveOperationId, payloadHash)
        val destination = when (kind) {
            CustodyLedgerActionKind.RELEASE -> mainWallet(owner, currency)
            CustodyLedgerActionKind.SETTLE -> {
                if (owner.uuid == treasuryOwner.uuid) throw badRequest("CUSTODY_TREASURY_CANNOT_SETTLE_ITSELF")
                mainWallet(treasuryOwner, currency)
            }
            else -> throw IllegalArgumentException("Only RELEASE and SETTLE close a reserve")
        }
        val category = when (kind) {
            CustodyLedgerActionKind.RELEASE -> TransferCategory.WITHDRAW_CANCEL
            CustodyLedgerActionKind.SETTLE -> TransferCategory.WITHDRAW_ACCEPT
            else -> error("unreachable")
        }
        return nativeTransfer(
            operationId, source, destination, amount, category,
            "Custody EVM test withdrawal ${kind.name.lowercase()} operation=$operationId reserve=$reserveOperationId",
        )
    }

    private suspend fun nativeTransfer(
        operationId: String,
        source: Wallet,
        destination: Wallet,
        amount: BigDecimal,
        category: TransferCategory,
        description: String,
    ): String {
        val result = transferManager.transfer(
            TransferCommand(
                sourceWallet = source,
                destWallet = destination,
                amount = Amount(source.currency, amount),
                destAmount = Amount(destination.currency, amount),
                description = description,
                transferRef = "wallet:custody-ledger:$operationId",
                transferCategory = category,
            ),
        )
        if (result.tx.isBlank()) throw IllegalStateException("Native wallet transfer returned an empty transaction id")
        return result.tx
    }

    private suspend fun requireFunds(
        source: Wallet,
        operationId: String,
        ownerUuid: String,
        asset: String,
        amount: BigDecimal,
        kind: CustodyLedgerActionKind,
        referenceOperationId: String?,
        payloadHash: String,
    ) {
        if (source.balance.amount < amount) {
            throw CustodyLedgerInsufficientFundsException(
                operationId = operationId,
                ownerUuid = ownerUuid,
                asset = asset,
                amount = canonicalAmount(amount),
                kind = kind,
                referenceOperationId = referenceOperationId,
                payloadHash = payloadHash,
            )
        }
    }

    private suspend fun mainWallet(owner: WalletOwner, currency: CurrencyCommand): Wallet {
        return walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            ?: walletManager.createWallet(owner, Amount(currency, BigDecimal.ZERO), currency, WalletType.MAIN)
    }

    private suspend fun cashoutWallet(owner: WalletOwner, currency: CurrencyCommand): Wallet {
        return walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.CASHOUT, currency)
            ?: walletManager.createWallet(owner, Amount(currency, BigDecimal.ZERO), currency, WalletType.CASHOUT)
    }

    private suspend fun currency(asset: String): CurrencyCommand {
        return currencyServiceManager.fetchCurrency(FetchCurrency(symbol = asset))
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TEST_ASSET_NOT_PROVISIONED")
    }

    private suspend fun treasuryOwner(): WalletOwner {
        val ownerUuid = treasuryOwnerUuid()
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_OWNER_NOT_CONFIGURED")
        if (ownerUuid == walletOwnerManager.systemUuid) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_OWNER_INVALID")
        }
        return walletOwnerManager.findWalletOwner(ownerUuid)
            ?: throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_OWNER_NOT_PROVISIONED")
    }

    private fun treasuryOwnerUuid(): String? {
        if (treasuryOwnerUuidValue.isBlank()) return null
        return try {
            UUID.fromString(treasuryOwnerUuidValue.trim()).toString()
        } catch (_: IllegalArgumentException) {
            throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "CUSTODY_TREASURY_OWNER_UUID_INVALID")
        }
    }

    private fun requireEnabled() {
        if (!enabled) throw ResponseStatusException(HttpStatus.NOT_FOUND, "CUSTODY_LEDGER_DISABLED")
    }

    private fun normalizeUuid(value: String, field: String): String {
        return try {
            UUID.fromString(value.trim()).toString()
        } catch (_: IllegalArgumentException) {
            throw badRequest("CUSTODY_LEDGER_INVALID_${field.uppercase()}")
        }
    }

    private fun normalizeAsset(value: String): String {
        val asset = value.trim()
        if (asset !in ASSETS) throw badRequest("CUSTODY_LEDGER_ASSET_NOT_ALLOWED")
        return asset
    }

    private fun parseAmount(value: String): BigDecimal {
        if (!DECIMAL_PATTERN.matches(value)) throw badRequest("CUSTODY_LEDGER_INVALID_AMOUNT")
        val amount = try {
            BigDecimal(value)
        } catch (_: NumberFormatException) {
            throw badRequest("CUSTODY_LEDGER_INVALID_AMOUNT")
        }
        val normalized = amount.stripTrailingZeros()
        val integerDigits = normalized.precision() - normalized.scale()
        if (amount <= BigDecimal.ZERO || normalized.scale() > MAX_ASSET_PRECISION ||
            normalized.precision() > MAX_NUMERIC_PRECISION || integerDigits > MAX_NUMERIC_INTEGER_DIGITS
        ) {
            throw badRequest("CUSTODY_LEDGER_INVALID_AMOUNT")
        }
        return amount
    }

    private fun validateKindReference(kind: CustodyLedgerActionKind, referenceOperationId: String?) {
        val referenceRequired = kind != CustodyLedgerActionKind.RESERVE
        if (referenceRequired != (referenceOperationId != null)) {
            throw badRequest("CUSTODY_LEDGER_REFERENCE_OPERATION_ID_REQUIRED")
        }
    }

    private fun canonicalAmount(value: BigDecimal): String = value.stripTrailingZeros().toPlainString()

    private fun badRequest(code: String) = ResponseStatusException(HttpStatus.BAD_REQUEST, code)

    private fun CustodyLedgerActionModel.toResponse(): CustodyLedgerActionResponse {
        val transactionId = transactionId ?: throw IllegalStateException("Applied custody action has no native transaction")
        return CustodyLedgerActionResponse(
            operationId = operationId,
            ownerUuid = ownerUuid,
            asset = asset,
            amount = canonicalAmount(amount),
            kind = CustodyLedgerActionKind.valueOf(kind),
            referenceOperationId = referenceOperationId,
            payloadHash = payloadHash,
            status = status,
            transactionId = transactionId.toString(),
        )
    }

    companion object {
        const val APPLIED_STATUS = "APPLIED"
        const val LOCAL_ASSET = "LOCAL_EVM_ETH"
        const val SEPOLIA_ASSET = "SEPOLIA_ETH"
        const val MAX_ASSET_PRECISION = 18
        const val MAX_NUMERIC_PRECISION = 78
        const val MAX_NUMERIC_INTEGER_DIGITS = 60
        val ASSETS = listOf(LOCAL_ASSET, SEPOLIA_ASSET)
        val ETH_PRECISION = BigDecimal("18")
        val DECIMAL_PATTERN = Regex("^[0-9]+(?:\\.[0-9]+)?$")
        val HASH_PATTERN = Regex("^[0-9a-f]{64}$")
    }
}
