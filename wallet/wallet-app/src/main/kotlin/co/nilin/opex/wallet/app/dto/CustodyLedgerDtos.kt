package co.nilin.opex.wallet.app.dto

import co.nilin.opex.wallet.app.service.CustodyLedgerActionKind

data class CustodyLedgerActionRequest(
    val operationId: String,
    val ownerUuid: String,
    val asset: String,
    val amount: String,
    val kind: CustodyLedgerActionKind,
    val referenceOperationId: String? = null,
    val payloadHash: String,
)

data class CustodyLedgerActionResponse(
    val operationId: String,
    val ownerUuid: String,
    val asset: String,
    val amount: String,
    val kind: CustodyLedgerActionKind,
    val referenceOperationId: String?,
    val payloadHash: String,
    val status: String,
    val transactionId: String?,
    val errorCode: String? = null,
)

data class CustodyLedgerBalanceResponse(
    val ownerUuid: String,
    val asset: String,
    val available: String,
    val reserved: String,
    val total: String,
)

data class CustodyLedgerAssetCapability(
    val asset: String,
    val currencyConfigured: Boolean,
    val treasuryWalletConfigured: Boolean,
    val fundedAmount: String,
    val creditAvailable: Boolean,
    val reserveAvailable: Boolean,
    val reason: String?,
)

data class CustodyLedgerCapabilitiesResponse(
    val enabled: Boolean,
    val bootstrapEnabled: Boolean,
    val treasuryOwnerConfigured: Boolean,
    val treasuryOwnerFound: Boolean,
    val assets: List<CustodyLedgerAssetCapability>,
)

data class CustodyLedgerBootstrapResponse(
    val treasuryOwnerUuid: String,
    val assets: List<String>,
    val createdCurrencies: List<String>,
    val createdWallets: List<String>,
    val balancesFunded: Boolean = false,
)
