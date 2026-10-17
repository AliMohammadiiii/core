package co.nilin.opex.wallet.app.dto

import java.math.BigDecimal

data class SandboxOperationRequest(
    val idempotencyKey: String,
    val currency: String,
    val amount: BigDecimal,
)

data class SandboxAvailability(
    val available: Boolean,
    val reason: String?,
)

data class SandboxCapabilitiesResponse(
    val provider: String = "SANDBOX",
    val environment: String = "TEST",
    val enabled: Boolean,
    val deposit: SandboxAvailability,
    val withdrawal: SandboxAvailability,
    val fundingOwnerConfigured: Boolean,
    val fundingOwnerFound: Boolean,
    val availableCurrencies: List<String>,
    val note: String = "Requires an existing configured funding wallet; startup does not create or fund one.",
)

data class SandboxOperationResponse(
    val id: String,
    val operationId: String,
    val idempotencyKey: String,
    val type: String,
    val state: String,
    val status: String,
    val provider: String = "SANDBOX",
    val environment: String = "TEST",
    val currency: String,
    val amount: String,
    val ledgerTransactionId: String,
    val withdrawalUuid: String?,
    val createdAt: String,
)
