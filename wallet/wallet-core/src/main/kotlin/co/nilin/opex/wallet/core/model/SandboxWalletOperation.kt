package co.nilin.opex.wallet.core.model

import java.math.BigDecimal
import java.time.LocalDateTime

enum class SandboxOperationType {
    DEPOSIT,
    WITHDRAWAL
}

enum class SandboxOperationStatus {
    COMPLETED
}

data class SandboxWalletOperation(
    val id: String,
    val ownerUuid: String,
    val idempotencyKey: String,
    val type: SandboxOperationType,
    val currency: String,
    val amount: BigDecimal,
    val status: SandboxOperationStatus,
    val ledgerTransactionId: String,
    val withdrawalUuid: String?,
    val createdAt: LocalDateTime,
)
