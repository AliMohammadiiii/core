package co.nilin.opex.wallet.ports.postgres.model

import co.nilin.opex.wallet.core.model.SandboxOperationStatus
import co.nilin.opex.wallet.core.model.SandboxOperationType
import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal
import java.time.LocalDateTime

@Table("sandbox_wallet_operations")
data class SandboxWalletOperationModel(
    @Id val id: String,
    @Column("owner_uuid") val ownerUuid: String,
    @Column("idempotency_key") val idempotencyKey: String,
    val type: SandboxOperationType,
    val currency: String,
    val amount: BigDecimal,
    val status: SandboxOperationStatus,
    @Column("ledger_transaction_id") val ledgerTransactionId: String,
    @Column("withdrawal_uuid") val withdrawalUuid: String?,
    @Column("created_at") val createdAt: LocalDateTime,
)
