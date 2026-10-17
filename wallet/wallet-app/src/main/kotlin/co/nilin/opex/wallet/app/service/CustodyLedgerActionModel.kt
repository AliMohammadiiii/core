package co.nilin.opex.wallet.app.service

import org.springframework.data.annotation.Id
import org.springframework.data.relational.core.mapping.Column
import org.springframework.data.relational.core.mapping.Table
import java.math.BigDecimal

@Table("custody_ledger_actions")
data class CustodyLedgerActionModel(
    @Id
    @Column("operation_id")
    val operationId: String,
    @Column("owner_uuid")
    val ownerUuid: String,
    val asset: String,
    val amount: BigDecimal,
    val kind: String,
    @Column("reference_operation_id")
    val referenceOperationId: String?,
    @Column("payload_hash")
    val payloadHash: String,
    val status: String,
    @Column("transaction_id")
    val transactionId: Long?,
    @Column("terminal_operation_id")
    val terminalOperationId: String?,
    @Column("terminal_kind")
    val terminalKind: String?,
)
