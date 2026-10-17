package co.nilin.opex.wallet.app.service

import org.springframework.data.r2dbc.repository.Modifying
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Mono

@Repository
interface CustodyLedgerActionRepository : ReactiveCrudRepository<CustodyLedgerActionModel, String> {
    @Query(
        """
        INSERT INTO custody_ledger_actions (
            operation_id, owner_uuid, asset, amount, kind, reference_operation_id,
            payload_hash, status, transaction_id
        ) VALUES (
            :operationId, :ownerUuid, :asset, :amount, :kind, :referenceOperationId,
            :payloadHash, 'PROCESSING', NULL
        )
        ON CONFLICT (operation_id) DO NOTHING
        RETURNING operation_id
        """,
    )
    fun insertIfAbsent(
        operationId: String,
        ownerUuid: String,
        asset: String,
        amount: java.math.BigDecimal,
        kind: String,
        referenceOperationId: String?,
        payloadHash: String,
    ): Mono<String>

    @Modifying
    @Query(
        """
        UPDATE custody_ledger_actions
        SET status = 'APPLIED', transaction_id = :transactionId
        WHERE operation_id = :operationId AND status = 'PROCESSING'
        """,
    )
    fun markApplied(operationId: String, transactionId: Long): Mono<Int>

    @Modifying
    @Query(
        """
        UPDATE custody_ledger_actions
        SET terminal_operation_id = :terminalOperationId, terminal_kind = :terminalKind
        WHERE operation_id = :reserveOperationId
          AND kind = 'RESERVE'
          AND status = 'APPLIED'
          AND terminal_operation_id IS NULL
        """,
    )
    fun closeReserve(
        reserveOperationId: String,
        terminalOperationId: String,
        terminalKind: String,
    ): Mono<Int>
}
