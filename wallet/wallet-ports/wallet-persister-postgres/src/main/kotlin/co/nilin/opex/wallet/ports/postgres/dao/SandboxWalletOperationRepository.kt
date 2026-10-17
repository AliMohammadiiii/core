package co.nilin.opex.wallet.ports.postgres.dao

import co.nilin.opex.wallet.ports.postgres.model.SandboxWalletOperationModel
import java.math.BigDecimal
import java.time.LocalDateTime
import org.springframework.data.r2dbc.repository.Query
import org.springframework.data.repository.reactive.ReactiveCrudRepository
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

@Repository
interface SandboxWalletOperationRepository : ReactiveCrudRepository<SandboxWalletOperationModel, String> {
    @Query(
        """
        INSERT INTO sandbox_wallet_operations (
            id, owner_uuid, idempotency_key, type, currency, amount, status,
            ledger_transaction_id, withdrawal_uuid, created_at
        ) VALUES (
            :id, :ownerUuid, :idempotencyKey, :type, :currency, :amount, :status,
            :ledgerTransactionId, :withdrawalUuid, :createdAt
        )
        """,
    )
    fun insertOperation(
        id: String,
        ownerUuid: String,
        idempotencyKey: String,
        type: String,
        currency: String,
        amount: BigDecimal,
        status: String,
        ledgerTransactionId: String,
        withdrawalUuid: String?,
        createdAt: LocalDateTime,
    ): Mono<Void>

    fun findByOwnerUuidAndIdempotencyKey(ownerUuid: String, idempotencyKey: String): Mono<SandboxWalletOperationModel>

    fun findByIdAndOwnerUuid(id: String, ownerUuid: String): Mono<SandboxWalletOperationModel>

    @Query("""
        SELECT * FROM sandbox_wallet_operations
        WHERE owner_uuid = :ownerUuid
        ORDER BY created_at DESC, id DESC
        LIMIT :limit OFFSET :offset
    """)
    fun findRecentByOwnerUuid(ownerUuid: String, limit: Int, offset: Int): Flux<SandboxWalletOperationModel>
}
