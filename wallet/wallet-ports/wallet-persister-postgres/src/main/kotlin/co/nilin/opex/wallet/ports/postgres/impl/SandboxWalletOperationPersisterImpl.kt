package co.nilin.opex.wallet.ports.postgres.impl

import co.nilin.opex.wallet.core.model.SandboxWalletOperation
import co.nilin.opex.wallet.core.spi.SandboxWalletOperationPersister
import co.nilin.opex.wallet.ports.postgres.dao.SandboxWalletOperationRepository
import co.nilin.opex.wallet.ports.postgres.model.SandboxWalletOperationModel
import kotlinx.coroutines.reactive.awaitFirst
import kotlinx.coroutines.reactive.awaitFirstOrNull
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service

@Service
class SandboxWalletOperationPersisterImpl(
    private val repository: SandboxWalletOperationRepository,
    private val databaseClient: DatabaseClient,
) : SandboxWalletOperationPersister {
    override suspend fun acquireIdempotencyLock(ownerUuid: String, idempotencyKey: String) {
        databaseClient.sql(
            "SELECT pg_advisory_xact_lock(hashtext(:ownerUuid), hashtext(:idempotencyKey))",
        )
            .bind("ownerUuid", ownerUuid)
            .bind("idempotencyKey", idempotencyKey)
            .map { _, _ -> Unit }
            .one()
            .awaitFirstOrNull()
    }

    override suspend fun persist(operation: SandboxWalletOperation): SandboxWalletOperation {
        repository.insertOperation(
            id = operation.id,
            ownerUuid = operation.ownerUuid,
            idempotencyKey = operation.idempotencyKey,
            type = operation.type.name,
            currency = operation.currency,
            amount = operation.amount,
            status = operation.status.name,
            ledgerTransactionId = operation.ledgerTransactionId,
            withdrawalUuid = operation.withdrawalUuid,
            createdAt = operation.createdAt,
        ).awaitFirstOrNull()
        val persisted = repository.findByIdAndOwnerUuid(operation.id, operation.ownerUuid).awaitFirstOrNull()
            ?: error("Persisted sandbox wallet operation ${operation.id} could not be reloaded.")
        return persisted.toDomain()
    }

    override suspend fun findByOwnerAndIdempotencyKey(
        ownerUuid: String,
        idempotencyKey: String,
    ): SandboxWalletOperation? = repository.findByOwnerUuidAndIdempotencyKey(ownerUuid, idempotencyKey)
        .awaitFirstOrNull()?.toDomain()

    override suspend fun findByIdAndOwner(id: String, ownerUuid: String): SandboxWalletOperation? =
        repository.findByIdAndOwnerUuid(id, ownerUuid).awaitFirstOrNull()?.toDomain()

    override suspend fun findRecentByOwner(
        ownerUuid: String,
        limit: Int,
        offset: Int,
    ): List<SandboxWalletOperation> = repository.findRecentByOwnerUuid(ownerUuid, limit, offset)
        .map { it.toDomain() }
        .collectList()
        .awaitFirst()

    private fun SandboxWalletOperationModel.toDomain() = SandboxWalletOperation(
        id = id,
        ownerUuid = ownerUuid,
        idempotencyKey = idempotencyKey,
        type = type,
        currency = currency,
        amount = amount,
        status = status,
        ledgerTransactionId = ledgerTransactionId,
        withdrawalUuid = withdrawalUuid,
        createdAt = createdAt,
    )
}
