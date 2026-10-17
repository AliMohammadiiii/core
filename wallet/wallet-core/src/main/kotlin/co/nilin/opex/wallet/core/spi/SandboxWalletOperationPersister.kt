package co.nilin.opex.wallet.core.spi

import co.nilin.opex.wallet.core.model.SandboxWalletOperation

interface SandboxWalletOperationPersister {
    suspend fun acquireIdempotencyLock(ownerUuid: String, idempotencyKey: String)

    suspend fun persist(operation: SandboxWalletOperation): SandboxWalletOperation

    suspend fun findByOwnerAndIdempotencyKey(ownerUuid: String, idempotencyKey: String): SandboxWalletOperation?

    suspend fun findByIdAndOwner(id: String, ownerUuid: String): SandboxWalletOperation?

    suspend fun findRecentByOwner(ownerUuid: String, limit: Int, offset: Int): List<SandboxWalletOperation>
}
