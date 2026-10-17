package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.core.model.SandboxOperationType
import org.springframework.stereotype.Component
import java.math.BigDecimal

data class SandboxProviderRequest(
    val operationId: String,
    val type: SandboxOperationType,
    val currency: String,
    val amount: BigDecimal,
)

enum class SandboxProviderDisposition {
    CONFIRMED,
    PENDING,
    REJECTED,
}

data class SandboxProviderResult(
    val provider: String,
    val environment: String,
    val disposition: SandboxProviderDisposition,
    val providerReference: String? = null,
)

/** A local test seam. It never calls a bank, blockchain, or remote payment service. */
interface SandboxTestProvider {
    suspend fun submit(request: SandboxProviderRequest): SandboxProviderResult

    suspend fun reconcile(request: SandboxProviderRequest): SandboxProviderResult
}

@Component
class LocalSandboxTestProvider : SandboxTestProvider {
    override suspend fun submit(request: SandboxProviderRequest): SandboxProviderResult =
        SandboxProviderResult("SANDBOX", "TEST", SandboxProviderDisposition.CONFIRMED)

    override suspend fun reconcile(request: SandboxProviderRequest): SandboxProviderResult = submit(request)
}
