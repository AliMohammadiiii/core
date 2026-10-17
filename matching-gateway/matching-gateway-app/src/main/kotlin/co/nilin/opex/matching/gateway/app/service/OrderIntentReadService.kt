package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.gateway.app.inout.OrderIntentCapabilities
import co.nilin.opex.matching.gateway.app.inout.OrderIntentMetadata
import co.nilin.opex.matching.gateway.app.spi.AccountantReadinessProxy
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import kotlinx.coroutines.CancellationException
import org.springframework.stereotype.Service

@Service
class OrderIntentReadService(
    private val store: OrderIntentStore,
    private val accountantReadiness: AccountantReadinessProxy,
) {
    suspend fun capabilities(): OrderIntentCapabilities {
        val supportedTypes = mutableListOf("LIMIT", "MARKET")
        val conditionalReady = try {
            accountantReadiness.conditionalOrdersReady()
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            false
        }

        if (conditionalReady) supportedTypes += "STOP_LIMIT"
        return OrderIntentCapabilities(supportedTypes)
    }

    suspend fun metadata(owner: String): List<OrderIntentMetadata> = store.metadata(owner).map { intent ->
        OrderIntentMetadata(
            ouid = intent.ouid,
            clientOrderId = intent.clientOrderId,
            symbol = intent.symbol,
            type = intent.type,
            side = intent.side,
            stopPrice = intent.stopPrice?.toPlainString(),
            price = intent.price.toPlainString(),
            quantity = intent.quantity.toPlainString(),
            filledQuantity = if (intent.status in uncertainExecutionStatuses) null
                else intent.filledQuantity.toPlainString(),
            remainingQuantity = when {
                intent.status in uncertainExecutionStatuses -> null
                intent.status == "WAITING_TRIGGER" -> intent.quantity.toPlainString()
                else -> intent.quantity.subtract(intent.filledQuantity).toPlainString()
            },
            createdAt = intent.createdAt.toInstant().toString(),
            status = if (intent.cancelRequested && intent.status !in terminalStatuses) {
                "CANCEL_PENDING"
            } else {
                intent.status
            },
        )
    }

    private companion object {
        val uncertainExecutionStatuses = setOf("ACCEPTED_PENDING", "TRIGGERED")
        val terminalStatuses = setOf("FILLED", "CANCELED", "REJECTED")
    }
}
