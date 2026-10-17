package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.engine.core.model.*
import co.nilin.opex.matching.gateway.app.inout.OrderIntentRequest
import co.nilin.opex.matching.gateway.app.spi.AccountantReadinessProxy
import co.nilin.opex.matching.gateway.app.spi.PairConfigLoader
import co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.gateway.ports.postgres.dto.*
import co.nilin.opex.matching.gateway.ports.postgres.service.*
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.CancellationException
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.security.MessageDigest
import java.util.UUID

@Service
class OrderIntentService(
    private val store: OrderIntentStore,
    private val abandonStore: OrderIntentAbandonStore,
    private val pairs: PairSettingService,
    private val config: PairConfigLoader,
    val mapper: ObjectMapper,
    private val accountantReadiness: AccountantReadinessProxy,
) {

    suspend fun submit(owner: String, request: OrderIntentRequest): OrderIntent {
        val hash = payloadHash(request)
        store.find(owner, request.clientOrderId, null)?.let {
            if (it.payloadHash != hash) throw OrderIntentConflict()
            return it
        }
        if (request.type == "STOP_LIMIT") {
            val ready = try {
                accountantReadiness.conditionalOrdersReady()
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                false
            }
            if (!ready) {
                throw ResponseStatusException(HttpStatus.SERVICE_UNAVAILABLE, "Conditional order trigger is not ready")
            }
        }

        val settings = pairs.load(request.symbol)
        requireRequest(settings.isAvailable, "Pair is not available")
        val type = if (request.type == "MARKET") OrderType.MARKET_ORDER else OrderType.LIMIT_ORDER
        requireRequest(settings.orderTypes.split(',').map { it.trim() }.contains(type.name), "Order type is not available")
        val notional = request.price.multiply(request.quantity)
        requireRequest(notional >= settings.minOrder && notional <= settings.maxOrder, "Order notional is outside pair limits")
        val direction = if (request.side == "BUY") OrderDirection.BID else OrderDirection.ASK
        val fractions = config.load(request.symbol, direction)
        val symbols = request.symbol.split('_')
        val ouid = UUID.randomUUID().toString()
        val event = try {
            OrderSubmitRequestEvent(owner, Pair(symbols[0], symbols[1]),
                request.price.divide(fractions.rightSideFraction).longValueExact(),
                request.quantity.divide(fractions.leftSideFraction).longValueExact(), direction,
                if (type == OrderType.LIMIT_ORDER) MatchConstraint.GTC else MatchConstraint.IOC, type, "*",
                clientOrderId = request.clientOrderId, postOnly = request.postOnly,
                requiresReservation = true,
                conditionalAction = if (request.type == "STOP_LIMIT") "HOLD" else null,
                ouid = ouid,
                stopPrice = request.stopPrice)
        } catch (e: ArithmeticException) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Price or quantity violates pair increments", e)
        }
        return store.accept(OrderIntent(owner, request.clientOrderId, ouid, hash, request.symbol,
            request.type, request.side, request.quantity, request.price, request.postOnly,
            fractions.leftSideFraction, mapper.writeValueAsString(event), stopPrice = request.stopPrice))
    }

    suspend fun abandon(owner: String, request: OrderIntentRequest): OrderIntent {
        val hash = payloadHash(request)
        val tombstone = OrderIntent(
            owner = owner,
            clientOrderId = request.clientOrderId,
            ouid = UUID.randomUUID().toString(),
            payloadHash = hash,
            symbol = request.symbol,
            type = request.type,
            side = request.side,
            quantity = request.quantity,
            price = request.price,
            postOnly = request.postOnly,
            // Tombstones are terminal and never pass through order conversion.
            leftFraction = BigDecimal.ONE,
            payload = mapper.writeValueAsString(request),
            status = "CANCELED",
            cancelRequested = true,
            stopPrice = request.stopPrice,
        )
        val existing = abandonStore.insertOrRead(tombstone)
        if (existing.payloadHash != hash) throw OrderIntentConflict()
        if (existing.status in setOf("FILLED", "CANCELED", "REJECTED")) return existing
        return cancel(owner, request.clientOrderId, null)
    }

    suspend fun get(owner: String, clientOrderId: String?, ouid: String?): OrderIntent {
        requireRequest((clientOrderId != null) xor (ouid != null), "Supply exactly one clientOrderId or ouid")
        return store.find(owner, clientOrderId, ouid) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")
    }

    suspend fun cancel(owner: String, clientOrderId: String?, ouid: String?): OrderIntent {
        val intent = get(owner, clientOrderId, ouid)
        if (intent.status in setOf("FILLED", "CANCELED", "REJECTED")) return intent
        val event = if (intent.type == "STOP_LIMIT") {
            mapper.readValue(intent.payload, OrderSubmitRequestEvent::class.java).apply { conditionalAction = "CANCEL" }
        } else {
            val sides = intent.symbol.split('_')
            co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderCancelRequestEvent(
                intent.ouid, owner, Pair(sides[0], sides[1]), intent.orderId ?: 0)
        }
        return store.cancel(owner, clientOrderId, ouid, mapper.writeValueAsString(event))
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")
    }

    private fun payloadHash(request: OrderIntentRequest): String {
        requireRequest(request.clientOrderId.matches(Regex("[A-Za-z0-9._:-]{1,80}")), "Invalid clientOrderId")
        requireRequest(request.symbol.matches(Regex("[A-Z0-9]+_[A-Z0-9]+")), "Invalid symbol")
        requireRequest(request.type in listOf("LIMIT", "MARKET", "STOP_LIMIT"), "Unsupported order type")
        requireRequest(request.side in listOf("BUY", "SELL"), "Invalid side")
        requireRequest(request.quantity > BigDecimal.ZERO && request.price > BigDecimal.ZERO,
            "Positive quantity and reservation price are required")
        requireRequest(request.type != "STOP_LIMIT" || request.stopPrice?.let { it > BigDecimal.ZERO } == true,
            "STOP_LIMIT requires a positive stopPrice")
        requireRequest(request.type == "STOP_LIMIT" || request.stopPrice == null,
            "stopPrice is only valid for STOP_LIMIT")
        requireRequest(!request.postOnly || request.type == "LIMIT", "postOnly requires LIMIT")
        val canonicalFields = listOf(request.symbol, request.type, request.side,
            request.quantity.stripTrailingZeros().toPlainString(), request.price.stripTrailingZeros().toPlainString(),
            request.postOnly.toString())
        val canonical = (if (request.type == "STOP_LIMIT") {
            canonicalFields + request.stopPrice!!.stripTrailingZeros().toPlainString()
        } else canonicalFields).joinToString("|")
        return MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray())
            .joinToString("") { "%02x".format(it) }
    }

    private fun requireRequest(valid: Boolean, message: String) {
        if (!valid) throw ResponseStatusException(HttpStatus.BAD_REQUEST, message)
    }
}
