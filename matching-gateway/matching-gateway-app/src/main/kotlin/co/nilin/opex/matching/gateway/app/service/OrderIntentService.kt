package co.nilin.opex.matching.gateway.app.service

import co.nilin.opex.matching.engine.core.model.*
import co.nilin.opex.matching.gateway.app.inout.OrderIntentRequest
import co.nilin.opex.matching.gateway.app.spi.PairConfigLoader
import co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.gateway.ports.postgres.dto.*
import co.nilin.opex.matching.gateway.ports.postgres.service.*
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.security.MessageDigest
import java.util.UUID

@Service
class OrderIntentService(private val store: OrderIntentStore, private val pairs: PairSettingService,
    private val config: PairConfigLoader, private val mapper: ObjectMapper) {

    suspend fun submit(owner: String, request: OrderIntentRequest): OrderIntent {
        requireRequest(request.clientOrderId.matches(Regex("[A-Za-z0-9._:-]{1,80}")), "Invalid clientOrderId")
        requireRequest(request.symbol.matches(Regex("[A-Z0-9]+_[A-Z0-9]+")), "Invalid symbol")
        requireRequest(request.type in listOf("LIMIT", "MARKET"), "Only LIMIT and MARKET are supported")
        requireRequest(request.side in listOf("BUY", "SELL"), "Invalid side")
        requireRequest(request.quantity > BigDecimal.ZERO && request.price > BigDecimal.ZERO, "Positive quantity and reservation price are required")
        requireRequest(!request.postOnly || request.type == "LIMIT", "postOnly requires LIMIT")
        val canonical = listOf(request.symbol, request.type, request.side,
            request.quantity.stripTrailingZeros().toPlainString(), request.price.stripTrailingZeros().toPlainString(),
            request.postOnly.toString()).joinToString("|")
        val hash = MessageDigest.getInstance("SHA-256").digest(canonical.toByteArray()).joinToString("") { "%02x".format(it) }
        store.find(owner, request.clientOrderId, null)?.let {
            if (it.payloadHash != hash) throw OrderIntentConflict()
            return it
        }
        val settings = pairs.load(request.symbol)
        requireRequest(settings.isAvailable, "Pair is not available")
        val type = if (request.type == "LIMIT") OrderType.LIMIT_ORDER else OrderType.MARKET_ORDER
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
                requiresReservation = true, ouid = ouid)
        } catch (e: ArithmeticException) {
            throw ResponseStatusException(HttpStatus.BAD_REQUEST, "Price or quantity violates pair increments", e)
        }
        return store.accept(OrderIntent(owner, request.clientOrderId, ouid, hash, request.symbol,
            request.type, request.side, request.quantity, request.price, request.postOnly,
            fractions.leftSideFraction, mapper.writeValueAsString(event)))
    }

    suspend fun get(owner: String, clientOrderId: String?, ouid: String?): OrderIntent {
        requireRequest((clientOrderId != null) xor (ouid != null), "Supply exactly one clientOrderId or ouid")
        return store.find(owner, clientOrderId, ouid) ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")
    }

    suspend fun cancel(owner: String, clientOrderId: String?, ouid: String?): OrderIntent {
        val intent = get(owner, clientOrderId, ouid)
        val sides = intent.symbol.split('_')
        val event = co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderCancelRequestEvent(
            intent.ouid, owner, Pair(sides[0], sides[1]), intent.orderId ?: 0)
        return store.cancel(owner, clientOrderId, ouid, mapper.writeValueAsString(event))
            ?: throw ResponseStatusException(HttpStatus.NOT_FOUND, "Order not found")
    }

    private fun requireRequest(valid: Boolean, message: String) {
        if (!valid) throw ResponseStatusException(HttpStatus.BAD_REQUEST, message)
    }
}
