package co.nilin.opex.accountant.ports.kafka.listener.inout

import co.nilin.opex.matching.engine.core.model.MatchConstraint
import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.engine.core.model.OrderType
import co.nilin.opex.matching.engine.core.model.Pair
import java.math.BigDecimal

class OrderSubmitRequestEvent(
    ouid: String,
    uuid: String,
    pair: Pair,
    val price: Long,
    val quantity: Long,
    val direction: OrderDirection,
    val matchConstraint: MatchConstraint,
    val orderType: OrderType,
    val userLevel: String,
    val orderId: Long? = null,
    val clientOrderId: String? = null,
    val postOnly: Boolean = false,
    val requiresReservation: Boolean = false,
    val reservationConfirmed: Boolean = false,
    var conditionalAction: String? = null,
    val stopPrice: BigDecimal? = null,
) : OrderRequestEvent(ouid, uuid, pair)
