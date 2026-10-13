package co.nilin.opex.matching.engine.app.listener

import co.nilin.opex.matching.engine.app.bl.ExchangeEventHandler
import co.nilin.opex.matching.engine.core.inout.*
import co.nilin.opex.matching.engine.ports.kafka.listener.spi.OrderRequestEventListener

class OrderListener(private val handler: ExchangeEventHandler) : OrderRequestEventListener {
    override fun id() = "OrderListener"
    override suspend fun onOrder(order: OrderRequestEvent, partition: Int, offset: Long, timestamp: Long) {
        if (order is OrderSubmitRequestEvent && order.requiresReservation && !order.reservationConfirmed) return
        handler.process(order.pair.toString()) { book ->
            when (order) {
                is OrderSubmitRequestEvent -> book.handleNewOrderCommand(OrderCreateCommand(
                    order.ouid, order.uuid, order.pair, order.price, order.quantity, order.direction,
                    order.matchConstraint, order.orderType, order.postOnly, order.clientOrderId,
                    order.requiresReservation))
                is OrderCancelRequestEvent -> book.handleCancelCommand(OrderCancelCommand(
                    order.ouid, order.uuid, order.orderId, order.pair))
            }
        }
    }
}
