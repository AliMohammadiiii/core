package co.nilin.opex.matching.engine.core

import co.nilin.opex.matching.engine.core.engine.SimpleOrderBook
import co.nilin.opex.matching.engine.core.eventh.EventDispatcher
import co.nilin.opex.matching.engine.core.eventh.events.*
import co.nilin.opex.matching.engine.core.inout.*
import co.nilin.opex.matching.engine.core.model.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.util.UUID

class OrderSafetyTest {
    private fun command(pair: Pair, ouid: String, owner: String, side: OrderDirection,
                        price: Long = 100, quantity: Long = 10) = OrderCreateCommand(
        ouid, owner, pair, price, quantity, side, MatchConstraint.GTC, OrderType.LIMIT_ORDER)

    @Test
    fun sameOwnerCrossCancelsTakerWithoutTrade() {
        val pair = Pair("STP", UUID.randomUUID().toString())
        val trades = mutableListOf<TradeEvent>()
        EventDispatcher.register<TradeEvent> { if (it.pair == pair) trades.add(it) }
        val book = SimpleOrderBook(pair, false)
        book.handleNewOrderCommand(command(pair, "maker", "owner", OrderDirection.ASK))
        book.handleNewOrderCommand(command(pair, "taker", "owner", OrderDirection.BID))
        assertEquals(0, trades.size)
        assertEquals(10L, book.bestAskOrder!!.remainedQuantity())
        assertNull(book.bestBidOrder)
    }

    @Test
    fun duplicateAdmissionDoesNotQueueOrFillTwice() {
        val pair = Pair("DEDUPE", UUID.randomUUID().toString())
        val book = SimpleOrderBook(pair, false)
        val first = command(pair, "stable-ouid", "owner", OrderDirection.BID)
        book.handleNewOrderCommand(first)
        book.handleNewOrderCommand(first)
        assertEquals(1, book.orders.size)
        assertEquals(10L, book.bestBidOrder!!.bucket!!.totalQuantity)
        assertEquals(1L, book.orderCounter.get())
    }

    @Test
    fun foreignCancelCannotRemoveOwnedOrder() {
        val pair = Pair("AUTH", UUID.randomUUID().toString())
        val book = SimpleOrderBook(pair, false)
        val order = book.handleNewOrderCommand(command(pair, "owned", "owner", OrderDirection.BID))!!
        book.handleCancelCommand(OrderCancelCommand("owned", "attacker", order.id()!!, pair))
        assertEquals(1, book.orders.size)
        assertEquals("owner", book.bestBidOrder!!.uuid)
    }

    @Test
    fun postOnlyEqualAndCrossedRejectBeforeAnyFill() {
        for (side in OrderDirection.values()) for (price in listOf(100L, if (side == OrderDirection.BID) 101L else 99L)) {
            val pair = Pair("POST", UUID.randomUUID().toString())
            val trades = mutableListOf<TradeEvent>()
            val rejects = mutableListOf<RejectOrderEvent>()
            EventDispatcher.register<TradeEvent> { if (it.pair == pair) trades.add(it) }
            EventDispatcher.register<RejectOrderEvent> { if (it.pair == pair) rejects.add(it) }
            val book = SimpleOrderBook(pair, false)
            val makerSide = if (side == OrderDirection.BID) OrderDirection.ASK else OrderDirection.BID
            book.handleNewOrderCommand(command(pair, "maker", "another-owner", makerSide))
            book.handleNewOrderCommand(command(pair, "post", "owner", side, price).copy(postOnly = true))
            assertEquals(0, trades.size)
            assertEquals(1, rejects.size)
            assertEquals(1, book.orders.size)
        }
    }

    @Test
    fun partialOtherOwnerFillThenOwnMakerCancelsOnlyUnusedTaker() {
        val pair = Pair("PARTIAL", UUID.randomUUID().toString())
        val trades = mutableListOf<TradeEvent>()
        val cancels = mutableListOf<CancelOrderEvent>()
        EventDispatcher.register<TradeEvent> { if (it.pair == pair) trades.add(it) }
        EventDispatcher.register<CancelOrderEvent> { if (it.pair == pair) cancels.add(it) }
        val book = SimpleOrderBook(pair, false)
        book.handleNewOrderCommand(command(pair, "external", "other", OrderDirection.ASK, 99, 4))
        book.handleNewOrderCommand(command(pair, "own", "owner", OrderDirection.ASK, 100, 10))
        book.handleNewOrderCommand(command(pair, "taker", "owner", OrderDirection.BID, 100, 10))
        assertEquals(1, trades.size)
        assertEquals(4L, trades.single().matchedQuantity)
        assertEquals(6L, cancels.single().remainedQuantity)
        assertEquals(10L, book.bestAskOrder!!.remainedQuantity())
        assertNull(book.bestBidOrder)
    }

    @Test
    fun terminalAdmissionReplayAfterRestartCannotFillAgain() {
        val pair = Pair("RESTART", UUID.randomUUID().toString())
        val trades = mutableListOf<TradeEvent>()
        EventDispatcher.register<TradeEvent> { if (it.pair == pair) trades.add(it) }
        val book = SimpleOrderBook(pair, false)
        val taker = command(pair, "taker", "taker-owner", OrderDirection.BID)
        book.handleNewOrderCommand(command(pair, "maker", "maker-owner", OrderDirection.ASK))
        book.handleNewOrderCommand(taker)
        val restored = SimpleOrderBook(pair, false)
        restored.rebuild(book.persistent())
        restored.handleNewOrderCommand(command(pair, "new-maker", "new-owner", OrderDirection.ASK))
        restored.handleNewOrderCommand(taker)
        assertEquals(1, trades.size)
        assertEquals(10L, restored.bestAskOrder!!.remainedQuantity())
    }

    @Test
    fun cancelBeforeReserveAdmissionSurvivesRestart() {
        val pair = Pair("PENDING", UUID.randomUUID().toString())
        val cancels = mutableListOf<CancelOrderEvent>()
        EventDispatcher.register<CancelOrderEvent> { if (it.pair == pair) cancels.add(it) }
        val book = SimpleOrderBook(pair, false)
        book.handleCancelCommand(OrderCancelCommand("pending", "owner", 0, pair))
        val restored = SimpleOrderBook(pair, false)
        restored.rebuild(book.persistent())
        restored.handleNewOrderCommand(command(pair, "pending", "owner", OrderDirection.BID))
        assertEquals(10L, cancels.single().remainedQuantity)
        assertEquals(0, restored.orders.size)
    }

    @Test
    fun reservedMarketBidCannotSpendAboveItsReservationPrice() {
        val pair = Pair("BOUND", UUID.randomUUID().toString())
        val trades = mutableListOf<TradeEvent>()
        EventDispatcher.register<TradeEvent> { if (it.pair == pair) trades.add(it) }
        val book = SimpleOrderBook(pair, false)
        book.handleNewOrderCommand(command(pair, "expensive", "other-owner", OrderDirection.ASK, 150, 2))
        book.handleNewOrderCommand(command(pair, "market", "owner", OrderDirection.BID, 100, 2).copy(
            orderType = OrderType.MARKET_ORDER, matchConstraint = MatchConstraint.IOC, reservePriceBound = true))
        assertEquals(0, trades.size)
        assertEquals(2L, book.bestAskOrder!!.remainedQuantity())
    }

    @Test
    fun cancelAfterAdmissionUsesActualIdWhenReceiptWasPending() {
        val pair = Pair("CANCEL_ID", UUID.randomUUID().toString())
        val cancels = mutableListOf<CancelOrderEvent>()
        EventDispatcher.register<CancelOrderEvent> { if (it.pair == pair) cancels.add(it) }
        val book = SimpleOrderBook(pair, false)
        book.handleNewOrderCommand(command(pair, "pending-id", "owner", OrderDirection.BID))
        book.handleCancelCommand(OrderCancelCommand("pending-id", "owner", 0, pair))
        assertEquals(1L, cancels.single().orderId)
        assertEquals(10L, cancels.single().remainedQuantity)
        assertTrue(book.orders.isEmpty())
    }
}
