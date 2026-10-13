package co.nilin.opex.accountant.core.service

import co.nilin.opex.accountant.core.api.FeeCalculator
import co.nilin.opex.accountant.core.inout.OrderStatus
import co.nilin.opex.accountant.core.model.*
import co.nilin.opex.accountant.core.model.Order
import co.nilin.opex.accountant.core.spi.*
import co.nilin.opex.matching.engine.core.eventh.events.*
import co.nilin.opex.matching.engine.core.inout.*
import co.nilin.opex.matching.engine.core.model.*
import io.mockk.*
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.Test
import java.math.BigDecimal

class OrderRefundSafetyTest {
    private val pair = Pair("USDT", "IRT")
    private val saved = mutableMapOf<String, Order>()
    private val persister = object : OrderPersister {
        override suspend fun load(ouid: String) = saved[ouid]
        override suspend fun save(order: Order): Order { saved[order.ouid] = order; return order }
    }
    private val actions = mutableListOf<FinancialAction>()
    private val finances = mockk<FinancialActionPersister>()
    private val pairConfig = mockk<PairConfigLoader>()
    private val fee = mockk<FeeCalculator>()
    private val richOrders = mutableListOf<co.nilin.opex.accountant.core.inout.RichOrderEvent>()
    private val richPublisher = mockk<RichOrderPublisher>()
    private val manager: OrderManagerImpl
    init {
        coEvery { pairConfig.load(any(), any()) } returns PairConfig("USDT_IRT", "USDT", "IRT", BigDecimal.ONE, BigDecimal.ONE)
        coEvery { fee.getUserFee(any()) } returns UserFee("test", BigDecimal("0.1"), BigDecimal("0.1"))
        coEvery { finances.persist(any()) } coAnswers { firstArg<List<FinancialAction>>().also { actions.addAll(it) } }
        coEvery { richPublisher.publish(any()) } coAnswers { richOrders.add(firstArg()); Unit }
        val loader = mockk<FinancialActionLoader>()
        coEvery { loader.findLast(any(), any()) } returns null
        manager = OrderManagerImpl(pairConfig, finances, loader, persister,
            mockk(relaxed=true), richPublisher, mockk(relaxed=true), fee)
    }
    private suspend fun reserve(ouid: String = "order") = manager.handleRequestOrder(SubmitOrderEvent(
        ouid, "owner", null, pair, 100, 10, 10, OrderDirection.BID, MatchConstraint.GTC,
        OrderType.LIMIT_ORDER, clientOrderId="stable-cid", postOnly=true))

    @Test
    fun rejectedPostOnlyReleasesWholeReserveOnce() = runBlocking {
        reserve()
        val reject = RejectOrderEvent("order", "owner", pair, 100, 10, OrderDirection.BID,
            MatchConstraint.GTC, OrderType.LIMIT_ORDER, RequestedOperation.PLACE_ORDER, RejectReason.POST_ONLY_WOULD_TRADE)
        assertEquals(BigDecimal("1000"), manager.handleRejectOrder(reject).single().amount)
        assertTrue(manager.handleRejectOrder(reject).isEmpty())
        assertEquals(BigDecimal.ZERO, saved["order"]!!.remainedTransferAmount)
        assertEquals(OrderStatus.REJECTED.code, saved["order"]!!.status)
        assertEquals(1, actions.count { it.category == FinancialActionCategory.ORDER_CANCEL })
        val projection = richOrders.last() as co.nilin.opex.accountant.core.inout.RichOrderUpdate
        assertEquals(BigDecimal.ZERO, projection.executedQuantity())
    }

    @Test
    fun definitiveReservationFailureDoesNotRefundUnreservedFunds() = runBlocking {
        reserve()
        val reject = RejectOrderEvent("order", "owner", pair, 100, 10, OrderDirection.BID,
            MatchConstraint.GTC, OrderType.LIMIT_ORDER, RequestedOperation.PLACE_ORDER, RejectReason.RESERVATION_FAILED)
        assertTrue(manager.handleRejectOrder(reject).isEmpty())
        assertEquals(0, actions.count { it.category == FinancialActionCategory.ORDER_CANCEL })
        assertEquals(OrderStatus.REJECTED.code, saved["order"]!!.status)
    }

    @Test
    fun partialFillCancelReturnsUnusedReserveIncludingPriceImprovementOnce() = runBlocking {
        reserve()
        saved["order"]!!.apply {
            filledQuantity = 4; filledOrigQuantity = BigDecimal("4")
            remainedTransferAmount = BigDecimal("604") //1000 - 4*99
        }
        val cancel = CancelOrderEvent("order", "owner", 7, pair, 100, 10, 6, OrderDirection.BID,
            MatchConstraint.GTC, OrderType.LIMIT_ORDER)
        assertEquals(BigDecimal("604"), manager.handleCancelOrder(cancel).single().amount)
        assertTrue(manager.handleCancelOrder(cancel).isEmpty())
        assertEquals(BigDecimal.ZERO, saved["order"]!!.remainedTransferAmount)
        assertEquals(4L, saved["order"]!!.filledQuantity)
    }

    @Test
    fun foreignCancelCannotReleaseOwnersReserve() = runBlocking {
        reserve()
        val error = runCatching { manager.handleCancelOrder(CancelOrderEvent("order", "attacker", 7,
            pair, 100, 10, 10, OrderDirection.BID, MatchConstraint.GTC, OrderType.LIMIT_ORDER)) }.exceptionOrNull()
        assertTrue(error is IllegalArgumentException)
        assertEquals(BigDecimal("1000"), saved["order"]!!.remainedTransferAmount)
    }

    @Test
    fun duplicateRequestKeepsReserveReferenceAndAdmissionUsesMatchingId() = runBlocking {
        val first = reserve().single()
        assertTrue(reserve().isEmpty())
        manager.handleNewOrder(CreateOrderEvent("order", "owner", 42, pair, 100, 10, 10,
            OrderDirection.BID, MatchConstraint.GTC, OrderType.LIMIT_ORDER))
        val projection = richOrders.last() as co.nilin.opex.accountant.core.inout.RichOrder
        assertEquals(42L, projection.orderId)
        assertEquals("stable-cid", projection.clientOrderId)
        assertEquals(first.uuid, actions.single().uuid)
    }
}
