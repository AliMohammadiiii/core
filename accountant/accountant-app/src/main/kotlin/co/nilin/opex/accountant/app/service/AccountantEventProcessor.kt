package co.nilin.opex.accountant.app.service

import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.accountant.core.api.TradeManager
import co.nilin.opex.accountant.core.spi.OrderPersister
import co.nilin.opex.matching.engine.core.eventh.events.*
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait

@Service
class AccountantEventProcessor(private val db: DatabaseClient, manager: ReactiveTransactionManager,
    private val orders: OrderPersister, private val orderManager: OrderManager, private val tradeManager: TradeManager) {
    private val tx = TransactionalOperator.create(manager)

    suspend fun process(event: CoreEvent) = tx.executeAndAwait {
        // Serialize temp replay and the pair's ordered matching stream across accountant replicas.
        db.sql("SELECT pg_advisory_xact_lock(hashtext(:pair))").bind("pair", event.pair.toString())
            .fetch().all().collectList().awaitSingle()
        val ready = when (event) {
            is TradeEvent -> orders.load(event.takerOuid) != null && orders.load(event.makerOuid) != null
            is OneOrderEvent -> orders.load(event.ouid()) != null
            else -> false
        }
        if (!ready) { dispatch(event); return@executeAndAwait }
        val key = when (event) {
            is TradeEvent -> "${event.pair}:trade:${event.tradeId}"
            is RejectOrderEvent -> "${event.pair}:reject:${event.ouid}:${event.requestedOperation}:${event.reason}"
            is OneOrderEvent -> "${event.pair}:${event.javaClass.simpleName}:${event.ouid()}"
            else -> return@executeAndAwait
        }
        val inserted = db.sql("INSERT INTO accountant_event_inbox(event_key) VALUES(:key) ON CONFLICT(event_key) DO NOTHING")
            .bind("key", key).fetch().rowsUpdated().awaitSingle()
        if (inserted != 0) dispatch(event)
    }

    private suspend fun dispatch(event: CoreEvent) {
        when (event) {
            is CreateOrderEvent -> orderManager.handleNewOrder(event)
            is CancelOrderEvent -> orderManager.handleCancelOrder(event)
            is RejectOrderEvent -> orderManager.handleRejectOrder(event)
            is UpdatedOrderEvent -> orderManager.handleUpdateOrder(event)
            is TradeEvent -> tradeManager.handleTrade(event)
        }
    }
}
