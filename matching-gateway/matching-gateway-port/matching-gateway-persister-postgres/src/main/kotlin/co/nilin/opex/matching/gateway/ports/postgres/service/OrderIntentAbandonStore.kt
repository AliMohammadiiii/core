package co.nilin.opex.matching.gateway.ports.postgres.service

import co.nilin.opex.matching.gateway.ports.postgres.dto.OrderIntent
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.math.BigDecimal

@Component
class OrderIntentAbandonStore(
    private val db: DatabaseClient,
    private val tx: TransactionalOperator,
    private val store: OrderIntentStore,
) {
    suspend fun insertOrRead(tombstone: OrderIntent): OrderIntent = tx.executeAndAwait {
        var insert = db.sql(
            """INSERT INTO order_intent(
                ouid, owner, client_order_id, payload_hash, symbol, order_type, side,
                quantity, price, stop_price, post_only, left_fraction, payload, status, cancel_requested
            ) VALUES (
                :ouid, :owner, :cid, :hash, :symbol, :type, :side,
                :quantity, :price, :stop, :post, :fraction, :payload, 'CANCELED', TRUE
            ) ON CONFLICT(owner, client_order_id) DO NOTHING""".trimIndent()
        )
            .bind("ouid", tombstone.ouid)
            .bind("owner", tombstone.owner)
            .bind("cid", tombstone.clientOrderId)
            .bind("hash", tombstone.payloadHash)
            .bind("symbol", tombstone.symbol)
            .bind("type", tombstone.type)
            .bind("side", tombstone.side)
            .bind("quantity", tombstone.quantity)
            .bind("price", tombstone.price)
            .bind("post", tombstone.postOnly)
            .bind("fraction", tombstone.leftFraction)
            .bind("payload", tombstone.payload)
        insert = if (tombstone.stopPrice == null) {
            insert.bindNull("stop", BigDecimal::class.java)
        } else {
            insert.bind("stop", tombstone.stopPrice)
        }
        insert.fetch().rowsUpdated().awaitSingle()
        store.find(tombstone.owner, tombstone.clientOrderId, null)
            ?: error("Order intent disappeared while abandoning ${tombstone.clientOrderId}")
    } ?: error("Abandon transaction did not produce an order intent")
}
