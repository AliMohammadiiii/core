package co.nilin.opex.matching.gateway.ports.postgres.service

import co.nilin.opex.matching.gateway.ports.postgres.dto.*
import io.r2dbc.spi.Row
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.stereotype.Component
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.math.BigDecimal
import java.time.OffsetDateTime

@Component
class OrderIntentStore(private val db: DatabaseClient, private val tx: TransactionalOperator) {
    suspend fun accept(candidate: OrderIntent): OrderIntent = tx.executeAndAwait {
        var insert = db.sql("""INSERT INTO order_intent(ouid,owner,client_order_id,payload_hash,symbol,order_type,side,
            quantity,price,stop_price,post_only,left_fraction,payload) VALUES(:ouid,:owner,:cid,:hash,:symbol,:type,:side,
            :quantity,:price,:stop,:post,:fraction,:payload) ON CONFLICT(owner,client_order_id) DO NOTHING""")
            .bind("ouid", candidate.ouid).bind("owner", candidate.owner).bind("cid", candidate.clientOrderId)
            .bind("hash", candidate.payloadHash).bind("symbol", candidate.symbol).bind("type", candidate.type)
            .bind("side", candidate.side).bind("quantity", candidate.quantity).bind("price", candidate.price)
            .bind("post", candidate.postOnly).bind("fraction", candidate.leftFraction).bind("payload", candidate.payload)
        insert = if (candidate.stopPrice == null) insert.bindNull("stop", BigDecimal::class.java)
            else insert.bind("stop", candidate.stopPrice)
        insert
            .fetch().rowsUpdated().awaitSingle()
        val saved = find(candidate.owner, candidate.clientOrderId, null)!!
        if (saved.payloadHash != candidate.payloadHash) throw OrderIntentConflict()
        if (saved.status in setOf("FILLED", "CANCELED", "REJECTED")) return@executeAndAwait saved
        enqueue(saved.ouid, "SUBMIT", saved.payload)
        saved
    }!!

    suspend fun find(owner: String, clientOrderId: String?, ouid: String?): OrderIntent? {
        require((clientOrderId != null) xor (ouid != null))
        val field = if (clientOrderId != null) "client_order_id" else "ouid"
        return db.sql("SELECT * FROM order_intent WHERE owner=:owner AND $field=:identity")
            .bind("owner", owner).bind("identity", clientOrderId ?: ouid!!)
            .map { row, _ -> intent(row) }.one().awaitSingleOrNull()
    }

    suspend fun owned(owner: String): List<OrderIntent> = db.sql("""SELECT * FROM order_intent
        WHERE owner=:owner AND status NOT IN ('FILLED','CANCELED','REJECTED') ORDER BY created_at,ouid""")
        .bind("owner", owner).map { row, _ -> intent(row) }.all().collectList().awaitSingle()

    suspend fun metadata(owner: String): List<OrderIntent> = db.sql("""SELECT * FROM order_intent
        WHERE owner=:owner ORDER BY created_at DESC,ouid DESC LIMIT 500""")
        .bind("owner", owner).map { row, _ -> intent(row) }.all().collectList().awaitSingle()

    suspend fun conditionalOrdersReady(): List<OrderIntent> = tx.executeAndAwait {
        db.sql("""UPDATE order_intent i SET status='WAITING_TRIGGER'
            FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
            WHERE i.ouid=a.ouid AND i.order_type='STOP_LIMIT'
              AND i.status='ACCEPTED_PENDING' AND NOT i.cancel_requested
              AND a.conditional AND NOT a.cancel_requested AND a.published_at IS NULL
              AND f.status='PROCESSED'""").fetch().rowsUpdated().awaitSingle()
        db.sql("""SELECT i.* FROM order_intent i
            JOIN order_admission a ON a.ouid=i.ouid
            JOIN fi_actions f ON f.uuid=a.reserve_uuid
            WHERE i.order_type='STOP_LIMIT' AND i.status='WAITING_TRIGGER'
              AND NOT i.cancel_requested AND a.conditional AND NOT a.cancel_requested
              AND a.published_at IS NULL AND f.status='PROCESSED'
            ORDER BY i.created_at,i.ouid LIMIT 500""")
            .map { row, _ -> intent(row) }.all().collectList().awaitSingle()
    }!!

    suspend fun activateConditional(owner: String, ouid: String, payload: String): Boolean = tx.executeAndAwait {
        val updated = db.sql("""UPDATE order_intent SET status='TRIGGERED'
            WHERE owner=:owner AND ouid=:ouid AND order_type='STOP_LIMIT'
              AND status='WAITING_TRIGGER' AND NOT cancel_requested""")
            .bind("owner", owner).bind("ouid", ouid).fetch().rowsUpdated().awaitSingle()
        if (updated == 0) return@executeAndAwait false
        enqueue(ouid, "ACTIVATE", payload)
        true
    }!!

    suspend fun cancel(owner: String, clientOrderId: String?, ouid: String?, payload: String): OrderIntent? = tx.executeAndAwait {
        val found = find(owner, clientOrderId, ouid) ?: return@executeAndAwait null
        if (found.status in listOf("FILLED", "CANCELED", "REJECTED")) return@executeAndAwait found
        db.sql("UPDATE order_intent SET cancel_requested=true WHERE ouid=:ouid AND owner=:owner")
            .bind("ouid", found.ouid).bind("owner", owner).fetch().rowsUpdated().awaitSingle()
        enqueue(found.ouid, "CANCEL", payload)
        find(owner, null, found.ouid)
    }

    private suspend fun enqueue(ouid: String, kind: String, payload: String) {
        db.sql("INSERT INTO order_intent_outbox(ouid,kind,payload) VALUES(:ouid,:kind,:payload) ON CONFLICT(ouid,kind) DO NOTHING")
            .bind("ouid", ouid).bind("kind", kind).bind("payload", payload).fetch().rowsUpdated().awaitSingle()
    }

    // Kafka delivery is at least once. Locks and the receipt transaction prevent an unrecorded publication.
    suspend fun publishNext(publish: suspend (String, String) -> Unit): Boolean = tx.executeAndAwait {
        val next = db.sql("""SELECT o.id,o.kind,o.payload FROM order_intent_outbox o
            WHERE o.published_at IS NULL
              AND NOT EXISTS (SELECT 1 FROM order_intent_outbox earlier
                WHERE earlier.published_at IS NULL AND earlier.id<o.id)
            ORDER BY o.id FOR UPDATE SKIP LOCKED LIMIT 1""")
            .map { row, _ -> Triple(row.get("id", java.lang.Long::class.java)!!.toLong(),
                row.get("kind", String::class.java)!!, row.get("payload", String::class.java)!!) }
            .one().awaitSingleOrNull() ?: return@executeAndAwait false
        publish(next.second, next.third)
        db.sql("UPDATE order_intent_outbox SET published_at=now() WHERE id=:id").bind("id", next.first)
            .fetch().rowsUpdated().awaitSingle()
        true
    }!!

    suspend fun conditionalStatus(owner: String, ouid: String, key: String, payload: String, status: String): Boolean {
        if (status !in setOf("WAITING_TRIGGER", "TRIGGERED", "CANCELED", "REJECTED")) return false
        val intent = find(owner, null, ouid) ?: return false
        if (intent.type != "STOP_LIMIT") return false
        outcome(owner, ouid, key, payload, status, null, null, null)
        return true
    }

    suspend fun outcome(owner: String, ouid: String, key: String, payload: String, status: String,
                        orderId: Long?, remaining: Long?, reason: String?) = tx.executeAndAwait {
        // Hold a per-owner transaction lock before BIGSERIAL allocation, through commit.
        // Forward readers can never observe a later same-owner cursor while an earlier event is uncommitted.
        db.sql("SELECT pg_advisory_xact_lock(hashtextextended(:scope,0))")
            .bind("scope", "opex:intent-events:$owner").map { _, _ -> true }.one().awaitSingle()
        val found = find(owner, null, ouid) ?: return@executeAndAwait
        val inserted = db.sql("INSERT INTO order_intent_event(event_key,owner,ouid,payload) VALUES(:key,:owner,:ouid,:payload) ON CONFLICT(event_key) DO NOTHING")
            .bind("key", key).bind("owner", owner).bind("ouid", ouid).bind("payload", payload)
            .fetch().rowsUpdated().awaitSingle()
        if (inserted == 0) return@executeAndAwait
        val filled = if (remaining == null) found.filledQuantity else found.quantity.subtract(remaining.toBigDecimal().multiply(found.leftFraction))
        require(filled >= BigDecimal.ZERO && filled <= found.quantity) { "Invalid matching quantity for intent" }
        var update = db.sql("""UPDATE order_intent SET order_id=coalesce(order_id,:id),
            filled_quantity=greatest(filled_quantity,:filled),
            status=CASE WHEN status IN ('FILLED','CANCELED','REJECTED') THEN status
                WHEN :status IN ('WAITING_TRIGGER','TRIGGERED') AND status IN ('OPEN','PARTIALLY_FILLED') THEN status
                WHEN :status='WAITING_TRIGGER' AND status='TRIGGERED' THEN status
                WHEN :status='OPEN' AND filled_quantity>0 THEN status ELSE :status END,
            reason=coalesce(:reason,reason) WHERE ouid=:ouid AND owner=:owner""")
            .bind("filled", filled).bind("status", status).bind("ouid", ouid).bind("owner", owner)
        update = if (orderId == null) update.bindNull("id", java.lang.Long::class.java) else update.bind("id", orderId)
        update = if (reason == null) update.bindNull("reason", String::class.java) else update.bind("reason", reason)
        update.fetch().rowsUpdated().awaitSingle()
    }

    suspend fun events(owner: String, cursor: Long): List<Map<String, Any>> {
        require(cursor >= 0)
        return db.sql("SELECT cursor,ouid,payload FROM order_intent_event WHERE owner=:owner AND cursor>:cursor ORDER BY cursor LIMIT 500")
            .bind("owner", owner).bind("cursor", cursor).map { row, _ -> mapOf<String, Any>(
                "cursor" to row.get("cursor", java.lang.Long::class.java)!!.toLong(),
                "ouid" to row.get("ouid", String::class.java)!!, "payload" to row.get("payload", String::class.java)!!) }
            .all().collectList().awaitSingle()
    }

    private fun intent(row: Row) = OrderIntent(
        row.get("owner", String::class.java)!!, row.get("client_order_id", String::class.java)!!,
        row.get("ouid", String::class.java)!!, row.get("payload_hash", String::class.java)!!,
        row.get("symbol", String::class.java)!!, row.get("order_type", String::class.java)!!,
        row.get("side", String::class.java)!!, row.get("quantity", BigDecimal::class.java)!!,
        row.get("price", BigDecimal::class.java)!!, row.get("post_only", java.lang.Boolean::class.java)!!.booleanValue(),
        row.get("left_fraction", BigDecimal::class.java)!!, row.get("payload", String::class.java)!!,
        row.get("status", String::class.java)!!, row.get("order_id", java.lang.Long::class.java)?.toLong(),
        row.get("filled_quantity", BigDecimal::class.java)!!,
        row.get("cancel_requested", java.lang.Boolean::class.java)!!.booleanValue(), row.get("reason", String::class.java),
        row.get("created_at", OffsetDateTime::class.java)!!, row.get("stop_price", BigDecimal::class.java))
}
