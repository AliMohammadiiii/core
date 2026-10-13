package co.nilin.opex.accountant.app.service

import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.accountant.ports.kafka.listener.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.eventh.events.SubmitOrderEvent
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.eventh.events.RejectOrderEvent
import co.nilin.opex.matching.engine.core.inout.RejectReason
import co.nilin.opex.matching.engine.core.inout.RequestedOperation
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent as ReservedOrder
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.r2dbc.core.DatabaseClient
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.transaction.reactive.executeAndAwait
import java.util.UUID
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlinx.coroutines.suspendCancellableCoroutine

@Service
class OrderAdmissionService(private val db: DatabaseClient, manager: ReactiveTransactionManager,
    private val orders: OrderManager, private val mapper: ObjectMapper,
    @Qualifier("accountantEventKafkaTemplate") private val eventProducer: KafkaTemplate<String?, CoreEvent>,
    @Qualifier("orderAdmissionKafkaTemplate") private val producer: KafkaTemplate<String, ReservedOrder>) {
    private val tx = TransactionalOperator.create(manager)

    suspend fun accept(event: OrderSubmitRequestEvent) {
        if (event.reservationConfirmed) return
        tx.executeAndAwait {
            val reserved = ReservedOrder(event.ouid, event.uuid, event.pair, event.price, event.quantity,
                event.direction, event.matchConstraint, event.orderType, event.userLevel, event.orderId,
                event.clientOrderId, event.postOnly, true, true)
            val inserted = db.sql("INSERT INTO order_admission(ouid,payload,reserve_uuid) VALUES(:ouid,:payload,:ref) ON CONFLICT(ouid) DO NOTHING")
                .bind("ouid", event.ouid).bind("payload", mapper.writeValueAsString(reserved))
                .bind("ref", reserveReference(event.ouid)).fetch().rowsUpdated().awaitSingle()
            if (inserted == 0) return@executeAndAwait
            orders.handleRequestOrder(SubmitOrderEvent(event.ouid, event.uuid, event.orderId, event.pair,
                event.price, event.quantity, event.quantity, event.direction, event.matchConstraint,
                event.orderType, event.userLevel, event.clientOrderId, event.postOnly))
        }
    }

    @Scheduled(fixedDelay = 500)
    fun publishReservedOrders() = runBlocking {
        repeat(50) {
            val sent = tx.executeAndAwait {
                val item = db.sql("""SELECT a.ouid,a.payload,f.status,
                    (SELECT e.body FROM fi_action_error e WHERE e.fa_id=f.id ORDER BY e.date DESC LIMIT 1) AS rejection_body
                    FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
                    WHERE a.published_at IS NULL AND (f.status='PROCESSED' OR
                        (f.status='ERROR' AND NOT a.reconciliation_required))
                    ORDER BY a.created_at FOR UPDATE OF a SKIP LOCKED LIMIT 1""")
                    .map { row, _ -> listOf(row.get("ouid", String::class.java)!!, row.get("payload", String::class.java)!!,
                        row.get("status", String::class.java)!!, row.get("rejection_body", String::class.java)) }
                    .one().awaitSingleOrNull() ?: return@executeAndAwait false
                val event = mapper.readValue(item[1], ReservedOrder::class.java)
                val failed = item[2] == "ERROR"
                if (failed) {
                    val body = runCatching { mapper.readTree(item[3] ?: "") }.getOrNull()
                    if (body?.get("code")?.asInt() != 6018 || body?.get("error")?.asText() != "NotEnoughBalance") {
                        // Keep the outcome unknown, but stop selecting it ahead of unrelated ready admissions.
                        // A subsequently reconciled PROCESSED transfer is eligible regardless of this flag.
                        db.sql("UPDATE order_admission SET reconciliation_required=true WHERE ouid=:ouid")
                            .bind("ouid", item[0]!!).fetch().rowsUpdated().awaitSingle()
                        return@executeAndAwait true
                    }
                }
                withTimeout(15000) {
                    suspendCancellableCoroutine<Unit> { cont ->
                        val future = if (failed) eventProducer.send("events_${event.pair}", event.pair.toString(),
                            RejectOrderEvent(event.ouid, event.uuid, event.pair, event.price, event.quantity,
                                event.direction, event.matchConstraint, event.orderType,
                                RequestedOperation.PLACE_ORDER, RejectReason.RESERVATION_FAILED))
                        else producer.send("orders_${event.pair}", event.pair.toString(), event)
                        future.addCallback({ cont.resume(Unit) }, { cont.resumeWithException(it) })
                    }
                }
                db.sql("UPDATE order_admission SET published_at=now() WHERE ouid=:ouid").bind("ouid", item[0]!!)
                    .fetch().rowsUpdated().awaitSingle()
                true
            }
            if (sent != true) return@runBlocking
        }
    }

    companion object {
        fun reserveReference(ouid: String) = UUID.nameUUIDFromBytes("opex:reserve:$ouid".toByteArray()).toString()
    }
}
