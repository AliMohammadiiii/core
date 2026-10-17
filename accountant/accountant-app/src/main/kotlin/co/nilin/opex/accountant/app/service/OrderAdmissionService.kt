package co.nilin.opex.accountant.app.service

import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.accountant.core.inout.OrderStatus
import co.nilin.opex.accountant.ports.kafka.listener.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.eventh.events.CancelOrderEvent
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.eventh.events.RejectOrderEvent
import co.nilin.opex.matching.engine.core.inout.OrderCancelRequestEvent
import co.nilin.opex.matching.engine.core.inout.OrderRequestEvent
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent as ReservedOrder
import co.nilin.opex.matching.engine.core.inout.RejectReason
import co.nilin.opex.matching.engine.core.inout.RequestedOperation
import co.nilin.opex.matching.engine.core.eventh.events.SubmitOrderEvent
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
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
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

private data class AdmissionDispatch(
    val ouid: String,
    val payload: String,
    val status: String,
    val rejectionBody: String?,
    val conditional: Boolean,
    val activationRequested: Boolean,
    val cancelRequested: Boolean,
    val publicationStarted: Boolean
)

private data class ActiveAdmissionCancel(
    val ouid: String,
    val payload: String,
    val orderId: Long,
    val status: Int
)

private data class ConditionalStatusCandidate(
    val ouid: String,
    val payload: String,
    val reserveStatus: String?,
    val rejectionBody: String?,
    val reconciliationRequired: Boolean,
    val activationRequested: Boolean,
    val cancelRequested: Boolean,
    val orderExists: Boolean,
    val reserveArchived: Boolean,
)

private data class AdmissionStatusOutbox(
    val id: Long,
    val ouid: String,
    val uuid: String,
    val status: String,
)

private data class ConditionalStatusPage(val lastOuid: String?, val hasMore: Boolean)

private data class AdmissionControl(
    val payload: String,
    val conditional: Boolean,
    val published: Boolean,
    val publicationStarted: Boolean,
    val activationRequested: Boolean,
    val cancelRequested: Boolean,
    val reservationConfirmed: Boolean
)

private sealed class ReservedOrderClaim {
    data class Claimed(val dispatch: AdmissionDispatch) : ReservedOrderClaim()
    object Skipped : ReservedOrderClaim()
    object Empty : ReservedOrderClaim()
}

@Service
class OrderAdmissionService(
    private val db: DatabaseClient,
    manager: ReactiveTransactionManager,
    private val orders: OrderManager,
    private val mapper: ObjectMapper,
    @Qualifier("accountantEventKafkaTemplate") private val eventProducer: KafkaTemplate<String?, CoreEvent>,
    @Qualifier("orderAdmissionKafkaTemplate") private val producer: KafkaTemplate<String, OrderRequestEvent>,
    @Qualifier("orderAdmissionStatusKafkaTemplate") private val statusProducer: KafkaTemplate<String, String>,
) {
    private val tx = TransactionalOperator.create(manager)
    @Volatile private var conditionalStatusAfterOuid: String? = null

    suspend fun accept(event: OrderSubmitRequestEvent) {
        if (event.reservationConfirmed) return
        val conditional = event.conditionalAction == "HOLD"
        require(event.conditionalAction == null || conditional) { "Invalid conditional admission action" }
        if (conditional) require(event.stopPrice?.signum() == 1) { "Conditional admission requires a positive stop price" }
        tx.executeAndAwait {
            val reserved = ReservedOrder(
                event.ouid, event.uuid, event.pair, event.price, event.quantity,
                event.direction, event.matchConstraint, event.orderType, event.userLevel, event.orderId,
                event.clientOrderId, event.postOnly, true, true, event.conditionalAction, event.stopPrice
            )
            val inserted = db.sql("""INSERT INTO order_admission
                (ouid,payload,reserve_uuid,conditional)
                VALUES(:ouid,:payload,:ref,:conditional) ON CONFLICT(ouid) DO NOTHING""")
                .bind("ouid", event.ouid).bind("payload", mapper.writeValueAsString(reserved))
                .bind("ref", reserveReference(event.ouid)).bind("conditional", conditional)
                .fetch().rowsUpdated().awaitSingle()
            if (inserted == 0) {
                val existing = loadForUpdate(event.ouid) ?: error("Admission row disappeared during duplicate submit")
                val current = mapper.readValue(existing.payload, ReservedOrder::class.java)
                require(current.uuid == event.uuid) { "Order admission owner mismatch" }
                applyPendingCommand(event.ouid, event.uuid, existing.conditional)
                return@executeAndAwait
            }
            orders.handleRequestOrder(SubmitOrderEvent(
                event.ouid, event.uuid, event.orderId, event.pair,
                event.price, event.quantity, event.quantity, event.direction, event.matchConstraint,
                event.orderType, event.userLevel, event.clientOrderId, event.postOnly
            ))
            applyPendingCommand(event.ouid, event.uuid, conditional)
        }
    }

    suspend fun activateConditional(event: OrderSubmitRequestEvent) {
        tx.executeAndAwait {
            val row = loadForUpdate(event.ouid)
            if (row == null) {
                queueConditionalCommand(event, "ACTIVATE")
                return@executeAndAwait
            }
            val reserved = mapper.readValue(row.payload, ReservedOrder::class.java)
            require(reserved.uuid == event.uuid) { "Conditional activation owner mismatch" }
            if (!row.conditional || row.published || row.publicationStarted || row.activationRequested || row.cancelRequested) return@executeAndAwait
            db.sql("""UPDATE order_admission SET activation_requested=true
                WHERE ouid=:ouid AND conditional AND published_at IS NULL AND NOT cancel_requested""")
                .bind("ouid", event.ouid).fetch().rowsUpdated().awaitSingle()
        }
    }

    suspend fun cancelConditional(event: OrderSubmitRequestEvent) {
        tx.executeAndAwait {
            val row = loadForUpdate(event.ouid)
            if (row == null) {
                queueConditionalCommand(event, "CANCEL")
                return@executeAndAwait
            }
            val reserved = mapper.readValue(row.payload, ReservedOrder::class.java)
            require(reserved.uuid == event.uuid) { "Conditional cancellation owner mismatch" }
            if (!row.conditional) return@executeAndAwait
            if (row.cancelRequested) return@executeAndAwait
            if (row.publicationStarted || row.published) {
                // An acknowledged or ambiguous engine publication may execute; preserve it and cancel in-engine.
                db.sql("UPDATE order_admission SET cancel_requested=true,activation_requested=true WHERE ouid=:ouid")
                    .bind("ouid", event.ouid).fetch().rowsUpdated().awaitSingle()
            } else {
                // A pending activation loses to cancellation while the admission row is locked.
                db.sql("""UPDATE order_admission SET cancel_requested=true,activation_requested=false
                    WHERE ouid=:ouid AND conditional AND published_at IS NULL""")
                    .bind("ouid", event.ouid).fetch().rowsUpdated().awaitSingle()
            }
        }
    }

    private suspend fun loadForUpdate(ouid: String): AdmissionControl? = db.sql("""SELECT payload,conditional,
        published_at IS NOT NULL AS published,publication_started_at IS NOT NULL AS publication_started,
        activation_requested,cancel_requested,
        COALESCE((SELECT f.status='PROCESSED' FROM fi_actions f WHERE f.uuid=a.reserve_uuid),FALSE) AS reservation_confirmed
        FROM order_admission a WHERE ouid=:ouid FOR UPDATE""")
        .bind("ouid", ouid)
        .map { row, _ -> AdmissionControl(
            row.get("payload", String::class.java)!!,
            row.get("conditional", java.lang.Boolean::class.java)!!.booleanValue(),
            row.get("published", java.lang.Boolean::class.java)!!.booleanValue(),
            row.get("publication_started", java.lang.Boolean::class.java)!!.booleanValue(),
            row.get("activation_requested", java.lang.Boolean::class.java)!!.booleanValue(),
            row.get("cancel_requested", java.lang.Boolean::class.java)!!.booleanValue(),
            row.get("reservation_confirmed", java.lang.Boolean::class.java)!!.booleanValue()
        ) }.one().awaitSingleOrNull()

    private suspend fun queueConditionalCommand(event: OrderSubmitRequestEvent, action: String) {
        db.sql("""INSERT INTO order_admission_commands(ouid,uuid,action) VALUES(:ouid,:uuid,:action)
            ON CONFLICT(ouid) DO UPDATE SET action=CASE
                WHEN order_admission_commands.action='CANCEL' OR EXCLUDED.action='CANCEL' THEN 'CANCEL'
                ELSE 'ACTIVATE' END""")
            .bind("ouid", event.ouid).bind("uuid", event.uuid).bind("action", action)
            .fetch().rowsUpdated().awaitSingle()
        val queuedOwner = db.sql("SELECT uuid FROM order_admission_commands WHERE ouid=:ouid FOR UPDATE")
            .bind("ouid", event.ouid)
            .map { row, _ -> row.get("uuid", String::class.java)!! }.one().awaitSingleOrNull()
        require(queuedOwner == event.uuid) { "Conditional command owner mismatch" }
        val row = loadForUpdate(event.ouid) ?: return
        val reserved = mapper.readValue(row.payload, ReservedOrder::class.java)
        require(reserved.uuid == event.uuid) { "Conditional command owner mismatch" }
        applyPendingCommand(event.ouid, event.uuid, row.conditional)
    }

    private suspend fun applyPendingCommand(ouid: String, uuid: String, conditional: Boolean) {
        val command = db.sql("SELECT uuid,action FROM order_admission_commands WHERE ouid=:ouid FOR UPDATE")
            .bind("ouid", ouid)
            .map { row, _ -> Pair(row.get("uuid", String::class.java)!!, row.get("action", String::class.java)!!) }
            .one().awaitSingleOrNull() ?: return
        require(command.first == uuid) { "Conditional command owner mismatch" }
        if (conditional) {
            when (command.second) {
                "ACTIVATE" -> db.sql("""UPDATE order_admission SET activation_requested=true
                    WHERE ouid=:ouid AND conditional AND NOT cancel_requested AND published_at IS NULL
                        AND publication_started_at IS NULL""")
                    .bind("ouid", ouid).fetch().rowsUpdated().awaitSingle()
                "CANCEL" -> db.sql("""UPDATE order_admission SET cancel_requested=true,
                    activation_requested=CASE WHEN publication_started_at IS NULL AND published_at IS NULL THEN false ELSE true END
                    WHERE ouid=:ouid AND conditional""")
                    .bind("ouid", ouid).fetch().rowsUpdated().awaitSingle()
            }
        }
        db.sql("DELETE FROM order_admission_commands WHERE ouid=:ouid AND uuid=:uuid")
            .bind("ouid", ouid).bind("uuid", uuid).fetch().rowsUpdated().awaitSingle()
    }

    @Scheduled(fixedDelay = 500)
    fun processPendingConditionalCommands() = runBlocking {
        repeat(100) {
            val applied = tx.executeAndAwait {
                val command = db.sql("""SELECT c.ouid,c.uuid,a.conditional FROM order_admission_commands c
                    JOIN order_admission a ON a.ouid=c.ouid
                    ORDER BY c.created_at LIMIT 1 FOR UPDATE OF c,a SKIP LOCKED""")
                    .map { row, _ -> Triple(
                        row.get("ouid", String::class.java)!!,
                        row.get("uuid", String::class.java)!!,
                        row.get("conditional", java.lang.Boolean::class.java)!!.booleanValue()
                    ) }.one().awaitSingleOrNull() ?: return@executeAndAwait false
                applyPendingCommand(command.first, command.second, command.third)
                true
            }
            if (applied != true) return@runBlocking
        }
    }

    suspend fun pendingConditionalTriggers(afterOuid: String? = null, limit: Int = 500): List<ReservedOrder> {
        require(limit in 1..500) { "Conditional trigger page size must be between 1 and 500" }
        val query = if (afterOuid == null) {
            db.sql("""SELECT a.payload
                FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
                WHERE a.conditional AND a.published_at IS NULL AND a.publication_started_at IS NULL
                    AND NOT a.activation_requested AND NOT a.cancel_requested
                    AND NOT a.reconciliation_required AND f.status='PROCESSED'
                ORDER BY a.ouid LIMIT :limit""")
        } else {
            db.sql("""SELECT a.payload
                FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
                WHERE a.conditional AND a.published_at IS NULL AND a.publication_started_at IS NULL
                    AND NOT a.activation_requested AND NOT a.cancel_requested
                    AND NOT a.reconciliation_required AND f.status='PROCESSED'
                    AND a.ouid > :afterOuid
                ORDER BY a.ouid LIMIT :limit""")
                .bind("afterOuid", afterOuid)
        }
        return query.bind("limit", limit)
            .map { row, _ -> mapper.readValue(row.get("payload", String::class.java)!!, ReservedOrder::class.java) }
            .all().collectList().awaitSingle()
    }

    suspend fun activateIfTriggered(
        candidate: ReservedOrder,
        tradePrice: BigDecimal,
        tradeTime: Instant,
        now: Instant,
        maxTradeAge: Duration,
    ): Boolean {
        if (tradeTime.isAfter(now) || tradeTime.isBefore(now.minus(maxTradeAge))) return false
        return tx.executeAndAwait {
            val row = loadForUpdate(candidate.ouid) ?: return@executeAndAwait false
            val reserved = mapper.readValue(row.payload, ReservedOrder::class.java)
            require(reserved.uuid == candidate.uuid) { "Conditional trigger owner mismatch" }
            if (!row.conditional || row.published || row.publicationStarted || row.activationRequested ||
                row.cancelRequested || !row.reservationConfirmed
            ) return@executeAndAwait false
            val stopPrice = reserved.stopPrice ?: return@executeAndAwait false
            val reached = when (reserved.direction) {
                co.nilin.opex.matching.engine.core.model.OrderDirection.BID -> tradePrice >= stopPrice
                co.nilin.opex.matching.engine.core.model.OrderDirection.ASK -> tradePrice <= stopPrice
            }
            if (!reached) return@executeAndAwait false
            db.sql("""UPDATE order_admission SET activation_requested=true
                WHERE ouid=:ouid AND conditional AND published_at IS NULL
                    AND publication_started_at IS NULL AND NOT cancel_requested""")
                .bind("ouid", candidate.ouid).fetch().rowsUpdated().awaitSingle() == 1
        } ?: false
    }

    @Scheduled(fixedDelay = 500)
    fun publishReservedOrders() = runBlocking {
        repeat(50) {
            val item = when (val claim = claimNextReservedOrder()) {
                is ReservedOrderClaim.Claimed -> claim.dispatch
                ReservedOrderClaim.Skipped -> return@repeat
                ReservedOrderClaim.Empty -> return@runBlocking
            }
            val event = mapper.readValue(item.payload, ReservedOrder::class.java)
            val matcherEvent = if (item.conditional && item.activationRequested) {
                // Conditional admission metadata is accountant-only. The matcher sees an ordinary
                // reserved order after the accountant has durably accepted the trigger.
                ReservedOrder(
                    event.ouid, event.uuid, event.pair, event.price, event.quantity,
                    event.direction, event.matchConstraint, event.orderType, event.userLevel,
                    event.orderId, event.clientOrderId, event.postOnly,
                    event.requiresReservation, event.reservationConfirmed, null, null,
                )
            } else {
                event
            }
            val future = when {
                item.status == "ERROR" -> eventProducer.send(
                    "events_${event.pair}", event.pair.toString(),
                    RejectOrderEvent(
                        event.ouid, event.uuid, event.pair, event.price, event.quantity,
                        event.direction, event.matchConstraint, event.orderType,
                        RequestedOperation.PLACE_ORDER, RejectReason.RESERVATION_FAILED,
                    ),
                )
                item.conditional && item.cancelRequested && !item.publicationStarted -> eventProducer.send(
                    "events_${event.pair}", event.pair.toString(),
                    CancelOrderEvent(
                        event.ouid, event.uuid, event.orderId ?: 0L, event.pair, event.price,
                        event.quantity, event.quantity, event.direction, event.matchConstraint, event.orderType,
                    ),
                )
                else -> producer.send("orders_${event.pair}", event.pair.toString(), matcherEvent)
            }
            awaitSend(future)
            tx.executeAndAwait {
                db.sql("UPDATE order_admission SET published_at=now() WHERE ouid=:ouid AND published_at IS NULL")
                    .bind("ouid", item.ouid).fetch().rowsUpdated().awaitSingle()
            }
        }
    }

    @Scheduled(fixedDelay = 500)
    fun publishConditionalStatusAcknowledgements() = runBlocking {
        for (page in 0 until 10) {
            if (!reconcileConditionalStatusesPage()) break
        }
        repeat(100) {
            val item = tx.executeAndAwait {
                val next = db.sql("""SELECT id,ouid,uuid,status FROM order_admission_status_outbox
                    WHERE published_at IS NULL
                      AND (dispatch_attempted_at IS NULL OR dispatch_attempted_at < now() - interval '15 seconds')
                    ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED""")
                    .map { row, _ -> AdmissionStatusOutbox(
                        row.get("id", java.lang.Long::class.java)!!.toLong(),
                        row.get("ouid", String::class.java)!!,
                        row.get("uuid", String::class.java)!!,
                        row.get("status", String::class.java)!!,
                    ) }.one().awaitSingleOrNull() ?: return@executeAndAwait null
                db.sql("UPDATE order_admission_status_outbox SET dispatch_attempted_at=now() WHERE id=:id")
                    .bind("id", next.id).fetch().rowsUpdated().awaitSingle()
                next
            }
            if (item == null) return@runBlocking

            val idempotencyKey = "conditional-admission:${item.ouid}:${item.status}"
            val payload = mapper.writeValueAsString(mapOf(
                "uuid" to item.uuid,
                "ouid" to item.ouid,
                "status" to item.status,
                "idempotencyKey" to idempotencyKey,
            ))
            awaitSend(statusProducer.send("order_admission_status", item.ouid, payload))
            tx.executeAndAwait {
                db.sql("UPDATE order_admission_status_outbox SET published_at=now() WHERE id=:id AND published_at IS NULL")
                    .bind("id", item.id).fetch().rowsUpdated().awaitSingle()
            }
        }
    }

    private suspend fun reconcileConditionalStatusesPage(): Boolean {
        val afterOuid = conditionalStatusAfterOuid
        val page = tx.executeAndAwait {
            val statement = if (afterOuid == null) {
                db.sql("""SELECT a.ouid,a.payload,COALESCE(f.status,reserve_archive.status) AS reserve_status,
                    (SELECT e.body FROM fi_action_error e WHERE e.fa_id=f.id ORDER BY e.date DESC LIMIT 1) AS rejection_body,
                    a.reconciliation_required,a.activation_requested,a.cancel_requested,
                    EXISTS(SELECT 1 FROM orders o WHERE o.ouid=a.ouid) AS order_exists,
                    EXISTS(SELECT 1 FROM fi_actions_archive archived WHERE archived.uuid=a.reserve_uuid) AS reserve_archived
                    FROM order_admission a
                    LEFT JOIN fi_actions f ON f.uuid=a.reserve_uuid
                    LEFT JOIN fi_actions_archive reserve_archive ON reserve_archive.uuid=a.reserve_uuid
                    WHERE a.conditional ORDER BY a.ouid LIMIT 500 FOR UPDATE OF a SKIP LOCKED""")
            } else {
                db.sql("""SELECT a.ouid,a.payload,COALESCE(f.status,reserve_archive.status) AS reserve_status,
                    (SELECT e.body FROM fi_action_error e WHERE e.fa_id=f.id ORDER BY e.date DESC LIMIT 1) AS rejection_body,
                    a.reconciliation_required,a.activation_requested,a.cancel_requested,
                    EXISTS(SELECT 1 FROM orders o WHERE o.ouid=a.ouid) AS order_exists,
                    EXISTS(SELECT 1 FROM fi_actions_archive archived WHERE archived.uuid=a.reserve_uuid) AS reserve_archived
                    FROM order_admission a
                    LEFT JOIN fi_actions f ON f.uuid=a.reserve_uuid
                    LEFT JOIN fi_actions_archive reserve_archive ON reserve_archive.uuid=a.reserve_uuid
                    WHERE a.conditional AND a.ouid > :afterOuid
                    ORDER BY a.ouid LIMIT 500 FOR UPDATE OF a SKIP LOCKED""")
                    .bind("afterOuid", afterOuid)
            }
            val candidates = statement.map { row, _ -> ConditionalStatusCandidate(
                row.get("ouid", String::class.java)!!,
                row.get("payload", String::class.java)!!,
                row.get("reserve_status", String::class.java),
                row.get("rejection_body", String::class.java),
                row.get("reconciliation_required", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("activation_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("cancel_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("order_exists", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("reserve_archived", java.lang.Boolean::class.java)!!.booleanValue(),
            ) }.all().collectList().awaitSingle()

            if (candidates.isEmpty()) return@executeAndAwait ConditionalStatusPage(null, false)

            for (candidate in candidates) {
                val body = if (candidate.reserveStatus == "ERROR" && !candidate.reconciliationRequired) {
                    runCatching { mapper.readTree(candidate.rejectionBody ?: "") }.getOrNull()
                } else null
                val definiteBalanceFailure = body?.get("code")?.asInt() == 6018 &&
                    body.get("error")?.asText() == "NotEnoughBalance"
                val neverReservedCancel = candidate.reserveStatus == null && candidate.cancelRequested &&
                    !candidate.orderExists && !candidate.reserveArchived
                val releaseProcessed = if (candidate.reserveStatus == "PROCESSED" && candidate.cancelRequested) {
                db.sql("""
                    SELECT EXISTS(
                        SELECT 1 FROM fi_actions WHERE uuid=:uuid AND status='PROCESSED'
                    ) OR EXISTS(
                        SELECT 1 FROM fi_actions_archive WHERE uuid=:uuid AND status='PROCESSED'
                    ) AS processed
                """.trimIndent())
                        .bind("uuid", releaseReference(candidate.ouid))
                    .map { row, _ -> row.get("processed", java.lang.Boolean::class.java)!!.booleanValue() }
                    .one().awaitSingle()
                } else false
                val shouldAcknowledge = definiteBalanceFailure || neverReservedCancel ||
                    (candidate.reserveStatus == "PROCESSED" &&
                        (candidate.activationRequested || !candidate.cancelRequested || releaseProcessed))
                if (!shouldAcknowledge) continue

                val order = mapper.readValue(candidate.payload, ReservedOrder::class.java)
                when {
                    definiteBalanceFailure -> enqueueConditionalStatus(candidate.ouid, order.uuid, "REJECTED")
                    neverReservedCancel -> enqueueConditionalStatus(candidate.ouid, order.uuid, "CANCELED")
                    candidate.reserveStatus == "PROCESSED" -> {
                        if (candidate.activationRequested) {
                            enqueueConditionalStatus(candidate.ouid, order.uuid, "TRIGGERED")
                        } else if (!candidate.cancelRequested) {
                            enqueueConditionalStatus(candidate.ouid, order.uuid, "WAITING_TRIGGER")
                        }
                        if (releaseProcessed) enqueueConditionalStatus(candidate.ouid, order.uuid, "CANCELED")
                    }
                }
            }
            ConditionalStatusPage(candidates.last().ouid, candidates.size == 500)
        } ?: ConditionalStatusPage(null, false)

        conditionalStatusAfterOuid = if (page.hasMore) page.lastOuid else null
        return page.hasMore
    }

    private suspend fun enqueueConditionalStatus(ouid: String, uuid: String, status: String) {
        db.sql("""INSERT INTO order_admission_status_outbox(ouid,uuid,status)
            VALUES(:ouid,:uuid,:status) ON CONFLICT(ouid,status) DO NOTHING""")
            .bind("ouid", ouid).bind("uuid", uuid).bind("status", status)
            .fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun claimNextReservedOrder(): ReservedOrderClaim = tx.executeAndAwait {
        val item = db.sql("""SELECT a.ouid,a.payload,f.status,
            (SELECT e.body FROM fi_action_error e WHERE e.fa_id=f.id ORDER BY e.date DESC LIMIT 1) AS rejection_body,
            a.conditional,a.activation_requested,a.cancel_requested,
            a.publication_started_at IS NOT NULL AS publication_started
            FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
            WHERE a.published_at IS NULL
                AND (NOT a.conditional OR a.activation_requested OR a.cancel_requested OR f.status='ERROR')
                AND (f.status='PROCESSED' OR (f.status='ERROR' AND NOT a.reconciliation_required))
                AND (a.dispatch_attempted_at IS NULL OR a.dispatch_attempted_at < now() - interval '15 seconds')
            ORDER BY a.created_at LIMIT 1 FOR UPDATE OF a SKIP LOCKED""")
            .map { row, _ -> AdmissionDispatch(
                row.get("ouid", String::class.java)!!,
                row.get("payload", String::class.java)!!,
                row.get("status", String::class.java)!!,
                row.get("rejection_body", String::class.java),
                row.get("conditional", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("activation_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("cancel_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("publication_started", java.lang.Boolean::class.java)!!.booleanValue(),
            ) }.one().awaitSingleOrNull() ?: return@executeAndAwait ReservedOrderClaim.Empty

        if (item.status == "ERROR") {
            val body = runCatching { mapper.readTree(item.rejectionBody ?: "") }.getOrNull()
            if (body?.get("code")?.asInt() != 6018 || body?.get("error")?.asText() != "NotEnoughBalance") {
                db.sql("UPDATE order_admission SET reconciliation_required=true WHERE ouid=:ouid")
                    .bind("ouid", item.ouid).fetch().rowsUpdated().awaitSingle()
                return@executeAndAwait ReservedOrderClaim.Skipped
            }
        }

        val releasesDormantHold = item.conditional && item.cancelRequested && !item.publicationStarted
        val claimSql = if (releasesDormantHold) {
            "UPDATE order_admission SET dispatch_attempted_at=now() WHERE ouid=:ouid AND published_at IS NULL"
        } else {
            """UPDATE order_admission SET publication_started_at=COALESCE(publication_started_at,now()),
                dispatch_attempted_at=now() WHERE ouid=:ouid AND published_at IS NULL"""
        }
        db.sql(claimSql).bind("ouid", item.ouid).fetch().rowsUpdated().awaitSingle()
        ReservedOrderClaim.Claimed(item)
    } ?: ReservedOrderClaim.Empty

    private suspend fun <T> awaitSend(future: org.springframework.util.concurrent.ListenableFuture<T>) {
        withTimeout(15000) {
            suspendCancellableCoroutine<Unit> { continuation ->
                future.addCallback({ continuation.resume(Unit) }, { continuation.resumeWithException(it) })
            }
        }
    }

    @Scheduled(fixedDelay = 500)
    fun publishActiveConditionalCancels() = runBlocking {
        repeat(50) {
            val item = tx.executeAndAwait {
                val next = db.sql("""SELECT a.ouid,a.payload,o.matching_engine_id,o.status
                    FROM order_admission a JOIN orders o ON o.ouid=a.ouid
                WHERE a.conditional AND a.publication_started_at IS NOT NULL
                    AND a.cancel_requested AND a.cancel_sent_at IS NULL
                      AND o.matching_engine_id IS NOT NULL
                ORDER BY a.created_at LIMIT 1 FOR UPDATE OF a SKIP LOCKED""")
                    .map { row, _ -> ActiveAdmissionCancel(
                        row.get("ouid", String::class.java)!!,
                        row.get("payload", String::class.java)!!,
                        row.get("matching_engine_id", java.lang.Integer::class.java)!!.toLong(),
                row.get("status", java.lang.Integer::class.java)!!.toInt()
                    ) }.one().awaitSingleOrNull() ?: return@executeAndAwait null
                next
            }
            if (item == null) return@runBlocking
            if (item.status !in listOf(OrderStatus.NEW.code, OrderStatus.PARTIALLY_FILLED.code)) {
                tx.executeAndAwait {
                    db.sql("UPDATE order_admission SET cancel_sent_at=now() WHERE ouid=:ouid AND cancel_sent_at IS NULL")
                        .bind("ouid", item.ouid).fetch().rowsUpdated().awaitSingle()
                }
                return@repeat
            }
            val event = mapper.readValue(item.payload, ReservedOrder::class.java)
            withTimeout(15000) {
                suspendCancellableCoroutine<Unit> { cont ->
                    producer.send("orders_${event.pair}", event.pair.toString(),
                        OrderCancelRequestEvent(event.ouid, event.uuid, event.pair, item.orderId))
                        .addCallback({ cont.resume(Unit) }, { cont.resumeWithException(it) })
                }
            }
            tx.executeAndAwait {
                db.sql("UPDATE order_admission SET cancel_sent_at=now() WHERE ouid=:ouid AND cancel_sent_at IS NULL")
                    .bind("ouid", item.ouid).fetch().rowsUpdated().awaitSingle()
            }
        }
    }

    companion object {
        fun reserveReference(ouid: String) = UUID.nameUUIDFromBytes("opex:reserve:$ouid".toByteArray()).toString()
        fun releaseReference(ouid: String) = UUID.nameUUIDFromBytes("opex:release:$ouid".toByteArray()).toString()
    }
}
