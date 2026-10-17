package co.nilin.opex.accountant.app

import co.nilin.opex.accountant.app.service.OrderAdmissionService
import co.nilin.opex.accountant.ports.kafka.listener.inout.OrderSubmitRequestEvent
import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.matching.engine.core.eventh.events.CancelOrderEvent
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.inout.OrderRequestEvent
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent as EngineOrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.model.MatchConstraint
import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.engine.core.model.OrderType
import co.nilin.opex.matching.engine.core.model.Pair
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.coEvery
import io.mockk.mockk
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * These tests deliberately use the disposable `opex_test` database. They exercise the real SQL
 * admission and reservation queries; Kafka is mocked only at the publication boundary so a test
 * can observe an acknowledged send independently from its following database update.
 */
@EnabledIfEnvironmentVariable(
    named = "OPEX_TEST_POSTGRES",
    matches = "^r2dbc:postgresql://[^/]+/opex_test(?:\\?.*)?$",
)
class OrderAdmissionConditionalTest {
    private val testUrl = requireNotNull(System.getenv("OPEX_TEST_POSTGRES"))
    private val connectionFactory = ConnectionFactories.get(testUrl)
    private val db = DatabaseClient.create(connectionFactory)
    private val mapper = jacksonObjectMapper()
    private val createdOrders = linkedMapOf<String, OrderSubmitRequestEvent>()
    private var starvationFixtureOuidPrefix: String? = null
    private lateinit var harness: Harness

    private data class Harness(
        val service: OrderAdmissionService,
        val orderManager: OrderManager,
        val engineProducers: MutableList<MockProducer<String, OrderRequestEvent>>,
        val eventProducers: MutableList<MockProducer<String?, CoreEvent>>,
        val statusProducers: MutableList<MockProducer<String, String>>,
    ) {
        fun submittedOrders(): List<EngineOrderSubmitRequestEvent> = engineProducers
            .flatMap { it.history() }
            .mapNotNull { it.value() as? EngineOrderSubmitRequestEvent }

        fun emittedEvents(): List<CoreEvent> = eventProducers
            .flatMap { it.history() }
            .mapNotNull { it.value() }

        fun conditionalStatuses(): List<String> = statusProducers
            .flatMap { it.history() }
            .map { jacksonObjectMapper().readTree(it.value())["status"].asText() }
    }

    private data class AdmissionFlags(
        val conditional: Boolean,
        val activationRequested: Boolean,
        val cancelRequested: Boolean,
        val publicationStarted: Boolean,
        val published: Boolean,
    )

    @BeforeEach
    fun checkDedicatedDatabaseAndBuildHarness() = runBlocking {
        val databaseName = db.sql("SELECT current_database() AS name")
            .map { row, _ -> row.get("name", String::class.java)!! }
            .one()
            .awaitSingle()
        assertEquals("opex_test", databaseName, "refusing to touch a database other than opex_test")

        // The scheduler method is intentionally exercised end-to-end. Fail closed if a stale
        // fixture could make it dispatch an unrelated admission from the disposable database.
        val eligibleRows = db.sql(
            """SELECT count(*) AS n
               FROM order_admission a JOIN fi_actions f ON f.uuid=a.reserve_uuid
               WHERE a.published_at IS NULL
                 AND (NOT a.conditional OR a.activation_requested OR a.cancel_requested OR f.status='ERROR')
                 AND (f.status='PROCESSED' OR (f.status='ERROR' AND NOT a.reconciliation_required))""",
        ).map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }
            .one()
            .awaitSingle()
        assertEquals(0L, eligibleRows, "opex_test contains an unrelated publishable admission fixture")

        createdOrders.clear()
        starvationFixtureOuidPrefix = null
        harness = newHarness()
    }

    @AfterEach
    fun removeOnlyFixturesCreatedByThisTest() = runBlocking {
        starvationFixtureOuidPrefix?.let { prefix ->
            db.sql("DELETE FROM order_admission WHERE ouid LIKE :prefix")
                .bind("prefix", "$prefix%")
                .fetch().rowsUpdated().awaitSingle()
        }
        createdOrders.values.forEach { event ->
            val reserveUuid = OrderAdmissionService.reserveReference(event.ouid)
            db.sql("DELETE FROM order_admission_status_outbox WHERE ouid=:ouid")
                .bind("ouid", event.ouid)
                .fetch().rowsUpdated().awaitSingle()
            db.sql("DELETE FROM order_admission_commands WHERE ouid=:ouid")
                .bind("ouid", event.ouid)
                .fetch().rowsUpdated().awaitSingle()
            db.sql(
                """DELETE FROM fi_action_error
                   WHERE fa_id IN (SELECT id FROM fi_actions WHERE uuid=:reserveUuid)""",
            ).bind("reserveUuid", reserveUuid).fetch().rowsUpdated().awaitSingle()
            db.sql("DELETE FROM order_admission WHERE ouid=:ouid")
                .bind("ouid", event.ouid)
                .fetch().rowsUpdated().awaitSingle()
            db.sql("DELETE FROM fi_actions_archive WHERE uuid IN (:reserveUuid, :releaseUuid)")
                .bind("reserveUuid", reserveUuid)
                .bind("releaseUuid", OrderAdmissionService.releaseReference(event.ouid))
                .fetch().rowsUpdated().awaitSingle()
            db.sql("DELETE FROM fi_actions WHERE uuid=:reserveUuid")
                .bind("reserveUuid", reserveUuid)
                .fetch().rowsUpdated().awaitSingle()
            db.sql("DELETE FROM fi_actions WHERE uuid=:releaseUuid")
                .bind("releaseUuid", OrderAdmissionService.releaseReference(event.ouid))
                .fetch().rowsUpdated().awaitSingle()
        }
    }

    @Test
    fun `processed HOLD remains dormant and never reaches the matcher`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED")

        assertEquals(1L, reserveActionCount(event))
        val candidate = harness.service.pendingConditionalTriggers().single { it.ouid == event.ouid }
        val now = Instant.parse("2026-10-16T10:00:00Z")
        assertFalse(
            harness.service.activateIfTriggered(candidate, BigDecimal("104.99"), now, now, Duration.ofSeconds(10)),
        )
        assertFalse(admissionFlags(event.ouid).activationRequested)

        harness.service.publishReservedOrders()
        assertTrue(harness.submittedOrders().isEmpty(), "an untriggered HOLD must not reach matcher")

        harness.service.publishReservedOrders()

        assertTrue(harness.submittedOrders().isEmpty(), "HOLD must never be published to the matcher")
        val flags = admissionFlags(event.ouid)
        assertFalse(flags.publicationStarted)
        assertFalse(flags.published)
    }

    @Test
    fun `triggered matcher submission omits conditional admission metadata`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED")
        val candidate = harness.service.pendingConditionalTriggers().single { it.ouid == event.ouid }
        val now = Instant.parse("2026-10-16T10:00:00Z")

        assertTrue(harness.service.activateIfTriggered(candidate, BigDecimal("105"), now, now, Duration.ofSeconds(10)))
        harness.service.publishReservedOrders()

        val published = harness.submittedOrders().single { it.ouid == event.ouid }
        assertNull(published.conditionalAction)
        assertNull(published.stopPrice)
    }

    @Test
    fun `conditional acknowledgements wait for reservation and release confirmation`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PENDING")

        harness.service.publishConditionalStatusAcknowledgements()
        assertTrue(harness.conditionalStatuses().isEmpty())

        setReserveStatus(event, "PROCESSED")
        harness.service.publishConditionalStatusAcknowledgements()
        harness.service.publishConditionalStatusAcknowledgements()
        assertEquals(listOf("WAITING_TRIGGER"), harness.conditionalStatuses())

        val now = Instant.parse("2026-10-16T10:00:00Z")
        val candidate = harness.service.pendingConditionalTriggers().single { it.ouid == event.ouid }
        assertTrue(
            harness.service.activateIfTriggered(
                candidate,
                BigDecimal("105"),
                now,
                now,
                Duration.ofSeconds(10),
            ),
        )
        harness.service.publishConditionalStatusAcknowledgements()
        assertEquals(listOf("WAITING_TRIGGER", "TRIGGERED"), harness.conditionalStatuses())

    harness.service.cancelConditional(event)
        insertReleaseAction(event, "PENDING")
        harness.service.publishConditionalStatusAcknowledgements()
        assertEquals(listOf("WAITING_TRIGGER", "TRIGGERED"), harness.conditionalStatuses())

        setReleaseStatus(event, "PROCESSED")
        harness.service.publishConditionalStatusAcknowledgements()
        assertEquals(
            listOf("WAITING_TRIGGER", "TRIGGERED", "CANCELED"),
            harness.conditionalStatuses(),
        )
    }

    @Test
    fun `archived processed reserve and release acknowledge conditional cancellation`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED")
        archiveAction(OrderAdmissionService.reserveReference(event.ouid))

        harness.service.cancelConditional(event)
        insertReleaseAction(event, "PROCESSED")
        archiveAction(OrderAdmissionService.releaseReference(event.ouid))

        harness.service.publishConditionalStatusAcknowledgements()

        assertEquals(listOf("CANCELED"), harness.conditionalStatuses())
    }

    @Test
    fun `archived error and unknown reserve statuses remain unacknowledged after cancellation`() = runBlocking {
        val errorReserve = newHold()
        val unknownReserve = newHold()
        admitHold(errorReserve, reservationStatus = "ERROR")
        admitHold(unknownReserve, reservationStatus = "PENDING")
        db.sql("UPDATE fi_actions SET status=NULL WHERE uuid=:uuid")
            .bind("uuid", OrderAdmissionService.reserveReference(unknownReserve.ouid))
            .fetch().rowsUpdated().awaitSingle()
        archiveAction(OrderAdmissionService.reserveReference(errorReserve.ouid))
        archiveAction(OrderAdmissionService.reserveReference(unknownReserve.ouid))

        harness.service.cancelConditional(errorReserve)
        harness.service.cancelConditional(unknownReserve)
        harness.service.publishConditionalStatusAcknowledgements()

        assertTrue(harness.conditionalStatuses().isEmpty())
    }

    @Test
    fun `one reconciliation call acknowledges rows beyond the first page`() = runBlocking {
        val runId = UUID.randomUUID().toString()
        val prefix = "a-starvation-$runId-"
        starvationFixtureOuidPrefix = prefix
        val inserted = db.sql(
            """
            INSERT INTO order_admission(ouid,payload,reserve_uuid,conditional)
            SELECT :prefix || lpad(fixture.i::text, 3, '0'), '{}',
                   :prefix || 'reserve-' || fixture.i::text, TRUE
            FROM generate_series(1, 500) AS fixture(i)
            """.trimIndent(),
        )
            .bind("prefix", prefix)
            .fetch().rowsUpdated().awaitSingle()
        assertEquals(500, inserted)

        val laterHeldOrder = newHold(ouid = "z-starvation-$runId")
        admitHold(laterHeldOrder, reservationStatus = "PROCESSED")

        harness.service.publishConditionalStatusAcknowledgements()

        assertEquals(listOf("WAITING_TRIGGER"), harness.conditionalStatuses())
    }

    @Test
    fun `only processed reservations and fresh crossing trades activate BUY and SELL stops`() = runBlocking {
        val buy = newHold(direction = OrderDirection.BID, stopPrice = BigDecimal("105"))
        val sell = newHold(direction = OrderDirection.ASK, stopPrice = BigDecimal("95"))
        admitHold(buy, reservationStatus = "PENDING")
        admitHold(sell, reservationStatus = "PENDING")

        assertTrue(harness.service.pendingConditionalTriggers().isEmpty())
        val now = Instant.parse("2026-10-16T10:00:00Z")
        val freshTrade = now.minusSeconds(1)
        assertFalse(harness.service.activateIfTriggered(engineCandidate(buy), BigDecimal("106"), freshTrade, now, Duration.ofSeconds(10)))
        assertFalse(harness.service.activateIfTriggered(engineCandidate(sell), BigDecimal("94"), freshTrade, now, Duration.ofSeconds(10)))

        setReserveStatus(buy, "PROCESSED")
        setReserveStatus(sell, "PROCESSED")
        val candidates = harness.service.pendingConditionalTriggers().associateBy { it.ouid }
        assertTrue(candidates.keys.containsAll(listOf(buy.ouid, sell.ouid)))

        assertFalse(harness.service.activateIfTriggered(candidates.getValue(buy.ouid), BigDecimal("104.99"), freshTrade, now, Duration.ofSeconds(10)))
        assertFalse(harness.service.activateIfTriggered(candidates.getValue(sell.ouid), BigDecimal("95.01"), freshTrade, now, Duration.ofSeconds(10)))
        assertTrue(harness.service.activateIfTriggered(candidates.getValue(buy.ouid), BigDecimal("105"), freshTrade, now, Duration.ofSeconds(10)))
        assertTrue(harness.service.activateIfTriggered(candidates.getValue(sell.ouid), BigDecimal("95"), freshTrade, now, Duration.ofSeconds(10)))

        assertTrue(admissionFlags(buy.ouid).activationRequested)
        assertTrue(admissionFlags(sell.ouid).activationRequested)
    }

    @Test
    fun `stale and future native trades cannot activate a stop`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED")
        val candidate = harness.service.pendingConditionalTriggers().single { it.ouid == event.ouid }
        val now = Instant.parse("2026-10-16T10:00:00Z")
        val maxAge = Duration.ofSeconds(10)

        assertFalse(harness.service.activateIfTriggered(candidate, BigDecimal("110"), now.minusSeconds(11), now, maxAge))
        assertFalse(harness.service.activateIfTriggered(candidate, BigDecimal("110"), now.plusSeconds(1), now, maxAge))
        assertFalse(admissionFlags(event.ouid).activationRequested)
    }

    @Test
    fun `ACTIVATE before HOLD survives recovery and is applied when the HOLD arrives`() = runBlocking {
        val event = newHold()

        harness.service.activateConditional(event)
        harness.service.processPendingConditionalCommands()
        assertEquals("ACTIVATE", queuedCommand(event.ouid))

        harness = newHarness()
        harness.service.accept(event)
        insertReserveAction(event, "PROCESSED")

        assertNull(queuedCommandOrNull(event.ouid))
        assertTrue(admissionFlags(event.ouid).activationRequested)
        assertTrue(harness.service.pendingConditionalTriggers().none { it.ouid == event.ouid })
        harness.service.publishReservedOrders()
        assertEquals(listOf(event.ouid), harness.submittedOrders().map { it.ouid })
        assertTrue(admissionFlags(event.ouid).published)
    }

    @Test
    fun `pre-HOLD CANCEL wins and releases a dormant reservation once`() = runBlocking {
        val event = newHold()

        harness.service.activateConditional(event)
        harness.service.cancelConditional(event)
        harness.service.processPendingConditionalCommands()
        assertEquals("CANCEL", queuedCommand(event.ouid))

        harness.service.accept(event)
        insertReserveAction(event, "PROCESSED")
        val flags = admissionFlags(event.ouid)
        assertTrue(flags.cancelRequested)
        assertFalse(flags.activationRequested)

        harness.service.publishReservedOrders()
        harness.service.publishReservedOrders()

        assertTrue(harness.submittedOrders().isEmpty(), "a cancelled dormant HOLD must not reach the matcher")
        assertEquals(1, harness.emittedEvents().count { it is CancelOrderEvent })
        assertTrue(admissionFlags(event.ouid).published)
    }

    @Test
    fun `cancellation after publication starts never releases the reserve early`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED", publicationStarted = true)

        harness.service.cancelConditional(event)
        assertTrue(admissionFlags(event.ouid).cancelRequested)
        assertTrue(harness.emittedEvents().none { it is CancelOrderEvent })

        harness.service.publishReservedOrders()

        assertTrue(harness.emittedEvents().none { it is CancelOrderEvent }, "publication may already have reached the engine")
        assertEquals(listOf(event.ouid), harness.submittedOrders().map { it.ouid })
        assertTrue(admissionFlags(event.ouid).published)
    }

    @Test
    fun `acknowledged send with failed published commit retries identical engine identity`() = runBlocking {
        val event = newHold()
        admitHold(event, reservationStatus = "PROCESSED")
        harness.service.activateConditional(event)
        val constraint = "deny_publish_${event.ouid.replace("-", "")}"

        // The producer auto-acks. This constraint fails only the following SQL commit, creating
        // the ambiguous-send window the persistent OUID deduplication contract must tolerate.
        db.sql(
            """ALTER TABLE order_admission ADD CONSTRAINT $constraint
               CHECK (ouid <> '${event.ouid}' OR published_at IS NULL)""",
        ).fetch().rowsUpdated().awaitSingle()
        try {
            assertThrows(Exception::class.java) { harness.service.publishReservedOrders() }
            val firstAttempt = harness.submittedOrders().single()
            assertEquals(event.ouid, firstAttempt.ouid)
            assertEquals(event.uuid, firstAttempt.uuid)
            assertTrue(admissionFlags(event.ouid).publicationStarted)
            assertFalse(admissionFlags(event.ouid).published)

            harness.service.cancelConditional(event)
            assertTrue(admissionFlags(event.ouid).cancelRequested)
            assertTrue(harness.emittedEvents().none { it is CancelOrderEvent }, "ambiguous publication must keep the reserve")

            db.sql("ALTER TABLE order_admission DROP CONSTRAINT $constraint")
                .fetch().rowsUpdated().awaitSingle()
            db.sql("UPDATE order_admission SET dispatch_attempted_at=now()-interval '1 minute' WHERE ouid=:ouid")
                .bind("ouid", event.ouid).fetch().rowsUpdated().awaitSingle()

            harness.service.publishReservedOrders()

            val attempts = harness.submittedOrders().filter { it.ouid == event.ouid }
            assertEquals(2, attempts.size)
            // The matching engine deduplicates by persistent OUID; retry must retain both identity fields.
            assertEquals(setOf(event.ouid), attempts.map { it.ouid }.toSet())
            assertEquals(setOf(event.uuid), attempts.map { it.uuid }.toSet())
            assertTrue(admissionFlags(event.ouid).published)
            assertTrue(harness.emittedEvents().none { it is CancelOrderEvent })
        } finally {
            db.sql("ALTER TABLE order_admission DROP CONSTRAINT IF EXISTS $constraint")
                .fetch().rowsUpdated().awaitSingle()
        }
    }

    private fun newHarness(): Harness {
        val manager = mockk<OrderManager>(relaxed = true)
        coEvery { manager.handleRequestOrder(any()) } returns emptyList()
        val engineProducers = mutableListOf<MockProducer<String, OrderRequestEvent>>()
        val eventProducers = mutableListOf<MockProducer<String?, CoreEvent>>()
        val orderFactory = object : ProducerFactory<String, OrderRequestEvent> {
            override fun createProducer(): Producer<String, OrderRequestEvent> =
                MockProducer<String, OrderRequestEvent>(true, StringSerializer(), JsonSerializer<OrderRequestEvent>())
                    .also(engineProducers::add)
        }
        val eventFactory = object : ProducerFactory<String?, CoreEvent> {
            override fun createProducer(): Producer<String?, CoreEvent> =
                MockProducer<String?, CoreEvent>(true, StringSerializer(), JsonSerializer<CoreEvent>())
                    .also(eventProducers::add)
        }
        val eventTemplate = KafkaTemplate(eventFactory)
        val orderTemplate = KafkaTemplate(orderFactory)
        val statusProducers = mutableListOf<MockProducer<String, String>>()
        val statusFactory = object : ProducerFactory<String, String> {
            override fun createProducer(): Producer<String, String> =
                MockProducer(true, StringSerializer(), StringSerializer()).also(statusProducers::add)
        }
        val statusTemplate = KafkaTemplate(statusFactory)
        return Harness(
            OrderAdmissionService(
                db,
                R2dbcTransactionManager(connectionFactory),
                manager,
                mapper,
                eventTemplate,
                orderTemplate,
                statusTemplate,
            ),
            manager,
            engineProducers,
            eventProducers,
            statusProducers,
        )
    }

    private suspend fun admitHold(
        event: OrderSubmitRequestEvent,
        reservationStatus: String,
        publicationStarted: Boolean = false,
    ) {
        createdOrders[event.ouid] = event
        harness.service.accept(event)
        insertReserveAction(event, reservationStatus)
        if (publicationStarted) {
            db.sql("UPDATE order_admission SET publication_started_at=now() WHERE ouid=:ouid")
                .bind("ouid", event.ouid).fetch().rowsUpdated().awaitSingle()
        }
    }

    private suspend fun insertReserveAction(event: OrderSubmitRequestEvent, status: String) {
        createdOrders[event.ouid] = event
        db.sql(
            """INSERT INTO fi_actions
               (uuid,event_type,pointer,symbol,amount,sender,sender_wallet_type,receiver,receiver_wallet_type,create_date,status)
               VALUES (:uuid,'SubmitOrderEvent',:pointer,'USDT',100,'owner','MAIN','exchange','EXCHANGE',now(),:status)""",
        )
            .bind("uuid", OrderAdmissionService.reserveReference(event.ouid))
            .bind("pointer", event.ouid)
            .bind("status", status)
            .fetch().rowsUpdated().awaitSingle()
    }


    private suspend fun insertReleaseAction(event: OrderSubmitRequestEvent, status: String) {
        db.sql(
            """INSERT INTO fi_actions
                (uuid,event_type,pointer,symbol,amount,sender,sender_wallet_type,receiver,receiver_wallet_type,create_date,status)
                VALUES (:uuid,'CancelOrderEvent',:pointer,'USDT',100,'owner','MAIN','owner','EXCHANGE',now(),:status)""",
        )
            .bind("uuid", OrderAdmissionService.releaseReference(event.ouid))
            .bind("pointer", event.ouid)
            .bind("status", status)
            .fetch()
            .rowsUpdated()
            .awaitSingle()
    }

    private suspend fun setReleaseStatus(event: OrderSubmitRequestEvent, status: String) {
        db.sql("UPDATE fi_actions SET status=:status WHERE uuid=:uuid")
            .bind("status", status)
            .bind("uuid", OrderAdmissionService.releaseReference(event.ouid))
            .fetch()
            .rowsUpdated()
            .awaitSingle()
    }

    private fun newHold(
        ouid: String = UUID.randomUUID().toString(),
        direction: OrderDirection = OrderDirection.BID,
        stopPrice: BigDecimal = BigDecimal("105"),
    ): OrderSubmitRequestEvent {
        val event = OrderSubmitRequestEvent(
            ouid = ouid,
            uuid = UUID.randomUUID().toString(),
            pair = Pair("BTC", "USDT"),
            price = 100L,
            quantity = 10L,
            direction = direction,
            matchConstraint = MatchConstraint.GTC,
            orderType = OrderType.LIMIT_ORDER,
            userLevel = "MAIN",
            clientOrderId = "conditional-${UUID.randomUUID()}",
            requiresReservation = true,
            conditionalAction = "HOLD",
            stopPrice = stopPrice,
        )
        createdOrders[event.ouid] = event
        return event
    }

    private fun engineCandidate(event: OrderSubmitRequestEvent) = EngineOrderSubmitRequestEvent(
        ouid = event.ouid,
        uuid = event.uuid,
        pair = event.pair,
        price = event.price,
        quantity = event.quantity,
        direction = event.direction,
        matchConstraint = event.matchConstraint,
        orderType = event.orderType,
        userLevel = event.userLevel,
        orderId = event.orderId,
        clientOrderId = event.clientOrderId,
        postOnly = event.postOnly,
        requiresReservation = event.requiresReservation,
        reservationConfirmed = event.reservationConfirmed,
        conditionalAction = event.conditionalAction,
        stopPrice = event.stopPrice,
    )

    private suspend fun setReserveStatus(event: OrderSubmitRequestEvent, status: String) {
        db.sql("UPDATE fi_actions SET status=:status WHERE uuid=:uuid")
            .bind("status", status)
            .bind("uuid", OrderAdmissionService.reserveReference(event.ouid))
            .fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun archiveAction(uuid: String) {
        val inserted = db.sql(
            """INSERT INTO fi_actions_archive
                (id,uuid,parent_id,event_type,pointer,symbol,amount,sender,sender_wallet_type,
                 receiver,receiver_wallet_type,agent,ip,create_date,status,category_name)
                SELECT id,uuid,parent_id,event_type,pointer,symbol,amount,sender,sender_wallet_type,
                       receiver,receiver_wallet_type,agent,ip,create_date,status,category_name
                FROM fi_actions WHERE uuid=:uuid""",
        ).bind("uuid", uuid).fetch().rowsUpdated().awaitSingle()
        assertEquals(1L, inserted.toLong(), "expected one active financial action to archive")
        db.sql("DELETE FROM fi_actions WHERE uuid=:uuid")
            .bind("uuid", uuid)
            .fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun reserveActionCount(event: OrderSubmitRequestEvent): Long = db.sql(
        "SELECT count(*) AS n FROM fi_actions WHERE uuid=:uuid AND status='PROCESSED'",
    )
        .bind("uuid", OrderAdmissionService.reserveReference(event.ouid))
        .map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }
        .one().awaitSingle()

    private suspend fun admissionFlags(ouid: String): AdmissionFlags = db.sql(
        """SELECT conditional,activation_requested,cancel_requested,
                  publication_started_at IS NOT NULL AS publication_started,
                  published_at IS NOT NULL AS published
           FROM order_admission WHERE ouid=:ouid""",
    )
        .bind("ouid", ouid)
        .map { row, _ ->
            AdmissionFlags(
                row.get("conditional", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("activation_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("cancel_requested", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("publication_started", java.lang.Boolean::class.java)!!.booleanValue(),
                row.get("published", java.lang.Boolean::class.java)!!.booleanValue(),
            )
        }
        .one().awaitSingle()

    private suspend fun queuedCommand(ouid: String): String = requireNotNull(queuedCommandOrNull(ouid))

    private suspend fun queuedCommandOrNull(ouid: String): String? = db.sql(
        "SELECT action FROM order_admission_commands WHERE ouid=:ouid",
    )
        .bind("ouid", ouid)
        .map { row, _ -> row.get("action", String::class.java)!! }
        .one().awaitSingleOrNull()
}
