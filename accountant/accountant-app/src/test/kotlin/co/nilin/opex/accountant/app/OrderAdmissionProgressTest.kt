package co.nilin.opex.accountant.app

import co.nilin.opex.accountant.app.service.OrderAdmissionService
import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.model.*
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.mockk
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.r2dbc.connection.R2dbcTransactionManager
import org.springframework.r2dbc.core.DatabaseClient
import java.util.UUID

@EnabledIfEnvironmentVariable(named="OPEX_TEST_POSTGRES", matches=".+")
class OrderAdmissionProgressTest {
    private val factory = ConnectionFactories.get(System.getenv("OPEX_TEST_POSTGRES") ?: "r2dbc:postgresql://opex_test:opex_test@localhost:18033/opex_test")
    private val db = DatabaseClient.create(factory)
    private val mapper = jacksonObjectMapper()
    private val reserved = mutableListOf<MockProducer<String, OrderSubmitRequestEvent>>()
    private val rejected = mutableListOf<MockProducer<String?, CoreEvent>>()
    private val orderFactory = object : ProducerFactory<String, OrderSubmitRequestEvent> {
        override fun createProducer(): Producer<String, OrderSubmitRequestEvent> =
            MockProducer(true, StringSerializer(), JsonSerializer<OrderSubmitRequestEvent>()).also { reserved.add(it) }
    }
    private val eventFactory = object : ProducerFactory<String?, CoreEvent> {
        override fun createProducer(): Producer<String?, CoreEvent> =
            MockProducer(true, StringSerializer(), JsonSerializer<CoreEvent>()).also { rejected.add(it) }
    }
    private fun service() = OrderAdmissionService(db, R2dbcTransactionManager(factory), mockk<OrderManager>(),
        mapper, KafkaTemplate(eventFactory), KafkaTemplate(orderFactory))

    @BeforeEach
    fun clean(): Unit = runBlocking {
        db.sql("DELETE FROM order_admission").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM fi_action_error").fetch().rowsUpdated().awaitSingle()
        db.sql("DELETE FROM fi_actions").fetch().rowsUpdated().awaitSingle()
    }

    private suspend fun seed(ouid: String, pair: Pair, status: String, error: String?) {
        val reference = OrderAdmissionService.reserveReference(ouid)
        val id = db.sql("""INSERT INTO fi_actions(uuid,event_type,pointer,symbol,amount,sender,sender_wallet_type,
            receiver,receiver_wallet_type,create_date,status,category_name)
            VALUES(:ref,'SubmitOrderEvent',:ouid,'USDT',100,'owner','MAIN','owner','EXCHANGE',now(),:status,'ORDER_CREATE') RETURNING id""")
            .bind("ref", reference).bind("ouid", ouid).bind("status", status)
            .map { row, _ -> row.get("id", java.lang.Integer::class.java)!!.toInt() }.one().awaitSingle()
        if (error != null) db.sql("INSERT INTO fi_action_error(fa_id,error,message,body,date) VALUES(:id,'ambiguous','failure',:body,now())")
            .bind("id", id).bind("body", error).fetch().rowsUpdated().awaitSingle()
        val event = OrderSubmitRequestEvent(ouid, "owner", pair, 100, 10, OrderDirection.BID,
            MatchConstraint.GTC, OrderType.LIMIT_ORDER, "*", requiresReservation=true, reservationConfirmed=true)
        db.sql("INSERT INTO order_admission(ouid,payload,reserve_uuid) VALUES(:ouid,:payload,:ref)")
            .bind("ouid", ouid).bind("payload", mapper.writeValueAsString(event)).bind("ref", reference)
            .fetch().rowsUpdated().awaitSingle()
    }

    @Test
    fun ambiguousOldestReservationDoesNotStarveSuccessfulOrDefiniteRefusal() = runBlocking {
        val unknown = UUID.randomUUID().toString()
        val success = UUID.randomUUID().toString()
        val refusal = UUID.randomUUID().toString()
        seed(unknown, Pair("BTC","USDT"), "ERROR", "{\"code\":9999,\"error\":\"AMBIGUOUS_TRANSFER\"}")
        seed(success, Pair("USDT","IRT"), "PROCESSED", null)
        seed(refusal, Pair("BTC","IRT"), "ERROR", "{\"code\":6018,\"error\":\"NotEnoughBalance\"}")
        service().publishReservedOrders()
        assertEquals(listOf(success), reserved.flatMap { it.history() }.map { it.value().ouid })
        assertEquals(1, rejected.flatMap { it.history() }.size)
        val pending = db.sql("SELECT count(*) AS n FROM order_admission WHERE ouid=:id AND published_at IS NULL")
            .bind("id", unknown).map { row, _ -> row.get("n", java.lang.Long::class.java)!!.toLong() }.one().awaitSingle()
        assertEquals(1L, pending)
        // An externally reconciled successful transfer becomes eligible even after being deferred.
        db.sql("UPDATE fi_actions SET status='PROCESSED' WHERE pointer=:id").bind("id", unknown).fetch().rowsUpdated().awaitSingle()
        service().publishReservedOrders()
        assertEquals(setOf(success, unknown), reserved.flatMap { it.history() }.map { it.value().ouid }.toSet())
    }
}
