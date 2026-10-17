package co.nilin.opex.matching.gateway.ports.kafka.submitter.service

import co.nilin.opex.matching.engine.core.model.MatchConstraint
import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.engine.core.model.OrderType
import co.nilin.opex.matching.engine.core.model.Pair
import co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderRequestEvent
import co.nilin.opex.matching.gateway.ports.kafka.submitter.inout.OrderSubmitRequestEvent
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.core.ProducerFactory
import org.springframework.kafka.support.serializer.JsonSerializer

class OrderRequestEventSubmitterTest {
    @Test
    fun `conditional commands use the accountant topic while regular orders use symbol topics`() = runBlocking {
        val producers = mutableListOf<MockProducer<String, OrderRequestEvent>>()
        val factory = object : ProducerFactory<String, OrderRequestEvent> {
            override fun createProducer(): Producer<String, OrderRequestEvent> {
                val producer = MockProducer<String, OrderRequestEvent>(
                    true,
                    StringSerializer(),
                    JsonSerializer<OrderRequestEvent>(),
                )
                producers += producer
                return producer
            }
        }
        val submitter = OrderRequestEventSubmitter(KafkaTemplate(factory))
        val hold = event("HOLD")
        val activate = event("ACTIVATE")
        val cancel = event("CANCEL")
        val regular = event(null)

        submitter.submit(hold)
        submitter.submit(activate)
        submitter.submit(cancel)
        submitter.submit(regular)

        assertEquals(
            listOf(
                "order_admission_commands",
                "order_admission_commands",
                "order_admission_commands",
                "orders_BTC_USDT",
            ),
            producers.flatMap { it.history() }.map { it.topic() },
        )
    }

    private fun event(action: String?) = OrderSubmitRequestEvent(
        uuid = "owner",
        pair = Pair("BTC", "USDT"),
        price = 100,
        quantity = 10,
        direction = OrderDirection.BID,
        matchConstraint = MatchConstraint.GTC,
        orderType = OrderType.LIMIT_ORDER,
        userLevel = "*",
        conditionalAction = action,
    )
}
