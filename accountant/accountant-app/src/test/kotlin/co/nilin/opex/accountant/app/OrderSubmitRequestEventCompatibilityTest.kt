package co.nilin.opex.accountant.app

import co.nilin.opex.accountant.app.config.AppConfig
import co.nilin.opex.accountant.ports.kafka.listener.config.AccountantKafkaConfig
import co.nilin.opex.accountant.ports.kafka.listener.inout.OrderSubmitRequestEvent as AccountantOrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.inout.OrderRequestEvent as EngineOrderRequestEvent
import co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent as EngineOrderSubmitRequestEvent
import co.nilin.opex.matching.engine.core.model.MatchConstraint
import co.nilin.opex.matching.engine.core.model.OrderDirection
import co.nilin.opex.matching.engine.core.model.OrderType
import co.nilin.opex.matching.engine.core.model.Pair
import co.nilin.opex.matching.engine.ports.kafka.listener.config.OrderKafkaConfig
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.kafka.support.serializer.JsonSerializer
import org.springframework.test.util.ReflectionTestUtils
import java.math.BigDecimal
import java.nio.charset.StandardCharsets.UTF_8

class OrderSubmitRequestEventCompatibilityTest {
    @Test
    fun `legacy engine JSON deserializes through accountant Kafka mapping`() {
        val mapper = jacksonObjectMapper()
        val legacyEngineJson = mapper.valueToTree<ObjectNode>(engineEvent()).apply {
            remove("conditionalAction")
            remove("stopPrice")
        }.toString().toByteArray(UTF_8)

        val accountantDeserializer = JsonDeserializer<Any>().apply {
            configure(accountantConsumerConfigs(), false)
        }
        val restored = accountantDeserializer.deserialize(TOPIC, typeHeaders(), legacyEngineJson)

        val accountantEvent = assertInstanceOf(AccountantOrderSubmitRequestEvent::class.java, restored)
        assertNull(accountantEvent.conditionalAction)
        assertNull(accountantEvent.stopPrice)
    }

    @Test
    fun `accountant LIMIT payload remains readable by a legacy matching engine consumer`() {
        val event = engineEvent()
        val template = AppConfig("USDT", "+03:30").orderAdmissionKafkaTemplate("localhost:9092")
        val producerFactory = template.producerFactory as DefaultKafkaProducerFactory<*, *>
        val producerHeaders = RecordHeaders()
        val payload = JsonSerializer<EngineOrderRequestEvent>().apply {
            configure(producerFactory.configurationProperties, false)
        }.serialize(TOPIC, producerHeaders, event)

        assertEquals(TYPE_ALIAS, producerHeaders.lastHeader(TYPE_ID_HEADER)?.value()?.toString(UTF_8))
        val payloadJson = jacksonObjectMapper().readTree(payload)
        assertEquals(true, payloadJson.has("conditionalAction"))
        assertEquals(true, payloadJson.has("stopPrice"))

        val legacyConsumerConfigs = matchingEngineConsumerConfigs().toMutableMap()
        val engineTypeMappings = legacyConsumerConfigs[JsonDeserializer.TYPE_MAPPINGS] as? String
            ?: error("matching-engine consumer config has no JSON type mappings")
        legacyConsumerConfigs[JsonDeserializer.TYPE_MAPPINGS] = engineTypeMappings
            .split(',')
            .joinToString(",") { mapping ->
                if (mapping.substringBefore(':') == TYPE_ALIAS) {
                    "$TYPE_ALIAS:${LegacyOrderSubmitRequestEvent::class.java.name}"
                } else {
                    mapping
                }
            }
        val legacyEngineDeserializer = JsonDeserializer<Any>().apply {
            configure(legacyConsumerConfigs, false)
        }

        val restored = legacyEngineDeserializer.deserialize(TOPIC, producerHeaders, payload)

        val legacyEvent = assertInstanceOf(LegacyOrderSubmitRequestEvent::class.java, restored)
        assertEquals(event.ouid, legacyEvent.ouid)
        assertEquals(event.price, legacyEvent.price)
        assertEquals(event.quantity, legacyEvent.quantity)
        assertEquals(event.orderType, legacyEvent.orderType)
        assertEquals(event.requiresReservation, legacyEvent.requiresReservation)
    }

    private fun engineEvent() = EngineOrderSubmitRequestEvent(
        ouid = "limit-ouid",
        uuid = "limit-uuid",
        pair = Pair("BTC", "USDT"),
        price = 100L,
        quantity = 10L,
        direction = OrderDirection.BID,
        matchConstraint = MatchConstraint.GTC,
        orderType = OrderType.LIMIT_ORDER,
        userLevel = "MAIN",
        clientOrderId = "limit-client-order",
        requiresReservation = true,
        conditionalAction = "HOLD",
        stopPrice = BigDecimal("105"),
    )

    // The order listener container uses this `accountantConsumerFactory` config.
    private fun accountantConsumerConfigs(): Map<String, Any?> = AccountantKafkaConfig().apply {
        ReflectionTestUtils.setField(this, "bootstrapServers", "localhost:9092")
        ReflectionTestUtils.setField(this, "groupId", "compatibility-test")
    }.consumerConfigs()

    private fun matchingEngineConsumerConfigs(): Map<String, Any?> = OrderKafkaConfig().apply {
        ReflectionTestUtils.setField(this, "bootstrapServers", "localhost:9092")
        ReflectionTestUtils.setField(this, "groupId", "compatibility-test")
    }.consumerConfigs()

    private fun typeHeaders() = RecordHeaders().add(
        RecordHeader(TYPE_ID_HEADER, TYPE_ALIAS.toByteArray(UTF_8)),
    )

    private companion object {
        const val TOPIC = "order.submit"
        const val TYPE_ALIAS = "order_request_submit"
        const val TYPE_ID_HEADER = "__TypeId__"
    }
}

/** Mirrors the pre-conditional-order matching-engine DTO and intentionally omits its new fields. */
class LegacyOrderSubmitRequestEvent {
    var ouid: String? = null
    var uuid: String? = null
    var pair: Pair? = null
    var price: Long? = null
    var quantity: Long? = null
    var direction: OrderDirection? = null
    var matchConstraint: MatchConstraint? = null
    var orderType: OrderType? = null
    var userLevel: String? = null
    var orderId: Long? = null
    var clientOrderId: String? = null
    var postOnly: Boolean? = null
    var requiresReservation: Boolean? = null
    var reservationConfirmed: Boolean? = null
}
