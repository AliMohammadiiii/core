package co.nilin.opex.matching.gateway.app.config

import co.nilin.opex.matching.engine.core.eventh.events.*
import co.nilin.opex.matching.engine.core.inout.RequestedOperation
import co.nilin.opex.matching.gateway.ports.postgres.service.OrderIntentStore
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.*
import org.springframework.kafka.support.serializer.JsonDeserializer
import org.springframework.transaction.ReactiveTransactionManager
import org.springframework.transaction.reactive.TransactionalOperator
import org.springframework.util.backoff.FixedBackOff
import java.util.regex.Pattern

@Configuration
class OrderIntentConfig {
    @Bean
    fun orderIntentTransactions(manager: ReactiveTransactionManager) = TransactionalOperator.create(manager)

    @Bean
    fun intentOutcomeContainer(@Value("\${spring.kafka.bootstrap-servers}") bootstrap: String,
        store: OrderIntentStore, mapper: ObjectMapper): KafkaMessageListenerContainer<String, CoreEvent> {
        val factory = DefaultKafkaConsumerFactory<String, CoreEvent>(mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG to "opex-gateway-intents-v2",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to JsonDeserializer::class.java,
            JsonDeserializer.TRUSTED_PACKAGES to "co.nilin.opex.*"))
        val props = ContainerProperties(Pattern.compile("events_.*"))
        props.ackMode = ContainerProperties.AckMode.RECORD
        props.messageListener = MessageListener<String, CoreEvent> { record -> runBlocking {
            val event = record.value()
            suspend fun update(owner: String, ouid: String, status: String, id: Long?, remaining: Long?,
                               reason: String? = null, execution: TradeEvent? = null) {
                val intent = store.find(owner, null, ouid) ?: return
                val payload = linkedMapOf<String, Any?>("eventType" to event.javaClass.simpleName,
                    "clientOrderId" to intent.clientOrderId, "ouid" to ouid, "symbol" to intent.symbol,
                    "orderId" to id, "status" to status, "reason" to reason)
                if (execution != null) {
                    val nativePrice = mapper.readTree(intent.payload)["price"].decimalValue()
                    val quoteFraction = intent.price.divide(nativePrice)
                    payload["executionId"] = "${event.pair}:${execution.tradeId}"
                    payload["quantity"] = execution.matchedQuantity.toBigDecimal().multiply(intent.leftFraction).toPlainString()
                    payload["price"] = execution.makerPrice.toBigDecimal().multiply(quoteFraction).toPlainString()
                }
                val key = if (event is TradeEvent) "${event.pair}:trade:${event.tradeId}:$ouid"
                    else "${event.pair}:${event.javaClass.simpleName}:$ouid:$reason"
                store.outcome(owner, ouid, key, mapper.writeValueAsString(payload), status, id, remaining, reason)
            }
            when (event) {
                is CreateOrderEvent -> update(event.uuid, event.ouid, "OPEN", event.orderId, event.remainedQuantity)
                is CancelOrderEvent -> {
                    val intent = store.find(event.uuid, null, event.ouid)
                    if (intent?.type != "STOP_LIMIT") {
                        update(event.uuid, event.ouid, "CANCELED", event.orderId, event.remainedQuantity)
                    }
                }
                is RejectOrderEvent -> if (event.requestedOperation == RequestedOperation.PLACE_ORDER)
                    update(event.uuid, event.ouid, "REJECTED", event.orderId, null, event.reason?.name)
                is TradeEvent -> {
                    update(event.takerUuid, event.takerOuid, if (event.takerRemainedQuantity == 0L) "FILLED" else "PARTIALLY_FILLED",
                        event.takerOrderId, event.takerRemainedQuantity, execution = event)
                    update(event.makerUuid, event.makerOuid, if (event.makerRemainedQuantity == 0L) "FILLED" else "PARTIALLY_FILLED",
                        event.makerOrderId, event.makerRemainedQuantity, execution = event)
                }
            }
        } }
        return KafkaMessageListenerContainer(factory, props).apply {
            commonErrorHandler = DefaultErrorHandler(FixedBackOff(1000, 20))
        }
    }

    @Bean
    fun conditionalAdmissionStatusContainer(
        @Value("\${spring.kafka.bootstrap-servers}") bootstrap: String,
        store: OrderIntentStore,
        mapper: ObjectMapper,
    ): KafkaMessageListenerContainer<String, String> {
        val factory = DefaultKafkaConsumerFactory<String, String>(mapOf(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ConsumerConfig.GROUP_ID_CONFIG to "opex-gateway-admission-status-v1",
            ConsumerConfig.AUTO_OFFSET_RESET_CONFIG to "earliest",
            ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG to false,
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.java,
        ))
        val props = ContainerProperties("order_admission_status")
        props.ackMode = ContainerProperties.AckMode.RECORD
        props.messageListener = MessageListener<String, String> { record ->
            runBlocking {
                val statusEvent = runCatching { mapper.readTree(record.value()) }.getOrNull() ?: return@runBlocking
                val owner = statusEvent.get("uuid")?.takeIf { it.isTextual }?.asText() ?: return@runBlocking
                val ouid = statusEvent.get("ouid")?.takeIf { it.isTextual }?.asText() ?: return@runBlocking
                val status = statusEvent.get("status")?.takeIf { it.isTextual }?.asText() ?: return@runBlocking
                if (status !in setOf("WAITING_TRIGGER", "TRIGGERED", "CANCELED", "REJECTED")) return@runBlocking

                // Derive the idempotency key from immutable identity and state, rather than
                // trusting a delivery-specific Kafka offset or timestamp.
                val idempotencyKey = "conditional-admission:$ouid:$status"
                store.conditionalStatus(owner, ouid, idempotencyKey, record.value(), status)
            }
        }
        return KafkaMessageListenerContainer(factory, props).apply {
            commonErrorHandler = DefaultErrorHandler(FixedBackOff(1000, 20))
        }
    }
}
