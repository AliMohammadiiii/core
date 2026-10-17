package co.nilin.opex.accountant.app.config

import co.nilin.opex.accountant.app.listener.*
import co.nilin.opex.accountant.app.service.*
import co.nilin.opex.accountant.core.api.FeeCalculator
import co.nilin.opex.accountant.core.api.FinancialActionJobManager
import co.nilin.opex.accountant.core.api.OrderManager
import co.nilin.opex.accountant.core.api.TradeManager
import co.nilin.opex.accountant.core.service.FinancialActionJobManagerImpl
import co.nilin.opex.accountant.core.service.OrderManagerImpl
import co.nilin.opex.accountant.core.service.TradeManagerImpl
import co.nilin.opex.accountant.core.spi.*
import co.nilin.opex.accountant.ports.kafka.listener.consumer.*
import co.nilin.opex.accountant.ports.kafka.listener.spi.FAResponseListener
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableScheduling
import co.nilin.opex.matching.engine.core.inout.OrderRequestEvent
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.StringSerializer
import org.springframework.kafka.core.DefaultKafkaProducerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.serializer.JsonSerializer

@Configuration
@EnableScheduling
class AppConfig(
    @Value("\${app.trade-volume-calculation-currency}")
    private val tradeVolumeCalculationCurrency: String,
    @Value("\${app.zone-offset}")
    private val zoneOffsetString: String,
) {

    @Bean("orderAdmissionKafkaTemplate")
    fun orderAdmissionKafkaTemplate(@Value("\${spring.kafka.bootstrap-servers}") bootstrap: String): KafkaTemplate<String, OrderRequestEvent> =
        KafkaTemplate(DefaultKafkaProducerFactory(mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to JsonSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
            JsonSerializer.TYPE_MAPPINGS to "order_request_submit:co.nilin.opex.matching.engine.core.inout.OrderSubmitRequestEvent,order_request_cancel:co.nilin.opex.matching.engine.core.inout.OrderCancelRequestEvent")))

    @Bean("orderAdmissionStatusKafkaTemplate")
    fun orderAdmissionStatusKafkaTemplate(@Value("\${spring.kafka.bootstrap-servers}") bootstrap: String): KafkaTemplate<String, String> =
        KafkaTemplate(DefaultKafkaProducerFactory(mapOf(
            ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrap,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG to StringSerializer::class.java,
            ProducerConfig.ACKS_CONFIG to "all",
        )))

    @Bean
    fun getFinancialActionJobManager(
        financialActionLoader: FinancialActionLoader,
        financialActionPersister: FinancialActionPersister,
        walletProxy: WalletProxy
    ): FinancialActionJobManager {
        return FinancialActionJobManagerImpl(
            financialActionLoader,
            financialActionPersister,
            walletProxy
        )
    }

    @Bean
    fun orderManager(
        pairConfigLoader: PairConfigLoader,
        financialActionPersister: FinancialActionPersister,
        financeActionLoader: FinancialActionLoader,
        orderPersister: OrderPersister,
        tempEventPersister: TempEventPersister,
        tempEventRepublisher: TempEventRepublisher,
        richOrderPublisher: RichOrderPublisher,
        financialActionPublisher: FinancialActionPublisher,
        feeCalculator: FeeCalculator
    ): OrderManager {
        return OrderManagerImpl(
            pairConfigLoader,
            financialActionPersister,
            financeActionLoader,
            orderPersister,
            tempEventPersister,
            richOrderPublisher,
            financialActionPublisher,
            feeCalculator
        )
    }

    @Bean
    fun tradeManager(
        financeActionPersister: FinancialActionPersister,
        financeActionLoader: FinancialActionLoader,
        orderPersister: OrderPersister,
        tempEventPersister: TempEventPersister,
        richTradePublisher: RichTradePublisher,
        richOrderPublisher: RichOrderPublisher,
        feeCalculator: FeeCalculator,
        financialActionPublisher: FinancialActionPublisher,
        currencyRatePersister: CurrencyRatePersister,
        userVolumePersister: UserTradeVolumePersister
    ): TradeManager {
        return TradeManagerImpl(
            financeActionPersister,
            financeActionLoader,
            orderPersister,
            tempEventPersister,
            richTradePublisher,
            richOrderPublisher,
            feeCalculator,
            financialActionPublisher,
            currencyRatePersister,
            userVolumePersister,
            tradeVolumeCalculationCurrency,
            zoneOffsetString
        )
    }

    @Bean
    fun orderListener(orderManager: OrderManager, admission: OrderAdmissionService): OrderListener {
        return OrderListener(orderManager, admission)
    }

    @Bean
    fun accountantTradeListener(tradeManager: TradeManager): AccountantTradeListener {
        return AccountantTradeListener(tradeManager)
    }

    @Bean
    fun accountantEventListener(processor: AccountantEventProcessor): AccountantEventListener {
        return AccountantEventListener(processor)
    }

    @Bean
    fun accountantTempEventListener(
        processor: AccountantEventProcessor
    ): AccountantTempEventListener {
        return AccountantTempEventListener(processor)
    }

    @Bean
    fun faResponseListener(financialActionPersister: FinancialActionPersister): FAResponseListener {
        return AccountantFAResponseEventListener(financialActionPersister)
    }

    @Autowired
    fun configureOrderListener(orderKafkaListener: OrderKafkaListener, orderListener: OrderListener) {
        orderKafkaListener.addListener(orderListener)
    }

    @Autowired
    fun configureTradeListener(
        tradeKafkaListener: TradeKafkaListener,
        accountantTradeListener: AccountantTradeListener
    ) {
        // Trades are processed in the ordered events stream, together with cancel/reject events.
    }

    @Autowired
    fun configureEventListener(
        eventKafkaListener: EventKafkaListener,
        accountantEventListener: AccountantEventListener,
    ) {
        eventKafkaListener.addListener(accountantEventListener)
    }

    @Autowired
    fun configureTempEventListener(
        tempEventKafkaListener: TempEventKafkaListener,
        accountantTempEventListener: AccountantTempEventListener
    ) {
        tempEventKafkaListener.addListener(accountantTempEventListener)
    }

    @Autowired
    fun configureWithdrawRequestEventListener(
        withdrawRequestKafkaListener: WithdrawRequestKafkaListener,
        withdrawRequestEventListener: WithdrawRequestEventListener
    ) {
        withdrawRequestKafkaListener.addListener(withdrawRequestEventListener)
    }


    @Autowired
    fun configureDepositEventListener(
        depositKafkaListener: DepositKafkaListener,
        depositEventListener: DepositEventListener
    ) {
        depositKafkaListener.addListener(depositEventListener)
    }
}
