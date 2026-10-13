package co.nilin.opex.accountant.app.listener
import co.nilin.opex.accountant.app.service.AccountantEventProcessor
import co.nilin.opex.accountant.ports.kafka.listener.spi.EventListener
import co.nilin.opex.matching.engine.core.eventh.events.CoreEvent
import kotlinx.coroutines.runBlocking
class AccountantEventListener(private val processor: AccountantEventProcessor) : EventListener {
    override fun id() = "AccountantEventListener"
    override fun onEvent(event: CoreEvent, partition: Int, offset: Long, timestamp: Long) { runBlocking { processor.process(event) } }
}
