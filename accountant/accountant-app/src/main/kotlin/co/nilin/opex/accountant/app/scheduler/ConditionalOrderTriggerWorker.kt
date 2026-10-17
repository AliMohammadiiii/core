package co.nilin.opex.accountant.app.scheduler

import co.nilin.opex.accountant.app.service.OrderAdmissionService
import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.core.ParameterizedTypeReference
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import org.springframework.web.reactive.function.client.WebClient
import java.math.BigDecimal
import java.time.Duration
import java.time.Instant
import java.util.Date

@JsonIgnoreProperties(ignoreUnknown = true)
private data class RecentTradePrice(val price: BigDecimal? = null, val time: Date? = null)

@Service
class ConditionalOrderTriggerWorker(
    private val admission: OrderAdmissionService,
    private val readiness: ConditionalOrderTriggerReadiness,
    private val webClient: WebClient,
    @Value("\${app.market.url:lb://opex-market/}") private val marketBaseUrl: String,
    @Value("\${app.order-admission.max-trade-age-ms:10000}") private val maxTradeAgeMs: Long,
    @Value("\${app.order-admission.market-timeout-ms:3000}") private val marketTimeoutMs: Long,
    @Value("\${app.order-admission.readiness-symbols:BTC_USDT}") readinessSymbols: String,
) {
    private val log = LoggerFactory.getLogger(javaClass)
    private val readinessSymbols = readinessSymbols.split(',').map(String::trim).filter(String::isNotEmpty)
    private val triggerPageSize = 500
    private var triggerCursor: String? = null

    init {
        require(this.readinessSymbols.isNotEmpty()) { "At least one conditional-order readiness symbol is required" }
        require(this.readinessSymbols.all { it.matches(Regex("[A-Z0-9]+_[A-Z0-9]+")) }) {
            "Conditional-order readiness symbols must use BASE_QUOTE format"
        }
    }

    @Scheduled(fixedDelayString = "\${app.order-admission.trigger-poll-ms:1000}")
    fun poll() = runBlocking {
        try {
            pollOnce()
        } catch (error: Exception) {
            log.warn("Conditional order trigger poll failed: {}", error.message)
        }
    }

    suspend fun pollOnce() {
        val candidates = admission.pendingConditionalTriggers(afterOuid = triggerCursor, limit = triggerPageSize)
        triggerCursor = if (candidates.size == triggerPageSize) candidates.last().ouid else null
        val grouped = candidates.groupBy { it.pair.toString() }
        val symbolsToPoll = (readinessSymbols + grouped.keys).distinct()
        var successfulPoll = false
        for (symbol in symbolsToPoll) {
            val trades = try {
                fetchRecentTrade(symbol)
            } catch (error: Exception) {
                log.warn("Could not read last trade for {}: {}", symbol, error.message)
                null
            }
            val validTrades = trades?.takeIf { recentTrades ->
                recentTrades.all { it.price?.signum() == 1 && it.time != null }
            } ?: continue
            successfulPoll = true
            val orders = grouped[symbol] ?: continue
            val trade = validTrades.firstOrNull() ?: continue
            val tradePrice = requireNotNull(trade.price)
            val tradeTime = requireNotNull(trade.time).toInstant()
            val now = Instant.now()
            orders.forEach { order ->
                admission.activateIfTriggered(
                    candidate = order,
                    tradePrice = tradePrice,
                    tradeTime = tradeTime,
                    now = now,
                    maxTradeAge = Duration.ofMillis(maxTradeAgeMs),
                )
            }
        }

        if (successfulPoll) readiness.markPollSucceeded()
    }

    private suspend fun fetchRecentTrade(symbol: String): List<RecentTradePrice>? = webClient.get()
        .uri("${marketBaseUrl.trimEnd('/')}/v1/market/$symbol/recent-trades?limit=1")
        .retrieve()
        .bodyToMono(object : ParameterizedTypeReference<List<RecentTradePrice>>() {})
        .timeout(Duration.ofMillis(marketTimeoutMs))
        .awaitSingleOrNull()


}
