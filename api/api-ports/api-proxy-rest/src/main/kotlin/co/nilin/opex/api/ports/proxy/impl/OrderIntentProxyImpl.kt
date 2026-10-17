package co.nilin.opex.api.ports.proxy.impl
import co.nilin.opex.api.core.inout.*
import co.nilin.opex.api.core.spi.OrderIntentProxy
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.beans.factory.annotation.Value
import org.springframework.http.HttpMethod
import org.springframework.stereotype.Component
import org.springframework.web.reactive.function.client.WebClient
import org.springframework.web.reactive.function.client.bodyToMono
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.util.UriComponentsBuilder
import java.net.URI

@Component
class OrderIntentProxyImpl(@Qualifier("generalWebClient") private val client: WebClient,
    @Value("\${app.matching-gateway.url}") private val baseUrl: String) : OrderIntentProxy {
    override suspend fun abandon(request: OrderIntentRequest, token: String) = client.post()
        .uri(URI.create("$baseUrl/v2/order/abandon")).headers { it.setBearerAuth(token) }.bodyValue(request)
        .retrieve().onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<OrderIntentReceipt>().awaitSingle()
    override suspend fun submit(request: OrderIntentRequest, token: String) = client.post()
        .uri(URI.create("$baseUrl/v2/order")).headers { it.setBearerAuth(token) }.bodyValue(request)
        .retrieve().onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<OrderIntentReceipt>().awaitSingle()
    override suspend fun lookup(clientOrderId: String?, ouid: String?, token: String) = request(HttpMethod.GET, clientOrderId, ouid, token)
    override suspend fun cancel(clientOrderId: String?, ouid: String?, token: String) = request(HttpMethod.DELETE, clientOrderId, ouid, token)
    private suspend fun request(method: HttpMethod, clientOrderId: String?, ouid: String?, token: String): OrderIntentReceipt {
        val uri = UriComponentsBuilder.fromUriString("$baseUrl/v2/order")
        if (clientOrderId != null) uri.queryParam("clientOrderId", clientOrderId)
        if (ouid != null) uri.queryParam("ouid", ouid)
        return client.method(method).uri(uri.build().encode().toUri()).headers { it.setBearerAuth(token) }
            .retrieve().onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
            .bodyToMono<OrderIntentReceipt>().awaitSingle()
    }
    override suspend fun owned(token: String) = client.get().uri(URI.create("$baseUrl/v2/order/owned"))
        .headers { it.setBearerAuth(token) }.retrieve().onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<List<OrderIntentReceipt>>().awaitSingle()
    override suspend fun events(cursor: Long, token: String) = client.get()
        .uri(URI.create("$baseUrl/v2/order/events?cursor=$cursor")).headers { it.setBearerAuth(token) }.retrieve()
        .onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<List<OrderIntentEvent>>().awaitSingle()

    override suspend fun capabilities(token: String) = client.get()
        .uri(URI.create("$baseUrl/v2/order/capabilities")).headers { it.setBearerAuth(token) }.retrieve()
        .onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<OrderIntentCapabilities>().awaitSingle()

    override suspend fun metadata(token: String) = client.get()
        .uri(URI.create("$baseUrl/v2/order/metadata")).headers { it.setBearerAuth(token) }.retrieve()
        .onStatus({ it.isError }) { it.createException().map { error -> ResponseStatusException(error.statusCode, error.responseBodyAsString, error) } }
        .bodyToMono<List<OrderIntentMetadata>>().awaitSingle()
}
