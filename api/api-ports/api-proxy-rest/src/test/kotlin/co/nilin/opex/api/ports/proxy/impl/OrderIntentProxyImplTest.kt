package co.nilin.opex.api.ports.proxy.impl

import co.nilin.opex.api.core.inout.OrderIntentCapabilities
import co.nilin.opex.api.core.inout.OrderIntentMetadata
import co.nilin.opex.api.core.inout.OrderIntentRequest
import java.math.BigDecimal
import org.springframework.http.HttpMethod
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.springframework.web.server.ResponseStatusException
import org.junit.jupiter.api.Test
import org.springframework.http.HttpHeaders
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.web.reactive.function.client.ClientRequest
import org.springframework.web.reactive.function.client.ClientResponse
import org.springframework.web.reactive.function.client.ExchangeFunction
import org.springframework.web.reactive.function.client.WebClient
import reactor.core.publisher.Mono

class OrderIntentProxyImplTest {
    @Test
    fun abandonPreservesNativeConflictAndUnavailableResponses() {
        for (status in listOf(HttpStatus.CONFLICT, HttpStatus.SERVICE_UNAVAILABLE)) {
            var dispatches = 0
            val exchange = ExchangeFunction {
                dispatches++
                Mono.just(ClientResponse.create(status)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .body("""{"error":"native reconciliation unavailable or conflicting identity"}""")
                    .build())
            }
            val proxy = OrderIntentProxyImpl(WebClient.builder().exchangeFunction(exchange).build(), "http://matching-gateway")
            val error = assertThrows(ResponseStatusException::class.java) {
                runBlocking {
                    proxy.abandon(OrderIntentRequest("uncertain-1", "BTC_USDT", "LIMIT", "BUY",
                        BigDecimal("0.0001"), BigDecimal("80000"), true), "owner-token")
                }
            }
            assertEquals(status, error.statusCode)
            assertEquals(1, dispatches)
        }
    }

    @Test
    fun abandonForwardsTheOriginalIdentityAndBearerToTheNativeCancellationRoute() = runBlocking {
        val requests = mutableListOf<ClientRequest>()
        val exchange = ExchangeFunction { request ->
            requests += request
            Mono.just(ClientResponse.create(HttpStatus.OK)
                .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                .body("""{"clientOrderId":"uncertain-1","ouid":"native-1","symbol":"BTC_USDT","status":"CANCELED","orderId":null,"type":"LIMIT","side":"BUY","postOnly":true,"quantity":"0.0001","price":"80000","filledQuantity":"0","remainingQuantity":"0.0001","reason":"abandoned_before_admission","createdAt":"2026-10-16T10:00:00Z"}""")
                .build())
        }
        val proxy = OrderIntentProxyImpl(WebClient.builder().exchangeFunction(exchange).build(), "http://matching-gateway")
        val receipt = proxy.abandon(OrderIntentRequest("uncertain-1", "BTC_USDT", "LIMIT", "BUY",
            BigDecimal("0.0001"), BigDecimal("80000"), true), "owner-token")
        assertEquals("CANCELED", receipt.status)
        assertEquals("uncertain-1", receipt.clientOrderId)
        assertEquals("0", receipt.filledQuantity)
        assertEquals("/v2/order/abandon", requests.single().url().path)
        assertEquals(HttpMethod.POST, requests.single().method())
        assertEquals("Bearer owner-token", requests.single().headers().getFirst(HttpHeaders.AUTHORIZATION))
    }

    @Test
    fun readEndpointsForwardBearerTokenAndDeserializeNativeOrderModels() = runBlocking {
        val requests = mutableListOf<ClientRequest>()
        val exchange = ExchangeFunction { request ->
            requests += request
            val body = when (request.url().path) {
                "/v2/order/capabilities" -> """{"supportedTypes":["LIMIT","MARKET","STOP_LIMIT"]}"""
                "/v2/order/metadata" -> """[{"ouid":"order-1","clientOrderId":"client-1","symbol":"BTC_USDT","type":"STOP_LIMIT","side":"BUY","stopPrice":"10.2500","price":"10.5000","quantity":"0.1250","filledQuantity":"0.1250","remainingQuantity":"0","createdAt":"2026-10-16T10:00:00Z","status":"FILLED"}]"""
                else -> error("Unexpected request path: ${request.url().path}")
            }
            Mono.just(
                ClientResponse.create(HttpStatus.OK)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .body(body)
                    .build()
            )
        }
        val proxy = OrderIntentProxyImpl(WebClient.builder().exchangeFunction(exchange).build(), "http://matching-gateway")

        val capabilities: OrderIntentCapabilities = proxy.capabilities("jwt-token")
        val metadata: List<OrderIntentMetadata> = proxy.metadata("jwt-token")

        assertEquals(listOf("LIMIT", "MARKET", "STOP_LIMIT"), capabilities.supportedTypes)
        assertEquals("10.2500", metadata.single().stopPrice)
        assertEquals("10.5000", metadata.single().price)
        assertEquals("0.1250", metadata.single().quantity)
        assertEquals("BUY", metadata.single().side)
        assertEquals("0.1250", metadata.single().filledQuantity)
        assertEquals("0", metadata.single().remainingQuantity)
        assertEquals("FILLED", metadata.single().status)
        assertEquals(listOf("/v2/order/capabilities", "/v2/order/metadata"), requests.map { it.url().path })
        assertEquals(listOf("Bearer jwt-token", "Bearer jwt-token"), requests.map { it.headers().getFirst(HttpHeaders.AUTHORIZATION) })
    }
}
