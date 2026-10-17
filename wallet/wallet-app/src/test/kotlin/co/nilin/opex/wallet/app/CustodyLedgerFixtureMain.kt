package co.nilin.opex.wallet.app

import co.nilin.opex.wallet.app.controller.CustodyLedgerController
import co.nilin.opex.wallet.app.dto.CustodyLedgerActionRequest
import co.nilin.opex.wallet.app.service.CustodyLedgerService
import co.nilin.opex.wallet.app.WalletApp
import co.nilin.opex.wallet.app.ExternalPostgresEnabledTest
import co.nilin.opex.wallet.app.CustodyLedgerFixtureSupport
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration
import org.springframework.context.ApplicationContextInitializer
import org.springframework.context.ConfigurableApplicationContext
import org.springframework.core.env.MapPropertySource
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.web.server.ResponseStatusException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.net.URLDecoder
import java.nio.charset.StandardCharsets
import java.util.concurrent.CountDownLatch
import com.sun.net.httpserver.HttpExchange
import com.sun.net.httpserver.HttpServer

/**
 * Test-only HTTP adapter for the native custody ledger service. It defaults to loopback and
 * checks the database name, PostgreSQL comment, and marker row before migrations.
 */
fun main(@Suppress("UNUSED_PARAMETER") args: Array<String>) {
    check(System.getenv("OPEX_CUSTODY_FIXTURE_ENABLED") == "true") {
        "Set OPEX_CUSTODY_FIXTURE_ENABLED=true to start the isolated custody fixture."
    }
    val settings = CustodyLedgerFixtureSupport.settingsFromEnvironment()
    CustodyLedgerFixtureSupport.migrateFixtureDatabase(settings)
    val ledgerKey = System.getenv("OPEX_CUSTODY_FIXTURE_LEDGER_KEY")
        ?: error("Set OPEX_CUSTODY_FIXTURE_LEDGER_KEY for the loopback test adapter")
    check(ledgerKey.isNotBlank()) { "OPEX_CUSTODY_FIXTURE_LEDGER_KEY must not be blank" }

    val fixtureProperties = CustodyLedgerFixtureSupport.springProperties(settings, ledgerKey)
    val context = SpringApplicationBuilder(
        WalletApp::class.java,
        ExternalPostgresEnabledTest::class.java,
        TestChannelBinderConfiguration::class.java,
    )
        .profiles("test", "external-postgres", "no-vault")
        .initializers(
            ApplicationContextInitializer<ConfigurableApplicationContext> { applicationContext ->
                applicationContext.environment.propertySources.addFirst(
                    MapPropertySource("custodyLedgerFixture", fixtureProperties),
                )
            },
        )
        .web(WebApplicationType.NONE)
        .run()

    val service = context.getBean(CustodyLedgerService::class.java)
    runBlocking {
        CustodyLedgerFixtureSupport.prepareNativeFixture(
            custodyService = service,
            currencyServiceManager = context.getBean(CurrencyServiceManager::class.java),
            walletOwnerManager = context.getBean(WalletOwnerManager::class.java),
            walletManager = context.getBean(WalletManager::class.java),
            jdbcTemplate = context.getBean(JdbcTemplate::class.java),
        )
    }

    val port = System.getenv("OPEX_CUSTODY_FIXTURE_PORT")?.toIntOrNull() ?: 18447
    require(port in 1..65535) { "OPEX_CUSTODY_FIXTURE_PORT must be between 1 and 65535" }
    val bindAddress = System.getenv("OPEX_CUSTODY_FIXTURE_BIND")?.ifBlank { null } ?: "127.0.0.1"
    require(bindAddress == "127.0.0.1" || bindAddress == "0.0.0.0") {
        "OPEX_CUSTODY_FIXTURE_BIND must be 127.0.0.1 or 0.0.0.0."
    }
    val server = CustodyLedgerFixtureHttpServer(
        controller = context.getBean(CustodyLedgerController::class.java),
        objectMapper = context.getBean(ObjectMapper::class.java),
        port = port,
        bindAddress = bindAddress,
    )
    server.start()
    println(
        "Custody ledger fixture ready on http://$bindAddress:$port; " +
            "database=${settings.database}; fixture=${CustodyLedgerFixtureSupport.FIXTURE_ID}; " +
            "treasury=${CustodyLedgerFixtureSupport.TREASURY_OWNER_UUID}; " +
            "user=${CustodyLedgerFixtureSupport.USER_OWNER_UUID}",
    )

    Runtime.getRuntime().addShutdownHook(Thread {
        server.stop()
        context.close()
    })
    CountDownLatch(1).await()
}

private class CustodyLedgerFixtureHttpServer(
    private val controller: CustodyLedgerController,
    private val objectMapper: ObjectMapper,
    port: Int,
    bindAddress: String,
) {
    private val server = HttpServer.create(
        InetSocketAddress(InetAddress.getByName(bindAddress), port),
        0,
    )

    init {
        server.createContext("/v1/custody/ledger") { exchange -> handle(exchange) }
        server.executor = java.util.concurrent.Executors.newCachedThreadPool()
    }

    fun start() = server.start()

    fun stop() = server.stop(0)

    private fun handle(exchange: HttpExchange) {
        try {
            val response = runBlocking { dispatch(exchange) }
            writeJson(exchange, HttpStatus.OK.value(), response)
        } catch (failure: ResponseStatusException) {
            writeJson(
                exchange,
                failure.status.value(),
                mapOf("errorCode" to (failure.reason ?: "CUSTODY_LEDGER_REQUEST_REJECTED")),
            )
        } catch (_: IllegalArgumentException) {
            writeJson(exchange, HttpStatus.BAD_REQUEST.value(), mapOf("errorCode" to "INVALID_REQUEST"))
        } catch (_: Exception) {
            writeJson(exchange, HttpStatus.INTERNAL_SERVER_ERROR.value(), mapOf("errorCode" to "INTERNAL_ERROR"))
        } finally {
            exchange.close()
        }
    }

    private suspend fun dispatch(exchange: HttpExchange): Any {
        val key = exchange.requestHeaders.getFirst(CUSTODY_KEY_HEADER)
        val path = exchange.requestURI.path.removePrefix(API_PREFIX).trim('/')
        val parts = path.split('/').filter(String::isNotEmpty).map(::decodePathSegment)
        return when {
            exchange.requestMethod == "POST" && parts == listOf("actions") -> {
                val bytes = exchange.requestBody.use { it.readNBytes(MAX_REQUEST_BYTES + 1) }
                if (bytes.size > MAX_REQUEST_BYTES) {
                    throw ResponseStatusException(HttpStatus.PAYLOAD_TOO_LARGE, "REQUEST_TOO_LARGE")
                }
                val request = objectMapper.readValue(bytes, CustodyLedgerActionRequest::class.java)
                controller.apply(key, request)
            }
            exchange.requestMethod == "GET" && parts.size == 2 && parts[0] == "actions" ->
                controller.getAction(key, parts[1])
            exchange.requestMethod == "GET" && parts.size == 3 && parts[0] == "balances" ->
                controller.balance(key, parts[1], parts[2])
            exchange.requestMethod == "GET" && parts == listOf("capabilities") ->
                controller.capabilities(key)
            exchange.requestMethod == "POST" && parts == listOf("provisioning") ->
                controller.bootstrap(key)
            else -> throw ResponseStatusException(HttpStatus.NOT_FOUND, "FIXTURE_ROUTE_NOT_FOUND")
        }
    }

    private fun writeJson(exchange: HttpExchange, status: Int, body: Any) {
        val bytes = objectMapper.writeValueAsBytes(body)
        exchange.responseHeaders.set("Content-Type", "application/json; charset=utf-8")
        exchange.sendResponseHeaders(status, bytes.size.toLong())
        exchange.responseBody.use { it.write(bytes) }
    }

    private fun decodePathSegment(value: String): String =
        URLDecoder.decode(value, StandardCharsets.UTF_8.name())

    private companion object {
        const val API_PREFIX = "/v1/custody/ledger/"
        const val CUSTODY_KEY_HEADER = "X-Custody-Key"
        const val MAX_REQUEST_BYTES = 16 * 1024
    }
}
