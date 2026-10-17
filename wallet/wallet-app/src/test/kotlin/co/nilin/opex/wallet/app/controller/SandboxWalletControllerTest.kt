package co.nilin.opex.wallet.app.controller

import co.nilin.opex.wallet.app.dto.SandboxOperationRequest
import co.nilin.opex.wallet.app.dto.SandboxOperationResponse
import co.nilin.opex.wallet.app.service.SandboxWalletService
import co.nilin.opex.utility.error.DefaultErrorConfig
import co.nilin.opex.utility.error.controller.ExceptionController
import co.nilin.opex.utility.error.data.DefaultExceptionResponse
import co.nilin.opex.utility.error.spi.ErrorTranslator
import co.nilin.opex.utility.error.spi.ExceptionResponse
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import org.springframework.core.MethodParameter
import org.springframework.http.HttpStatus
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.web.reactive.BindingContext
import org.springframework.web.reactive.result.method.HandlerMethodArgumentResolver
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.ServerWebExchange
import reactor.core.publisher.Mono
import java.math.BigDecimal
import java.util.Date

class SandboxWalletControllerTest {
    private val sandboxWalletService = mockk<SandboxWalletService>()
    private val controller = SandboxWalletController(sandboxWalletService)
    private val request = SandboxOperationRequest("key", "BTC", BigDecimal.ONE)
    private val translator = mockk<ErrorTranslator>()

    private fun client(): WebTestClient {
        every { translator.translate(any()) } returns Mono.just<ExceptionResponse>(
            DefaultExceptionResponse("internal", 500, "internal", HttpStatus.INTERNAL_SERVER_ERROR, null, null, Date()),
        )
        val sharedAdvice = ExceptionController(translator, DefaultErrorConfig(), jacksonObjectMapper())
        val jwt = Jwt.withTokenValue("verified-token")
            .header("alg", "none")
            .subject("requesting-owner")
            .build()

        return WebTestClient.bindToController(controller)
            .controllerAdvice(sharedAdvice)
            .argumentResolvers { configurer ->
                configurer.addCustomResolver(object : HandlerMethodArgumentResolver {
                    override fun supportsParameter(parameter: MethodParameter): Boolean =
                        parameter.hasParameterAnnotation(AuthenticationPrincipal::class.java)

                    override fun resolveArgument(
                        parameter: MethodParameter,
                        bindingContext: BindingContext,
                        exchange: ServerWebExchange,
                    ): Mono<Any> = Mono.just(jwt)
                })
            }
            .build()
    }

    @Test
    fun `rejects requests without a verified JWT principal`() {
        val exception = assertThrows(ResponseStatusException::class.java) {
            runBlocking { controller.deposit(request, null) }
        }

        assertEquals(HttpStatus.UNAUTHORIZED, exception.status)
        coVerify(exactly = 0) { sandboxWalletService.createDeposit(any(), any()) }
    }

    @Test
    fun `uses the signed JWT subject rather than a profile id claim`() {
        val jwt = Jwt.withTokenValue("verified-token")
            .header("alg", "none")
            .subject("jwt-owner")
            .claim("id", "profile-owner")
            .build()
        val expected = SandboxOperationResponse(
            id = "operation-1",
            operationId = "operation-1",
            idempotencyKey = request.idempotencyKey,
            type = "DEPOSIT",
            state = "COMPLETED",
            status = "TEST_CREDITED",
            currency = "BTC",
            amount = "1",
            ledgerTransactionId = "transaction-1",
            withdrawalUuid = null,
            createdAt = "2026-10-16T12:00:00Z",
        )
        coEvery { sandboxWalletService.createDeposit("jwt-owner", request) } returns expected

        val response = runBlocking { controller.deposit(request, jwt) }

        assertEquals("operation-1", response.operationId)
        coVerify(exactly = 1) { sandboxWalletService.createDeposit("jwt-owner", request) }
    }

    @Test
    fun `keeps a foreign owner's operation lookup as not found with shared error advice`() {
        coEvery { sandboxWalletService.getOperation("requesting-owner", "foreign-operation") } throws
            ResponseStatusException(HttpStatus.NOT_FOUND, "SANDBOX_OPERATION_NOT_FOUND")

        client().get().uri("/v1/sandbox/operations/foreign-operation").exchange()
            .expectStatus().isNotFound
            .expectBody().jsonPath("$.error").isEqualTo("SANDBOX_OPERATION_NOT_FOUND")

        coVerify(exactly = 1) { sandboxWalletService.getOperation("requesting-owner", "foreign-operation") }
    }
}
