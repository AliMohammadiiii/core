package co.nilin.opex.wallet.app.controller

import co.nilin.opex.wallet.app.dto.SandboxCapabilitiesResponse
import co.nilin.opex.wallet.app.dto.SandboxOperationRequest
import co.nilin.opex.wallet.app.dto.SandboxOperationResponse
import co.nilin.opex.wallet.app.service.SandboxWalletService
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.security.core.annotation.AuthenticationPrincipal
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.server.ResponseStatusException

@RestController
@RequestMapping("/v1/sandbox")
class SandboxWalletController(
    private val sandboxWalletService: SandboxWalletService,
) {
    @ExceptionHandler(ResponseStatusException::class)
    fun expectedFailure(error: ResponseStatusException): ResponseEntity<Map<String, String>> =
        ResponseEntity.status(error.status).body(mapOf("error" to (error.reason ?: "Sandbox request failed")))

    @PostMapping("/deposits")
    suspend fun deposit(
        @RequestBody request: SandboxOperationRequest,
        @AuthenticationPrincipal jwt: Jwt?,
    ): SandboxOperationResponse = sandboxWalletService.createDeposit(requireOwnerUuid(jwt), request)

    @PostMapping("/withdrawals")
    suspend fun withdraw(
        @RequestBody request: SandboxOperationRequest,
        @AuthenticationPrincipal jwt: Jwt?,
    ): SandboxOperationResponse = sandboxWalletService.createWithdrawal(requireOwnerUuid(jwt), request)

    @GetMapping("/operations")
    suspend fun operations(
        @RequestParam(defaultValue = "50") limit: Int,
        @RequestParam(defaultValue = "0") offset: Int,
        @AuthenticationPrincipal jwt: Jwt?,
    ): List<SandboxOperationResponse> =
        sandboxWalletService.listOperations(requireOwnerUuid(jwt), limit, offset)

    @GetMapping("/operations/{id}")
    suspend fun operation(
        @PathVariable id: String,
        @AuthenticationPrincipal jwt: Jwt?,
    ): SandboxOperationResponse = sandboxWalletService.getOperation(requireOwnerUuid(jwt), id)

    @GetMapping("/capabilities")
    suspend fun capabilities(@AuthenticationPrincipal jwt: Jwt?): SandboxCapabilitiesResponse {
        requireOwnerUuid(jwt)
        return sandboxWalletService.capabilities()
    }

    private fun requireOwnerUuid(jwt: Jwt?): String = jwt?.subject?.takeIf { it.isNotBlank() }
        ?: throw ResponseStatusException(HttpStatus.UNAUTHORIZED, "AUTHENTICATED_JWT_REQUIRED")
}
