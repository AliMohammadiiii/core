package co.nilin.opex.wallet.app.controller

import co.nilin.opex.wallet.app.dto.CustodyLedgerActionRequest
import co.nilin.opex.wallet.app.dto.CustodyLedgerActionResponse
import co.nilin.opex.wallet.app.service.CustodyLedgerActionKind
import co.nilin.opex.wallet.app.service.CustodyLedgerInsufficientFundsException
import co.nilin.opex.wallet.app.service.CustodyLedgerService
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestHeader
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RestController

@RestController
@RequestMapping("/v1/custody/ledger")
class CustodyLedgerController(private val service: CustodyLedgerService) {
    @PostMapping("/actions")
    suspend fun apply(
        @RequestHeader("X-Custody-Key", required = false) custodyKey: String?,
        @RequestBody request: CustodyLedgerActionRequest,
    ): CustodyLedgerActionResponse {
        service.authorize(custodyKey)
        return try {
            service.apply(request)
        } catch (failure: CustodyLedgerInsufficientFundsException) {
            CustodyLedgerActionResponse(
                operationId = failure.operationId,
                ownerUuid = failure.ownerUuid,
                asset = failure.asset,
                amount = failure.amount,
                kind = failure.kind,
                referenceOperationId = failure.referenceOperationId,
                payloadHash = failure.payloadHash,
                status = "INSUFFICIENT_FUNDS",
                transactionId = null,
                errorCode = "INSUFFICIENT_FUNDS",
            )
        }
    }

    @GetMapping("/actions/{operationId}")
    suspend fun getAction(
        @RequestHeader("X-Custody-Key", required = false) custodyKey: String?,
        @PathVariable operationId: String,
    ): CustodyLedgerActionResponse {
        service.authorize(custodyKey)
        return service.getAction(operationId)
    }

    @GetMapping("/balances/{ownerUuid}/{asset}")
    suspend fun balance(
        @RequestHeader("X-Custody-Key", required = false) custodyKey: String?,
        @PathVariable ownerUuid: String,
        @PathVariable asset: String,
    ) = run {
        service.authorize(custodyKey)
        service.balance(ownerUuid, asset)
    }

    @GetMapping("/capabilities")
    suspend fun capabilities(
        @RequestHeader("X-Custody-Key", required = false) custodyKey: String?,
    ) = run {
        service.authorize(custodyKey)
        service.capabilities()
    }

    @PostMapping("/provisioning")
    suspend fun bootstrap(
        @RequestHeader("X-Custody-Key", required = false) custodyKey: String?,
    ) = run {
        service.authorize(custodyKey)
        service.bootstrapAssets()
    }
}
