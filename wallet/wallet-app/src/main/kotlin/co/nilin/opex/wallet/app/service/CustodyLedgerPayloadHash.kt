package co.nilin.opex.wallet.app.service

import java.math.BigDecimal
import java.security.MessageDigest

object CustodyLedgerPayloadHash {
    fun calculate(
        operationId: String,
        ownerUuid: String,
        asset: String,
        amount: BigDecimal,
        kind: CustodyLedgerActionKind,
        referenceOperationId: String?,
    ): String {
        val canonicalPayload = listOf(
            "v1",
            operationId,
            ownerUuid,
            asset,
            amount.stripTrailingZeros().toPlainString(),
            kind.name,
            referenceOperationId.orEmpty(),
        ).joinToString("\n")
        val digest = MessageDigest.getInstance("SHA-256").digest(canonicalPayload.toByteArray(Charsets.UTF_8))
        return digest.joinToString("") { byte -> "%02x".format(byte.toInt() and 0xff) }
    }
}
