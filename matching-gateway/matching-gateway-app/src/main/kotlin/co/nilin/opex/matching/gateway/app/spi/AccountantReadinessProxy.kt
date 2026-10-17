package co.nilin.opex.matching.gateway.app.spi

interface AccountantReadinessProxy {
    suspend fun conditionalOrdersReady(): Boolean
}
