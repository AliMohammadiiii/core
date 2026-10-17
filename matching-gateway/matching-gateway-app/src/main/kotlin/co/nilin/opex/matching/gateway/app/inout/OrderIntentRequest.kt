package co.nilin.opex.matching.gateway.app.inout

import java.math.BigDecimal

data class OrderIntentRequest(val clientOrderId: String, val symbol: String, val type: String,
    val side: String, val quantity: BigDecimal, val price: BigDecimal, val postOnly: Boolean = false,
    val stopPrice: BigDecimal? = null)
