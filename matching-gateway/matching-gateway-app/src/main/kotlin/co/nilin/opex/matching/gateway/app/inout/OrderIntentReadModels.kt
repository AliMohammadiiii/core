package co.nilin.opex.matching.gateway.app.inout

data class OrderIntentCapabilities(val supportedTypes: List<String>)

data class OrderIntentMetadata(
    val ouid: String,
    val clientOrderId: String,
    val symbol: String,
    val type: String,
    val side: String,
    val stopPrice: String?,
    val price: String,
    val quantity: String,
    val filledQuantity: String?,
    val remainingQuantity: String?,
    val createdAt: String,
    val status: String,
)
