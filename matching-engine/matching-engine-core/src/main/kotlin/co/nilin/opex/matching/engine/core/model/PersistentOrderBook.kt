package co.nilin.opex.matching.engine.core.model

class PersistentOrderBook {

    lateinit var pair: Pair
    var lastOrder: PersistentOrder? = null
    var orders: List<PersistentOrder>? = emptyList()
    var tradeCounter: Long = 0
    var orderCounter: Long = 0
    var processedOuids: List<String> = emptyList()
    var pendingCancellations: Map<String, String> = emptyMap()
    var pendingEvents: MutableList<String> = mutableListOf()

    constructor() {
    }

    constructor(pair: Pair) {
        this.pair = pair
    }

}
