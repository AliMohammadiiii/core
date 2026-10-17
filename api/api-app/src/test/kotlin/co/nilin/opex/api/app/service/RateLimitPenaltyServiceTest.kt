package co.nilin.opex.api.app.service

import co.nilin.opex.api.app.data.RateLimitPenaltyState
import co.nilin.opex.api.core.inout.RateLimitGroup
import co.nilin.opex.api.core.spi.RateLimitConfigService
import co.nilin.opex.api.ports.postgres.util.RedisCacheHelper
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import io.mockk.mockk
import io.mockk.every
import io.mockk.verify

class RateLimitPenaltyServiceTest {
    @Test
    fun emptyPenaltyConfigurationStillBlocksForTheRequestWindow() {
        assertMissingPenaltiesBlocked(60, 60)
    }

    @Test
    fun invalidRequestWindowCannotProduceAnUnboundedOrZeroRetry() {
        assertMissingPenaltiesBlocked(0, 1)
    }

    private fun assertMissingPenaltiesBlocked(windowSeconds: Int, expectedRetry: Int) {
        val config = mockk<RateLimitConfigService>()
        val redis = mockk<RedisCacheHelper>()
        every { redis.get<RateLimitPenaltyState>(any()) } returns null
        every { config.getGroup(7L) } returns RateLimitGroup(
            id = 7, name = "history", requestCount = 100,
            requestWindowSeconds = windowSeconds, cooldownSeconds = 60, maxPenaltyLevel = 0
        )
        every { config.getPenalties(7L) } returns emptyList()

        val result = RateLimitPenaltyService(config, redis)
            .onLimit("owner", 7, "/opex/v1/user/history/trade", "GET")

        assertTrue(result.blocked)
        assertEquals(expectedRetry, result.retryAfterSeconds)
        verify(exactly = 0) { redis.get<RateLimitPenaltyState>(any()) }
    }
}
