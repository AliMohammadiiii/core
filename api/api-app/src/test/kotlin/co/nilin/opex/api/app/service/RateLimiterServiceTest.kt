package co.nilin.opex.api.app.service

import co.nilin.opex.api.ports.postgres.util.RedisCacheHelper
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

class RateLimiterServiceTest {
    @Test
    fun concurrentNewRequestIdentitiesDoNotCorruptTheBucketRegistry() {
        val limiter = RateLimiterService(cache())
        runConcurrent(16) { worker ->
            repeat(100) { request ->
                limiter.checkRateLimit("owner-$worker-$request", 10, 3600, "/wallet", "GET")
            }
            100
        }
    }

    @Test
    fun concurrentRequestsForOneIdentityConsumeEachTokenOnce() {
        val limiter = RateLimiterService(cache())
        val allowed = runConcurrent(16) {
            (1..20).count { limiter.checkRateLimit("owner", 10, 3600, "/history", "GET") }
        }.sum()
        assertEquals(10, allowed)
    }

    private fun cache(): RedisCacheHelper {
        val values = ConcurrentHashMap<String, Long>()
        return mockk<RedisCacheHelper>().also { cache ->
            every { cache.get<Long>(any()) } answers { values[firstArg()] }
            every { cache.put(any(), any(), any()) } answers {
                values[firstArg()] = secondArg<Long>()
                Unit
            }
        }
    }

    private fun runConcurrent(workers: Int, action: (Int) -> Int): List<Int> {
        val start = CountDownLatch(1)
        val executor = Executors.newFixedThreadPool(workers)
        return try {
            val futures = (0 until workers).map { worker ->
                executor.submit<Int> { start.await(); action(worker) }
            }
            start.countDown()
            futures.map { it.get(30, TimeUnit.SECONDS) }
        } finally {
            executor.shutdownNow()
        }
    }
}
