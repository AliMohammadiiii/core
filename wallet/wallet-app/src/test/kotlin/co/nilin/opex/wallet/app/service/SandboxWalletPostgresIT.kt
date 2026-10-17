package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.app.dto.SandboxOperationRequest
import co.nilin.opex.wallet.app.ExternalPostgresEnabledTest
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.model.SandboxOperationStatus
import co.nilin.opex.wallet.core.model.SandboxOperationType
import co.nilin.opex.wallet.core.model.SandboxWalletOperation
import co.nilin.opex.wallet.core.model.Wallet
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.SandboxWalletOperationPersister
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import io.r2dbc.spi.ConnectionFactoryOptions
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration
import org.springframework.context.annotation.Import
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import org.springframework.http.HttpStatus
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.nio.charset.StandardCharsets
import java.sql.DriverManager
import java.time.LocalDateTime
import java.time.ZoneOffset
import java.util.UUID

@TestPropertySource(
    properties = [
        "app.wallet.sandbox.enabled=true",
        "app.wallet.sandbox.funding-owner-uuid=sandbox-postgres-funding-owner",
        "spring.kafka.bootstrap-servers=127.0.0.1:1",
        "spring.kafka.listener.auto-startup=false",
        "spring.kafka.admin.auto-create=false",
    ],
)
@EnabledIfEnvironmentVariable(named = "OPEX_TEST_POSTGRES", matches = ".+")
@SpringBootTest
@ActiveProfiles("test", "external-postgres")
@DirtiesContext
@Import(TestChannelBinderConfiguration::class, ExternalPostgresEnabledTest::class)
class SandboxWalletPostgresIT {
    @Autowired
    private lateinit var sandboxWalletService: SandboxWalletService

    @Autowired
    private lateinit var walletOwnerManager: WalletOwnerManager

    @Autowired
    private lateinit var walletManager: WalletManager

    @Autowired
    private lateinit var currencyService: CurrencyServiceManager

    @Autowired
    private lateinit var operationPersister: SandboxWalletOperationPersister

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    private lateinit var currency: co.nilin.opex.wallet.core.inout.CurrencyCommand
    private lateinit var fundingOwner: WalletOwner

    @BeforeEach
    fun setUpFundingWallet() = runBlocking {
        currency = currencyService.fetchCurrency(FetchCurrency(symbol = CURRENCY_SYMBOL))
            ?: currencyService.createNewCurrency(
                CurrencyCommand(name = CURRENCY_SYMBOL, symbol = CURRENCY_SYMBOL, precision = BigDecimal("8")),
            )
            ?: error("Failed to create sandbox integration-test currency")
        fundingOwner = walletOwnerManager.findWalletOwner(FUNDING_OWNER_UUID)
            ?: walletOwnerManager.createWalletOwner(FUNDING_OWNER_UUID, "Sandbox integration funding", "system")
        val existingFundingWallet = walletManager.findWalletByOwnerAndCurrencyAndType(
            fundingOwner,
            WalletType.MAIN,
            currency,
        )
        if (existingFundingWallet == null) {
            walletManager.createWallet(
                fundingOwner,
                Amount(currency, BigDecimal("1000000")),
                currency,
                WalletType.MAIN,
            )
        }
    }

    @Test
    fun `withdrawal refreshes cashout balance and settles through PostgreSQL`() = runBlocking {
        val userOwner = createUser()
        val userWalletBefore = mainWallet(userOwner)
        val fundingBefore = mainWallet(fundingOwner).balance.amount
        assertDecimalEquals(BigDecimal("5"), userWalletBefore.balance.amount)

        val response = sandboxWalletService.createWithdrawal(
            userOwner.uuid,
            SandboxOperationRequest("withdraw-${UUID.randomUUID()}", CURRENCY_SYMBOL, AMOUNT),
        )

        assertEquals("COMPLETED", response.state)
        assertEquals("TEST_SETTLED", response.status)
        assertEquals(true, response.createdAt.endsWith("Z"))
        assertDecimalEquals(BigDecimal("4"), mainWallet(userOwner).balance.amount)
        // Both the reserve and test settlement have completed, so cashout must be empty.
        assertDecimalEquals(BigDecimal.ZERO, cashoutWallet(userOwner)!!.balance.amount)
        assertDecimalEquals(fundingBefore.add(AMOUNT), mainWallet(fundingOwner).balance.amount)
        assertEquals(1L, count("SELECT COUNT(*) FROM withdraws WHERE withdraw_uuid = ?", response.withdrawalUuid!!))
        assertEquals(1L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?", "wallet:sandbox:withdrawal:${response.operationId}:reserve"))
        assertEquals(1L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?", "wallet:sandbox:withdrawal:${response.operationId}:settle"))
    }

    @Test
    fun `deposit effects roll back when final operation insert collides in PostgreSQL`() = runBlocking {
        val userOwner = createUser()
        val userBalanceBefore = mainWallet(userOwner).balance.amount
        val fundingBalanceBefore = mainWallet(fundingOwner).balance.amount
        val key = "deposit-${UUID.randomUUID()}"
        val operationId = stableOperationId(userOwner.uuid, key)
        seedOperationIdCollision(operationId)

        assertThrows(Exception::class.java) {
            runBlocking {
                sandboxWalletService.createDeposit(
                    userOwner.uuid,
                    SandboxOperationRequest(key, CURRENCY_SYMBOL, AMOUNT),
                )
            }
        }

        assertEquals(0L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?", "wallet:sandbox:deposit:$operationId"))
        assertEquals(0L, count("SELECT COUNT(*) FROM user_transaction ut JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ?", "wallet:sandbox:deposit:$operationId"))
        assertEquals(0L, count("SELECT COUNT(*) FROM deposits WHERE deposit_uuid = ?", operationId))
        assertNull(operationPersister.findByOwnerAndIdempotencyKey(userOwner.uuid, key))
        assertDecimalEquals(userBalanceBefore, mainWallet(userOwner).balance.amount)
        assertDecimalEquals(fundingBalanceBefore, mainWallet(fundingOwner).balance.amount)
    }

    @Test
    fun `withdrawal effects roll back when final operation insert collides in PostgreSQL`() = runBlocking {
        val userOwner = createUser()
        val userBalanceBefore = mainWallet(userOwner).balance.amount
        val fundingBalanceBefore = mainWallet(fundingOwner).balance.amount
        val key = "withdraw-${UUID.randomUUID()}"
        val operationId = stableOperationId(userOwner.uuid, key)
        seedOperationIdCollision(operationId)

        assertThrows(Exception::class.java) {
            runBlocking {
                sandboxWalletService.createWithdrawal(
                    userOwner.uuid,
                    SandboxOperationRequest(key, CURRENCY_SYMBOL, AMOUNT),
                )
            }
        }

        assertEquals(0L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref IN (?, ?)", "wallet:sandbox:withdrawal:$operationId:reserve", "wallet:sandbox:withdrawal:$operationId:settle"))
        assertEquals(0L, count("SELECT COUNT(*) FROM user_transaction ut JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref IN (?, ?)", "wallet:sandbox:withdrawal:$operationId:reserve", "wallet:sandbox:withdrawal:$operationId:settle"))
        assertEquals(0L, count("SELECT COUNT(*) FROM withdraws WHERE withdraw_uuid = ?", operationId))
        assertNull(operationPersister.findByOwnerAndIdempotencyKey(userOwner.uuid, key))
        assertDecimalEquals(userBalanceBefore, mainWallet(userOwner).balance.amount)
        assertDecimalEquals(fundingBalanceBefore, mainWallet(fundingOwner).balance.amount)
        assertNull(cashoutWallet(userOwner))
    }

    @Test
    fun `concurrent deposits with the same idempotency key return one receipt and one ledger movement`() = runBlocking {
        val userOwner = createUser()
        val fundingBalanceBefore = mainWallet(fundingOwner).balance.amount
        val key = "concurrent-${UUID.randomUUID()}"
        val request = SandboxOperationRequest(key, CURRENCY_SYMBOL, AMOUNT)

        val responses = coroutineScope {
            (1..4).map {
                async(Dispatchers.IO) {
                    sandboxWalletService.createDeposit(userOwner.uuid, request)
                }
            }.awaitAll()
        }

        val operationId = responses.first().operationId
        assertEquals(1, responses.distinct().size, "Identical requests must receive the same complete receipt; responses=$responses")
        assertEquals(1, responses.map { it.operationId }.distinct().size, "Identical requests must share one operation ID.")
        assertEquals(1, responses.map { it.ledgerTransactionId }.distinct().size, "Identical requests must share one ledger transaction.")
        assertEquals(1L, count(
            "SELECT COUNT(*) FROM sandbox_wallet_operations WHERE owner_uuid = ? AND idempotency_key = ?",
            userOwner.uuid,
            key,
        ), "Identical requests must persist one operation receipt.")
        val transferRef = "wallet:sandbox:deposit:$operationId"
        assertEquals(1L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?", transferRef), "There must be one transfer record.")
        assertEquals(
            2L,
            count(
                """SELECT COUNT(*) FROM user_transaction ut
                    JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ?""",
                transferRef,
            ),
            "A single manual deposit is recorded as one credit and one funding debit.",
        )
        assertDecimalEquals(
            BigDecimal.ZERO,
            jdbcTemplate.queryForObject(
                """SELECT COALESCE(SUM(ut.balance_change), 0) FROM user_transaction ut
                    JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ?""",
                BigDecimal::class.java,
                transferRef,
            )!!,
        )
        assertDecimalEquals(
            AMOUNT,
            jdbcTemplate.queryForObject(
                """SELECT COALESCE(SUM(ut.balance_change), 0) FROM user_transaction ut
                    JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ? AND ut.owner_id = ?""",
                BigDecimal::class.java,
                transferRef,
                userOwner.id!!,
            )!!,
        )
        assertDecimalEquals(
            AMOUNT.negate(),
            jdbcTemplate.queryForObject(
                """SELECT COALESCE(SUM(ut.balance_change), 0) FROM user_transaction ut
                    JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ? AND ut.owner_id = ?""",
                BigDecimal::class.java,
                transferRef,
                fundingOwner.id!!,
            )!!,
        )
        assertEquals(1L, count("SELECT COUNT(*) FROM deposits WHERE deposit_uuid = ?", operationId), "There must be one deposit record.")
        assertDecimalEquals(BigDecimal("6"), mainWallet(userOwner).balance.amount)
        assertDecimalEquals(fundingBalanceBefore.subtract(AMOUNT), mainWallet(fundingOwner).balance.amount)
    }

    @Test
    fun `concurrent idempotency key reuse with a different payload returns conflict`() = runBlocking {
        val userOwner = createUser()
        val key = "conflicting-${UUID.randomUUID()}"
        val requests = listOf(
            SandboxOperationRequest(key, CURRENCY_SYMBOL, BigDecimal("1")),
            SandboxOperationRequest(key, CURRENCY_SYMBOL, BigDecimal("2")),
        )

        val outcomes = coroutineScope {
            requests.map { request ->
                async(Dispatchers.IO) {
                    runCatching { sandboxWalletService.createDeposit(userOwner.uuid, request) }
                }
            }.awaitAll()
        }

        val accepted = outcomes.mapNotNull { it.getOrNull() }
        val rejected = outcomes.mapNotNull { it.exceptionOrNull() }
        assertEquals(1, accepted.size)
        assertEquals(1, rejected.size)
        assertEquals(ResponseStatusException::class.java, rejected.single()::class.java)
        assertEquals(HttpStatus.CONFLICT, (rejected.single() as ResponseStatusException).status)

        val operationId = accepted.single().operationId
        assertEquals(1L, count(
            "SELECT COUNT(*) FROM sandbox_wallet_operations WHERE owner_uuid = ? AND idempotency_key = ?",
            userOwner.uuid,
            key,
        ))
        assertEquals(1L, count("SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?", "wallet:sandbox:deposit:$operationId"))
        assertDecimalEquals(BigDecimal("5").add(BigDecimal(accepted.single().amount)), mainWallet(userOwner).balance.amount)
    }

    private suspend fun createUser(): WalletOwner {
        val ownerUuid = UUID.randomUUID().toString()
        val owner = walletOwnerManager.createWalletOwner(ownerUuid, "Sandbox integration user", "user")
        walletManager.createWallet(owner, Amount(currency, BigDecimal("5")), currency, WalletType.MAIN)
        return owner
    }

    private suspend fun mainWallet(owner: WalletOwner): Wallet =
        walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            ?: error("Expected a MAIN wallet for ${owner.uuid}")

    private suspend fun cashoutWallet(owner: WalletOwner): Wallet? =
        walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.CASHOUT, currency)

    private suspend fun seedOperationIdCollision(operationId: String) {
        operationPersister.persist(
            SandboxWalletOperation(
                id = operationId,
                ownerUuid = "collision-owner-${UUID.randomUUID()}",
                idempotencyKey = "existing-key",
                type = SandboxOperationType.DEPOSIT,
                currency = CURRENCY_SYMBOL,
                amount = AMOUNT,
                status = SandboxOperationStatus.COMPLETED,
                ledgerTransactionId = "preexisting-transaction",
                withdrawalUuid = null,
                createdAt = LocalDateTime.now(ZoneOffset.UTC),
            ),
        )
    }

    private fun count(sql: String, vararg args: Any): Long =
        jdbcTemplate.queryForObject(sql, Long::class.java, *args)
            ?: error("Count query returned null: $sql")

    private fun assertDecimalEquals(expected: BigDecimal, actual: BigDecimal) {
        assertEquals(0, expected.compareTo(actual), "Expected $expected but was $actual")
    }

    private fun stableOperationId(ownerUuid: String, key: String): String =
        UUID.nameUUIDFromBytes("$ownerUuid:${key.trim()}".toByteArray(StandardCharsets.UTF_8)).toString()

    private companion object {
        const val CURRENCY_SYMBOL = "SBXTEST"
        const val FUNDING_OWNER_UUID = "sandbox-postgres-funding-owner"
        const val DISPOSABLE_DATABASE_SENTINEL = "cex_wallet_disposable_20261016"
        val AMOUNT = BigDecimal("1")

        @JvmStatic
        @DynamicPropertySource
        fun isolatedPostgresProperties(registry: DynamicPropertyRegistry) {
            val url = System.getenv("OPEX_TEST_POSTGRES")
                ?: throw IllegalStateException(
                    "SandboxWalletPostgresIT requires OPEX_TEST_POSTGRES; the test is skipped when it is unset.",
                )
            val options = ConnectionFactoryOptions.parse(url)
            val database = options.getRequiredValue(ConnectionFactoryOptions.DATABASE).toString()
            val host = options.getRequiredValue(ConnectionFactoryOptions.HOST).toString()
            val port = options.getValue(ConnectionFactoryOptions.PORT)?.toString() ?: "5432"
            val username = options.getRequiredValue(ConnectionFactoryOptions.USER).toString()
            val password = options.getRequiredValue(ConnectionFactoryOptions.PASSWORD).toString()
            val jdbcUrl = "jdbc:postgresql://$host:$port/$database"

            if (database == "opex") {
                check(System.getenv("OPEX_TEST_DISPOSABLE_DATABASE") == "true") {
                    "SandboxWalletPostgresIT only allows database 'opex' when OPEX_TEST_DISPOSABLE_DATABASE=true."
                }
                val databaseComment = DriverManager.getConnection(jdbcUrl, username, password).use { connection ->
                    connection.prepareStatement(
                        "SELECT current_database() AS name, shobj_description(oid, 'pg_database') AS comment " +
                            "FROM pg_database WHERE datname = current_database()",
                    ).use { statement ->
                        statement.executeQuery().use { result ->
                            check(result.next()) { "Could not verify the disposable database comment." }
                            check(result.getString("name") == "opex") {
                                "SandboxWalletPostgresIT connected to a different database than requested."
                            }
                            result.getString("comment")
                        }
                    }
                }
                check(databaseComment == DISPOSABLE_DATABASE_SENTINEL) {
                    "SandboxWalletPostgresIT refused database 'opex'; expected COMMENT '$DISPOSABLE_DATABASE_SENTINEL'."
                }
            } else {
                check(database.endsWith("_test")) {
                    "SandboxWalletPostgresIT refused database '$database'; OPEX_TEST_POSTGRES must use a *_test database."
                }
            }

            registry.add("spring.r2dbc.url") { url }
            registry.add("spring.r2dbc.username") { username }
            registry.add("spring.r2dbc.password") { password }
            registry.add("spring.datasource.url") { jdbcUrl }
            registry.add("spring.datasource.username") { username }
            registry.add("spring.datasource.password") { password }
            registry.add("spring.flyway.url") { jdbcUrl }
            registry.add("spring.flyway.user") { username }
            registry.add("spring.flyway.password") { password }
            registry.add("DB_IP_PORT") { "$host:$port" }
            registry.add("DB_NAME") { database }
            registry.add("dbusername") { username }
            registry.add("dbpassword") { password }
        }
    }
}
