package co.nilin.opex.wallet.app.service

import co.nilin.opex.wallet.app.ExternalPostgresEnabledTest
import co.nilin.opex.wallet.app.CustodyLedgerFixtureSupport
import co.nilin.opex.wallet.app.dto.CustodyLedgerActionRequest
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.cloud.stream.binder.test.TestChannelBinderConfiguration
import org.springframework.context.annotation.Import
import org.springframework.http.HttpStatus
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.springframework.test.context.TestPropertySource
import org.springframework.web.server.ResponseStatusException
import java.math.BigDecimal
import java.util.UUID

@TestPropertySource(
    properties = [
        "app.wallet.custody.ledger.enabled=true",
        "app.wallet.custody.ledger.bootstrap-enabled=true",
        "app.wallet.custody.ledger.key=integration-test-custody-key",
        "app.wallet.custody.ledger.treasury-owner-uuid=${CustodyLedgerFixtureSupport.TREASURY_OWNER_UUID}",
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
class CustodyLedgerPostgresIT {
    @Autowired
    private lateinit var service: CustodyLedgerService

    @Autowired
    private lateinit var walletOwnerManager: WalletOwnerManager

    @Autowired
    private lateinit var walletManager: WalletManager

    @Autowired
    private lateinit var currencyService: CurrencyServiceManager

    @Autowired
    private lateinit var actionRepository: CustodyLedgerActionRepository

    @Autowired
    private lateinit var jdbcTemplate: JdbcTemplate

    private lateinit var treasuryOwner: WalletOwner
    private lateinit var localCurrency: CurrencyCommand

    @BeforeEach
    fun prepareExplicitPostgresFixture() = runBlocking {
        CustodyLedgerFixtureSupport.prepareNativeFixture(
            custodyService = service,
            currencyServiceManager = currencyService,
            walletOwnerManager = walletOwnerManager,
            walletManager = walletManager,
            jdbcTemplate = jdbcTemplate,
        )
        treasuryOwner = walletOwnerManager.findWalletOwner(TEST_TREASURY_OWNER_UUID)
            ?: error("Additive test bootstrap did not create the configured treasury owner")
        localCurrency = currencyService.fetchCurrency(FetchCurrency(symbol = CustodyLedgerService.LOCAL_ASSET))
            ?: error("Additive test bootstrap did not create LOCAL_EVM_ETH")
    }

    @Test
    fun `bootstrap is additive idempotent and never funds the treasury`() = runBlocking {
        val owner = walletOwnerManager.findWalletOwner(TEST_TREASURY_OWNER_UUID)!!
        val before = mainBalance(owner, localCurrency)

        val result = service.bootstrapAssets()
        val after = mainBalance(owner, localCurrency)

        assertEquals(TEST_TREASURY_OWNER_UUID, result.treasuryOwnerUuid)
        assertEquals(listOf("LOCAL_EVM_ETH", "SEPOLIA_ETH"), result.assets)
        assertTrue(result.createdCurrencies.isEmpty())
        assertTrue(result.createdWallets.isEmpty())
        assertFalse(result.balancesFunded)
        assertDecimalEquals(before, after)
        assertEquals(BigDecimal("18"), localCurrency.precision)
        assertFalse(localCurrency.isTransitive == true)
        assertEquals(0L, transactionCount("wallet:custody-ledger:bootstrap"))
    }

    @Test
    fun `credit uses native transfer history and replays the same durable receipt`() = runBlocking {
        val owner = createOwner()
        val treasuryBefore = mainBalance(treasuryOwner, localCurrency)
        val operationId = uuid()
        val request = request(
            operationId = operationId,
            ownerUuid = owner.uuid,
            asset = CustodyLedgerService.LOCAL_ASSET,
            amount = "1.25",
            kind = CustodyLedgerActionKind.CREDIT,
            referenceOperationId = uuid(),
        )

        val applied = service.apply(request)
        val replay = service.apply(request)

        assertEquals("APPLIED", applied.status)
        assertEquals(applied.transactionId, replay.transactionId)
        assertEquals(applied.payloadHash, replay.payloadHash)
        assertDecimalEquals(BigDecimal("1.25"), mainBalance(owner, localCurrency))
        assertDecimalEquals(treasuryBefore.subtract(BigDecimal("1.25")), mainBalance(treasuryOwner, localCurrency))
        assertEquals(1L, transactionCount(transferRef(operationId)))
        assertEquals(2L, userTransactionCount(transferRef(operationId)))
        assertEquals("APPLIED", service.getAction(operationId).status)
    }

    @Test
    fun `same operation id with a different payload is rejected without another transfer`() = runBlocking {
        val owner = createOwner()
        val operationId = uuid()
        val depositUuid = uuid()
        val first = request(operationId, owner.uuid, CustodyLedgerService.LOCAL_ASSET, "2", CustodyLedgerActionKind.CREDIT, depositUuid)
        service.apply(first)
        val balanceAfterFirst = mainBalance(owner, localCurrency)
        val conflicting = request(operationId, owner.uuid, CustodyLedgerService.LOCAL_ASSET, "3", CustodyLedgerActionKind.CREDIT, depositUuid)

        val failure = try {
            service.apply(conflicting)
            error("Expected operation id conflict")
        } catch (expected: ResponseStatusException) {
            expected
        }

        assertEquals(HttpStatus.CONFLICT, failure.status)
        assertDecimalEquals(balanceAfterFirst, mainBalance(owner, localCurrency))
        assertEquals(1L, transactionCount(transferRef(operationId)))
    }

    @Test
    fun `insufficient treasury credit is non-applied and retryable with the same operation id`() = runBlocking {
        val owner = createOwner()
        val operationId = uuid()
        val request = request(
            operationId,
            owner.uuid,
            CustodyLedgerService.LOCAL_ASSET,
            "999999999999999999999999999999999999999999999999",
            CustodyLedgerActionKind.CREDIT,
            uuid(),
        )

        val insufficient = try {
            service.apply(request)
            error("Expected an insufficient-funds result")
        } catch (failure: CustodyLedgerInsufficientFundsException) {
            failure
        }

        assertEquals(request.operationId, insufficient.operationId)
        assertEquals(request.payloadHash, insufficient.payloadHash)
        assertEquals(0L, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM custody_ledger_actions WHERE operation_id = ?",
            Long::class.java,
            operationId,
        ))
        assertEquals(0L, transactionCount(transferRef(operationId)))

        val retry = try {
            service.apply(request)
            error("Expected an insufficient-funds result when the treasury opening state is unchanged")
        } catch (failure: CustodyLedgerInsufficientFundsException) {
            failure
        }
        assertEquals(request.operationId, retry.operationId)
        assertEquals(0L, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM custody_ledger_actions WHERE operation_id = ?",
            Long::class.java,
            operationId,
        ))
        assertEquals(0L, transactionCount(transferRef(operationId)))
    }

    @Test
    fun `concurrent reserves cannot overspend one owner balance`() = runBlocking {
        val owner = createOwner()
        service.apply(
            request(uuid(), owner.uuid, CustodyLedgerService.LOCAL_ASSET, "5", CustodyLedgerActionKind.CREDIT, uuid()),
        )
        walletManager.createWallet(owner, Amount(localCurrency, BigDecimal.ZERO), localCurrency, WalletType.CASHOUT)

        val attempts = coroutineScope {
            listOf(uuid(), uuid()).map { operationId ->
                async(Dispatchers.IO) {
                    runCatching {
                        service.apply(
                            request(operationId, owner.uuid, CustodyLedgerService.LOCAL_ASSET, "4", CustodyLedgerActionKind.RESERVE),
                        )
                    }
                }
            }.awaitAll()
        }

        assertEquals(1, attempts.count { it.isSuccess })
        assertEquals(1, attempts.count { it.isFailure })
        val balance = service.balance(owner.uuid, CustodyLedgerService.LOCAL_ASSET)
        assertDecimalEquals(BigDecimal("1"), BigDecimal(balance.available))
        assertDecimalEquals(BigDecimal("4"), BigDecimal(balance.reserved))
        assertEquals(1L, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM custody_ledger_actions WHERE owner_uuid = ? AND kind = 'RESERVE'",
            Long::class.java,
            owner.uuid,
        ))
    }

    @Test
    fun `release and settle race closes one reserve exactly once`() = runBlocking {
        val owner = createOwner()
        service.apply(
            request(uuid(), owner.uuid, CustodyLedgerService.LOCAL_ASSET, "5", CustodyLedgerActionKind.CREDIT, uuid()),
        )
        val reserveId = uuid()
        service.apply(
            request(reserveId, owner.uuid, CustodyLedgerService.LOCAL_ASSET, "3", CustodyLedgerActionKind.RESERVE),
        )
        val release = request(uuid(), owner.uuid, CustodyLedgerService.LOCAL_ASSET, "3", CustodyLedgerActionKind.RELEASE, reserveId)
        val settle = request(uuid(), owner.uuid, CustodyLedgerService.LOCAL_ASSET, "3", CustodyLedgerActionKind.SETTLE, reserveId)

        val attempts = coroutineScope {
            listOf(release, settle).map { action ->
                async(Dispatchers.IO) { runCatching { service.apply(action) } }
            }.awaitAll()
        }

        assertEquals(1, attempts.count { it.isSuccess })
        assertEquals(1, attempts.count { it.isFailure })
        val root = actionRepository.findById(reserveId).block()!!
        assertTrue(root.terminalKind == "RELEASE" || root.terminalKind == "SETTLE")
        assertEquals(1L, jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM custody_ledger_actions WHERE reference_operation_id = ? AND kind IN ('RELEASE', 'SETTLE')",
            Long::class.java,
            reserveId,
        ))
        val finalBalance = service.balance(owner.uuid, CustodyLedgerService.LOCAL_ASSET)
        if (root.terminalKind == "RELEASE") {
            assertDecimalEquals(BigDecimal("5"), BigDecimal(finalBalance.total))
        } else {
            assertDecimalEquals(BigDecimal("2"), BigDecimal(finalBalance.total))
            assertEquals(1L, transactionCount(transferRef(root.terminalOperationId!!)))
        }
    }

    private suspend fun createOwner(): WalletOwner = walletOwnerManager.createWalletOwner(
        uuid = uuid(),
        title = "Custody ledger integration owner",
        userLevel = "1",
    )

    private fun request(
        operationId: String,
        ownerUuid: String,
        asset: String,
        amount: String,
        kind: CustodyLedgerActionKind,
        referenceOperationId: String? = null,
    ): CustodyLedgerActionRequest {
        val normalizedAmount = BigDecimal(amount)
        val normalizedOperationId = UUID.fromString(operationId).toString()
        val normalizedOwnerUuid = UUID.fromString(ownerUuid).toString()
        val normalizedReference = referenceOperationId?.let { UUID.fromString(it).toString() }
        return CustodyLedgerActionRequest(
            operationId = normalizedOperationId,
            ownerUuid = normalizedOwnerUuid,
            asset = asset,
            amount = amount,
            kind = kind,
            referenceOperationId = normalizedReference,
            payloadHash = CustodyLedgerPayloadHash.calculate(
                normalizedOperationId,
                normalizedOwnerUuid,
                asset,
                normalizedAmount,
                kind,
                normalizedReference,
            ),
        )
    }

    private suspend fun mainBalance(owner: WalletOwner, currency: CurrencyCommand): BigDecimal =
        walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)?.balance?.amount
            ?: BigDecimal.ZERO

    private fun transactionCount(reference: String): Long = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM transaction WHERE transfer_ref = ?",
        Long::class.java,
        reference,
    ) ?: 0L

    private fun userTransactionCount(reference: String): Long = jdbcTemplate.queryForObject(
        "SELECT COUNT(*) FROM user_transaction ut JOIN transaction t ON t.id = ut.tx_id WHERE t.transfer_ref = ?",
        Long::class.java,
        reference,
    ) ?: 0L

    private fun assertDecimalEquals(expected: BigDecimal, actual: BigDecimal) {
        assertEquals(0, expected.compareTo(actual), "expected $expected but got $actual")
    }

    private fun transferRef(operationId: String) = "wallet:custody-ledger:$operationId"

    companion object {
        fun uuid() = UUID.randomUUID().toString()

        @JvmStatic
        @DynamicPropertySource
        fun isolatedPostgresProperties(registry: DynamicPropertyRegistry) {
            val settings = CustodyLedgerFixtureSupport.settingsFromEnvironment()
            CustodyLedgerFixtureSupport.migrateFixtureDatabase(settings)
            CustodyLedgerFixtureSupport.bindSpringProperties(registry, settings)
        }
    }
}

private const val TEST_TREASURY_OWNER_UUID = CustodyLedgerFixtureSupport.TREASURY_OWNER_UUID
