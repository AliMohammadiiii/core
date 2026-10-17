package co.nilin.opex.wallet.app

import co.nilin.opex.wallet.app.service.CustodyLedgerService
import co.nilin.opex.wallet.core.inout.CurrencyCommand
import co.nilin.opex.wallet.core.model.Amount
import co.nilin.opex.wallet.core.model.FetchCurrency
import co.nilin.opex.wallet.core.model.WalletOwner
import co.nilin.opex.wallet.core.model.WalletType
import co.nilin.opex.wallet.core.spi.CurrencyServiceManager
import co.nilin.opex.wallet.core.spi.WalletManager
import co.nilin.opex.wallet.core.spi.WalletOwnerManager
import io.r2dbc.spi.ConnectionFactoryOptions
import org.flywaydb.core.Flyway
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import java.math.BigDecimal
import java.sql.DriverManager

/**
 * Safety and one-time opening-state support for the isolated custody fixture.
 * This lives under src/test and is never included in the wallet application artifact.
 */
internal object CustodyLedgerFixtureSupport {
    // Legacy wallet migrations use a three-part `opex.public.*` qualifier and
    // PostgreSQL accepts it only when the current database is also `opex`.
    const val DATABASE_NAME = "opex"
    const val DATABASE_COMMENT = "cex_custody_ledger_fixture_20261017"
    private const val DATABASE_MARKER_TABLE = "public.custody_fixture_marker"
    const val FIXTURE_ID = "custody-local-evm-genesis-v1"
    const val TREASURY_OWNER_UUID = "00000000-0000-4000-8000-000000000017"
    const val USER_OWNER_UUID = "00000000-0000-4000-8000-000000000018"
    const val ASSET = "LOCAL_EVM_ETH"
    val OPENING_AMOUNT: BigDecimal = System.getenv("OPEX_CUSTODY_FIXTURE_OPENING_TREASURY_AMOUNT")
        ?.let { BigDecimal(it) }
        ?: BigDecimal("100")

    init {
        require(OPENING_AMOUNT.compareTo(BigDecimal.ZERO) > 0) {
            "OPEX_CUSTODY_FIXTURE_OPENING_TREASURY_AMOUNT must be greater than zero."
        }
    }

    private const val GENESIS_TABLE = "custody_test_fixture_opening_state"
    private const val GENESIS_PROVENANCE =
        "TEST ONLY finite local-chain fixture opening state; not an external deposit or user-fund credit"

    internal data class DatabaseSettings(
        val r2dbcUrl: String,
        val jdbcUrl: String,
        val host: String,
        val port: String,
        val database: String,
        val username: String,
        val password: String,
    )

    fun settingsFromEnvironment(): DatabaseSettings {
        val r2dbcUrl = System.getenv("OPEX_TEST_POSTGRES")
            ?: error("Set OPEX_TEST_POSTGRES to the dedicated custody fixture database R2DBC URL")
        val options = ConnectionFactoryOptions.parse(r2dbcUrl)
        val host = options.getRequiredValue(ConnectionFactoryOptions.HOST).toString()
        val port = options.getValue(ConnectionFactoryOptions.PORT)?.toString() ?: "5432"
        val database = options.getRequiredValue(ConnectionFactoryOptions.DATABASE).toString()
        val username = options.getRequiredValue(ConnectionFactoryOptions.USER).toString()
        val password = options.getValue(ConnectionFactoryOptions.PASSWORD)?.toString().orEmpty()

        check(database == DATABASE_NAME) {
            "Custody fixture refuses database '$database'; expected the dedicated '$DATABASE_NAME' database."
        }

        val settings = DatabaseSettings(
            r2dbcUrl = r2dbcUrl,
            jdbcUrl = "jdbc:postgresql://$host:$port/$database",
            host = host,
            port = port,
            database = database,
            username = username,
            password = password,
        )
        verifyDatabaseMarker(settings)
        return settings
    }

    fun verifyDatabaseMarker(settings: DatabaseSettings) {
        DriverManager.getConnection(settings.jdbcUrl, settings.username, settings.password).use { connection ->
            connection.prepareStatement(
                "SELECT current_database() AS name, " +
                    "shobj_description(oid, 'pg_database') AS comment " +
                    "FROM pg_database WHERE datname = current_database()",
            ).use { statement ->
                statement.executeQuery().use { result ->
                    check(result.next()) { "Custody fixture could not verify its PostgreSQL database marker." }
                    check(result.getString("name") == DATABASE_NAME) {
                        "Custody fixture connected to a different PostgreSQL database."
                    }
                    check(result.getString("comment") == DATABASE_COMMENT) {
                        "Custody fixture refused a database without the '$DATABASE_COMMENT' marker."
                    }
                }
            }
            connection.prepareStatement(
                "SELECT marker FROM $DATABASE_MARKER_TABLE WHERE marker = ?",
            ).use { statement ->
                statement.setString(1, DATABASE_COMMENT)
                statement.executeQuery().use { result ->
                    check(result.next()) {
                        "Custody fixture refused a database without the custody_fixture_marker row."
                    }
                }
            }
        }
    }

    fun migrateFixtureDatabase(settings: DatabaseSettings) {
        verifyDatabaseMarker(settings)
        Flyway.configure()
            .dataSource(settings.jdbcUrl, settings.username, settings.password)
            .locations("classpath:db/migration")
            .baselineOnMigrate(true)
            .baselineVersion("1")
            .load()
            .migrate()
    }

    fun bindSpringProperties(registry: DynamicPropertyRegistry, settings: DatabaseSettings) {
        registry.add("spring.r2dbc.url") { settings.r2dbcUrl }
        registry.add("spring.r2dbc.username") { settings.username }
        registry.add("spring.r2dbc.password") { settings.password }
        registry.add("spring.datasource.url") { settings.jdbcUrl }
        registry.add("spring.datasource.username") { settings.username }
        registry.add("spring.datasource.password") { settings.password }
        registry.add("spring.flyway.enabled") { false }
        registry.add("DB_IP_PORT") { "${settings.host}:${settings.port}" }
        registry.add("DB_NAME") { settings.database }
        registry.add("dbusername") { settings.username }
        registry.add("dbpassword") { settings.password }
    }

    fun springProperties(settings: DatabaseSettings, ledgerKey: String): Map<String, Any> = mapOf(
        "spring.r2dbc.url" to settings.r2dbcUrl,
        "spring.r2dbc.username" to settings.username,
        "spring.r2dbc.password" to settings.password,
        "spring.datasource.url" to settings.jdbcUrl,
        "spring.datasource.username" to settings.username,
        "spring.datasource.password" to settings.password,
        "spring.flyway.enabled" to false,
        "spring.main.web-application-type" to "none",
        "spring.kafka.bootstrap-servers" to "127.0.0.1:1",
        "spring.kafka.listener.auto-startup" to false,
        "spring.kafka.admin.auto-create" to false,
        "app.symbols" to "",
        "app.wallet.custody.ledger.enabled" to true,
        "app.wallet.custody.ledger.bootstrap-enabled" to true,
        "app.wallet.custody.ledger.key" to ledgerKey,
        "app.wallet.custody.ledger.treasury-owner-uuid" to TREASURY_OWNER_UUID,
    )

    suspend fun prepareNativeFixture(
        custodyService: CustodyLedgerService,
        currencyServiceManager: CurrencyServiceManager,
        walletOwnerManager: WalletOwnerManager,
        walletManager: WalletManager,
        jdbcTemplate: JdbcTemplate,
    ) {
        custodyService.bootstrapAssets()
        ensureZeroBalanceFixtureUser(currencyServiceManager, walletOwnerManager, walletManager)
        seedTreasuryOpeningState(currencyServiceManager, walletOwnerManager, walletManager, jdbcTemplate)
    }

    private suspend fun ensureZeroBalanceFixtureUser(
        currencyServiceManager: CurrencyServiceManager,
        walletOwnerManager: WalletOwnerManager,
        walletManager: WalletManager,
    ) {
        val existingOwner = walletOwnerManager.findWalletOwner(USER_OWNER_UUID)
        val owner = existingOwner
            ?: walletOwnerManager.createWalletOwner(USER_OWNER_UUID, "TEST custody fixture user", "1")
        val currency = currencyServiceManager.fetchCurrency(FetchCurrency(symbol = ASSET))
            ?: error("Custody fixture bootstrap did not provision $ASSET")
        val wallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            ?: walletManager.createWallet(owner, Amount(currency, BigDecimal.ZERO), currency, WalletType.MAIN)
        if (existingOwner == null) check(wallet.balance.amount.compareTo(BigDecimal.ZERO) == 0) {
            "Custody fixture user wallet must start at zero; it is never reset on restart."
        }
    }

    private suspend fun seedTreasuryOpeningState(
        currencyServiceManager: CurrencyServiceManager,
        walletOwnerManager: WalletOwnerManager,
        walletManager: WalletManager,
        jdbcTemplate: JdbcTemplate,
    ) {
        jdbcTemplate.execute(
            """
            CREATE TABLE IF NOT EXISTS $GENESIS_TABLE (
                fixture_id VARCHAR(80) PRIMARY KEY,
                owner_uuid VARCHAR(36) NOT NULL,
                asset VARCHAR(25) NOT NULL,
                amount NUMERIC(78, 18) NOT NULL CHECK (amount > 0),
                provenance TEXT NOT NULL,
                status VARCHAR(16) NOT NULL CHECK (status IN ('PREPARED', 'APPLIED')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """.trimIndent(),
        )

        val owner = walletOwnerManager.findWalletOwner(TREASURY_OWNER_UUID)
            ?: error("Custody fixture bootstrap did not provision its treasury owner")
        val currency: CurrencyCommand = currencyServiceManager.fetchCurrency(FetchCurrency(symbol = ASSET))
            ?: error("Custody fixture bootstrap did not provision $ASSET")
        val wallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
            ?: error("Custody fixture bootstrap did not provision the treasury wallet")

        val inserted = jdbcTemplate.update(
            """
            INSERT INTO $GENESIS_TABLE (fixture_id, owner_uuid, asset, amount, provenance, status)
            VALUES (?, ?, ?, ?, ?, 'PREPARED')
            ON CONFLICT (fixture_id) DO NOTHING
            """.trimIndent(),
            FIXTURE_ID,
            TREASURY_OWNER_UUID,
            ASSET,
            OPENING_AMOUNT,
            GENESIS_PROVENANCE,
        )
        val existing = jdbcTemplate.queryForList(
            "SELECT owner_uuid, asset, amount, provenance, status FROM $GENESIS_TABLE WHERE fixture_id = ?",
            FIXTURE_ID,
        ).singleOrNull() ?: error("Custody fixture opening-state receipt disappeared")
        check(existing["owner_uuid"] == TREASURY_OWNER_UUID && existing["asset"] == ASSET) {
            "Custody fixture opening-state record does not match the configured treasury asset."
        }
        check((existing["amount"] as BigDecimal).compareTo(OPENING_AMOUNT) == 0) {
            "Custody fixture opening-state amount changed; refusing implicit reseeding."
        }
        check(existing["provenance"] == GENESIS_PROVENANCE) {
            "Custody fixture opening-state provenance does not match this fixture."
        }

        when {
            existing["status"] == "APPLIED" -> return
            inserted == 0 -> {
                // A prior process reserved the one-time seed but did not finish it. Recover only
                // the exact expected opening balance; never add another balance on restart.
                when (wallet.balance.amount.compareTo(OPENING_AMOUNT)) {
                    0 -> jdbcTemplate.update(
                        "UPDATE $GENESIS_TABLE SET status = 'APPLIED' WHERE fixture_id = ? AND status = 'PREPARED'",
                        FIXTURE_ID,
                    )
                    else -> error(
                        "Custody fixture opening-state seed was interrupted; refusing automatic reseeding. " +
                            "Create a fresh dedicated fixture database after reviewing its provenance."
                    )
                }
            }
            else -> {
                check(wallet.balance.amount.compareTo(BigDecimal.ZERO) == 0) {
                    "Treasury has a balance but no fixture opening-state record; refusing to relabel it."
                }

                // This is a test-only opening balance for an isolated local-chain fixture, not a deposit.
                walletManager.increaseBalance(wallet, OPENING_AMOUNT)
                val updatedWallet = walletManager.findWalletByOwnerAndCurrencyAndType(owner, WalletType.MAIN, currency)
                    ?: error("Custody fixture treasury wallet disappeared while recording opening state")
                check(updatedWallet.balance.amount.compareTo(OPENING_AMOUNT) == 0) {
                    "Custody fixture opening balance did not persist exactly once."
                }
                check(
                    jdbcTemplate.update(
                        "UPDATE $GENESIS_TABLE SET status = 'APPLIED' WHERE fixture_id = ? AND status = 'PREPARED'",
                        FIXTURE_ID,
                    ) == 1,
                ) { "Custody fixture opening-state receipt could not be finalized" }
            }
        }
    }
}
