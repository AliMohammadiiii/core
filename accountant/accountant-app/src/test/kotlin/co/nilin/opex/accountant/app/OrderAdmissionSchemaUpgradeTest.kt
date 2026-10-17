package co.nilin.opex.accountant.app

import io.r2dbc.spi.Connection
import io.r2dbc.spi.ConnectionFactories
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID

/** Runs the real additive schema statements against a legacy-shaped table in an isolated schema. */
@EnabledIfEnvironmentVariable(
    named = "OPEX_TEST_POSTGRES",
    matches = "^r2dbc:postgresql://[^/]+/opex_test(?:\\?.*)?$",
)
class OrderAdmissionSchemaUpgradeTest {
    private val testUrl = requireNotNull(System.getenv("OPEX_TEST_POSTGRES"))

    @Test
    fun `existing order admission rows upgrade additively and retain publication history`() = runBlocking {
        val factory = ConnectionFactories.get(testUrl)
        val schema = "admission_upgrade_${UUID.randomUUID().toString().replace("-", "")}"
        val connection = Mono.from(factory.create()).awaitSingle()
        try {
            val databaseNameResult = Mono.from(
                connection.createStatement("SELECT current_database() AS name").execute(),
            ).awaitSingle()
            val databaseName = Mono.from(
                databaseNameResult.map { row, _ -> row.get("name", String::class.java)!! },
            ).awaitSingle()
            assertEquals("opex_test", databaseName, "refusing to touch a database other than opex_test")

            execute(connection, "CREATE SCHEMA $schema")
            execute(connection, "SET search_path TO $schema")
            execute(
                connection,
                """CREATE TABLE order_admission (
                   ouid VARCHAR(72) PRIMARY KEY,
                   payload TEXT NOT NULL,
                   reserve_uuid VARCHAR(72) NOT NULL UNIQUE,
                   published_at TIMESTAMPTZ,
                   created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )""",
            )
            execute(
                connection,
                """INSERT INTO order_admission(ouid,payload,reserve_uuid,published_at)
                   VALUES ('legacy-ouid','{}','legacy-reserve','2026-10-01T00:00:00Z')""",
            )

            val sourceSql = loadSchemaSql()
            val upgradeStatements = sourceSql.split(';').map(String::trim).filter { statement ->
                statement.startsWith("ALTER TABLE order_admission ADD COLUMN IF NOT EXISTS", ignoreCase = true) ||
                    statement.startsWith("UPDATE order_admission SET publication_started_at", ignoreCase = true) ||
            statement.startsWith("CREATE TABLE IF NOT EXISTS order_admission_commands", ignoreCase = true) ||
            statement.startsWith("CREATE TABLE IF NOT EXISTS order_admission_status_outbox", ignoreCase = true) ||
            statement.startsWith("CREATE INDEX IF NOT EXISTS idx_order_admission_conditional_ouid", ignoreCase = true) ||
            statement.startsWith("CREATE INDEX IF NOT EXISTS order_admission_status_outbox_pending", ignoreCase = true)
            }
            assertTrue(upgradeStatements.isNotEmpty(), "the production schema must contain additive admission upgrades")

            // Re-running the additive changes models repeatable startup/schema application.
            upgradeStatements.forEach { execute(connection, it) }
            upgradeStatements.forEach { execute(connection, it) }

            val columnsResult = Mono.from(connection.createStatement(
                """SELECT column_name FROM information_schema.columns
                   WHERE table_schema='$schema' AND table_name='order_admission'""",
            ).execute()).awaitSingle()
            val columns = Flux.from(columnsResult.map { row, _ -> row.get("column_name", String::class.java)!! })
                .collectList().awaitSingle().toSet()
            assertTrue(
                columns.containsAll(
                    setOf(
                        "conditional",
                        "activation_requested",
                        "cancel_requested",
                        "publication_started_at",
                        "dispatch_attempted_at",
                        "cancel_sent_at",
                        "reconciliation_required",
                    ),
                ),
                "all new admission state must be present on an already-existing table",
            )

            val backfillResult = Mono.from(connection.createStatement(
                """SELECT publication_started_at IS NOT NULL AS was_backfilled
                   FROM order_admission WHERE ouid='legacy-ouid'""",
            ).execute()).awaitSingle()
            val backfilled = Flux.from(backfillResult.map { row, _ -> row.get("was_backfilled", java.lang.Boolean::class.java)!!.booleanValue() })
                .collectList().awaitSingle()
            assertEquals(listOf(true), backfilled, "published legacy rows must be marked as potentially dispatched")

            val commandTableResult = Mono.from(connection.createStatement(
                """SELECT to_regclass('$schema.order_admission_commands') IS NOT NULL AS present""",
            ).execute()).awaitSingle()
            val commandTableExists = Flux.from(commandTableResult.map { row, _ -> row.get("present", java.lang.Boolean::class.java)!!.booleanValue() })
                .collectList().awaitSingle()
            assertEquals(listOf(true), commandTableExists)

        val statusOutboxResult = Mono.from(connection.createStatement(
            "SELECT to_regclass('$schema.order_admission_status_outbox') IS NOT NULL AS present",
        ).execute()).awaitSingle()
        val statusOutboxExists = Flux.from(statusOutboxResult.map { row, _ -> row.get("present", java.lang.Boolean::class.java)!!.booleanValue() })
            .collectList().awaitSingle()
        assertEquals(listOf(true), statusOutboxExists)
        } finally {
            Mono.from(connection.close()).awaitSingleOrNull()
            // The generated schema is isolated; only remove the schema created by this test.
            val cleanupConnection = Mono.from(factory.create()).awaitSingle()
            try {
                execute(cleanupConnection, "DROP SCHEMA IF EXISTS $schema CASCADE")
            } finally {
                Mono.from(cleanupConnection.close()).awaitSingleOrNull()
            }
        }
    }

    private suspend fun execute(connection: Connection, sql: String) {
        Flux.from(connection.createStatement(sql).execute()).then().awaitSingleOrNull()
    }

    private fun loadSchemaSql(): String {
        val candidates = listOf(
            Path.of("../accountant-ports/accountant-persister-postgres/src/main/resources/schema.sql"),
            Path.of("accountant/accountant-ports/accountant-persister-postgres/src/main/resources/schema.sql"),
        )
        val path = candidates.firstOrNull(Files::isRegularFile)
            ?: error("could not locate accountant-persister-postgres schema.sql")
        return Files.readString(path)
    }
}
