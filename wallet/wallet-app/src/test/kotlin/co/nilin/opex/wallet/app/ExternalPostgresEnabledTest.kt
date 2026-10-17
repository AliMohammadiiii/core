package co.nilin.opex.wallet.app

import com.zaxxer.hikari.HikariDataSource
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Profile
import org.springframework.data.r2dbc.repository.config.EnableR2dbcRepositories
import org.springframework.jdbc.core.JdbcTemplate
import javax.sql.DataSource

/**
 * Enables the application's real R2DBC repositories when an integration test
 * supplies its own PostgreSQL connection properties instead of using
 * [PostgresEnabledTest]'s Testcontainers fixture.
 */
@TestConfiguration
@Profile("test & external-postgres")
@EnableR2dbcRepositories(basePackages = ["co.nilin.opex"])
class ExternalPostgresEnabledTest {
    @Bean
    fun externalPostgresDataSource(
        @Value("\${spring.datasource.url}") jdbcUrl: String,
        @Value("\${spring.datasource.username}") username: String,
        @Value("\${spring.datasource.password}") password: String,
    ): DataSource = HikariDataSource().apply {
        this.jdbcUrl = jdbcUrl
        this.username = username
        this.password = password
    }

    @Bean
    fun externalPostgresJdbcTemplate(dataSource: DataSource): JdbcTemplate = JdbcTemplate(dataSource)
}
