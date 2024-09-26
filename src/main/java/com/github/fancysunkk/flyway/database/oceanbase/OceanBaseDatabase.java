package com.github.fancysunkk.flyway.database.oceanbase;

import lombok.CustomLog;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.api.MigrationType;
import org.flywaydb.core.api.MigrationVersion;
import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.base.Table;
import org.flywaydb.core.internal.database.mysql.MySQLDatabaseType;
import org.flywaydb.core.internal.database.mysql.mariadb.MariaDBDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.JdbcTemplate;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author Sunk
 * @version 1.0
 */
@CustomLog
public class OceanBaseDatabase extends Database<OceanBaseConnection> {

    private static final Pattern MYSQL_VERSION_PATTERN = Pattern.compile("(\\d+\\.\\d+)\\.\\d+\\w*");

    private final boolean pxcStrict;

    private final boolean gtidConsistencyEnforced;

    final boolean eventSchedulerQueryable;

    public OceanBaseDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        super(configuration, jdbcConnectionFactory, statementInterceptor);

        JdbcTemplate jdbcTemplate = new JdbcTemplate(rawMainJdbcConnection, databaseType);
        pxcStrict = isMySQL() && isRunningInPerconaXtraDBClusterWithStrictMode(jdbcTemplate);
        gtidConsistencyEnforced = isMySQL() && isRunningInGTIDConsistencyMode(jdbcTemplate);
        eventSchedulerQueryable = isMySQL() || isEventSchedulerQueryable(jdbcTemplate);
    }


    boolean isMariaDB() {
        return databaseType instanceof MariaDBDatabaseType;
    }
    boolean isMySQL() {
        return databaseType instanceof MySQLDatabaseType;
    }

    private static boolean isEventSchedulerQueryable(JdbcTemplate jdbcTemplate) {
        try {
            // Attempt query
            jdbcTemplate.queryForString("SELECT event_name FROM information_schema.events LIMIT 1");
            return true;
        } catch (SQLException e) {
            LOG.debug("Detected unqueryable MariaDB event scheduler, most likely due to it being OFF or DISABLED.");
            return false;
        }
    }
    static boolean isRunningInPerconaXtraDBClusterWithStrictMode(JdbcTemplate jdbcTemplate) {
        try {
            String pcx_strict_mode = jdbcTemplate.queryForString(
                    "select VARIABLE_VALUE from performance_schema.global_variables"
                            + " where variable_name = 'pxc_strict_mode'");
            if ("ENFORCING".equals(pcx_strict_mode) || "MASTER".equals(pcx_strict_mode)) {
                LOG.debug("Detected Percona XtraDB Cluster in strict mode");
                return true;
            }
        } catch (SQLException e) {
            LOG.debug("Unable to detect whether we are running in a Percona XtraDB Cluster. Assuming not to be.");
        }

        return false;
    }

    static boolean isRunningInGTIDConsistencyMode(JdbcTemplate jdbcTemplate) {
        try {
            String gtidConsistency = jdbcTemplate.queryForString("SELECT @@GLOBAL.ENFORCE_GTID_CONSISTENCY");
            if ("ON".equals(gtidConsistency)) {
                LOG.debug("Detected GTID consistency being enforced");
                return true;
            }
        } catch (SQLException e) {
            LOG.debug("Unable to detect whether database enforces GTID consistency. Assuming not.");
        }

        return false;
    }

    @Override
    public String getRawCreateScript(Table table, boolean baseline) {
        String tablespace =


                configuration.getTablespace() == null
                        ? ""
                        : " TABLESPACE \"" + configuration.getTablespace() + "\"";

        String baselineMarker = "";
        if (baseline) {
            if (isCreateTableAsSelectAllowed()) {
                baselineMarker = " AS SELECT" +
                        "     1 as \"installed_rank\"," +
                        "     '" + configuration.getBaselineVersion() + "' as \"version\"," +
                        "     '" + configuration.getBaselineDescription() + "' as \"description\"," +
                        "     '" + MigrationType.BASELINE + "' as \"type\"," +
                        "     '" + configuration.getBaselineDescription() + "' as \"script\"," +
                        "     NULL as \"checksum\"," +
                        "     '" + getInstalledBy() + "' as \"installed_by\"," +
                        "     CURRENT_TIMESTAMP as \"installed_on\"," +
                        "     0 as \"execution_time\"," +
                        "     TRUE as \"success\"\n";
            } else {
                // Revert to regular insert, which unfortunately is not safe in concurrent scenarios
                // due to MySQL implicit commits after DDL statements.
                baselineMarker = ";\n" + getBaselineStatement(table);
            }
        }

        return "CREATE TABLE " + table + " (\n" +
                "    `installed_rank` INT NOT NULL,\n" +
                "    `version` VARCHAR(50),\n" +
                "    `description` VARCHAR(200) NOT NULL,\n" +
                "    `type` VARCHAR(20) NOT NULL,\n" +
                "    `script` VARCHAR(1000) NOT NULL,\n" +
                "    `checksum` INT,\n" +
                "    `installed_by` VARCHAR(100) NOT NULL,\n" +
                "    `installed_on` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n" +
                "    `execution_time` INT NOT NULL,\n" +
                "    `success` BOOL NOT NULL,\n" +
                "    CONSTRAINT " + getConstraintName(table.getName()) + " PRIMARY KEY (`installed_rank`)\n" +
                ")" + tablespace + " ENGINE=InnoDB" +
                baselineMarker +
                ";\n" +
                "CREATE INDEX `" + table.getName() + "_s_idx` ON " + table + " (`success`);";
    }

    protected String getConstraintName(String tableName) {
        return "`" + tableName + "_pk`";
    }
    private static MigrationVersion extractVersionFromString(String versionString, Pattern... patterns) {
        for (Pattern pattern : patterns) {
            Matcher matcher = pattern.matcher(versionString);
            if (matcher.find()) {
                return MigrationVersion.fromVersion(matcher.group(1));
            }
        }
        throw new FlywayException("Unable to determine version from '" + versionString + "'");
    }
    @Override
    protected String doGetCurrentUser() throws SQLException {
        return getMainConnection().getJdbcTemplate().queryForString("SELECT SUBSTRING_INDEX(USER(),'@',1)");
    }
    @Override
    public boolean supportsDdlTransactions() {
        return false;
    }

    @Override
    public boolean supportsChangingCurrentSchema() {
        return true;
    }

    @Override
    public String getBooleanTrue() {
        return "1";
    }

    @Override
    public String getBooleanFalse() {
        return "0";
    }

    @Override
    public String doQuote(String identifier) {
        return "`" + identifier + "`";
    }

    @Override
    public boolean catalogIsSchema() {
        return true;
    }

    @Override
    public boolean useSingleConnection() {
        return !pxcStrict;
    }

    @Override
    protected OceanBaseConnection doGetConnection(Connection connection) {
        return new OceanBaseConnection(this, connection);
    }


    protected boolean isCreateTableAsSelectAllowed() {
        return true;
    }

    @Override
    public void ensureSupported() {
        ensureDatabaseIsRecentEnough("1.4");
        recommendFlywayUpgradeIfNecessary("5.0");
    }

    @Override
    protected MigrationVersion determineVersion() {
        String versionNumber;
        try {
            versionNumber = OceanBaseJdbcUtils.getVersionNumber(rawMainJdbcConnection);
        } catch (SQLException e) {
            throw new FlywayException("Failed to get version number", e);
        }
        return MigrationVersion.fromVersion(versionNumber);
    }
}
