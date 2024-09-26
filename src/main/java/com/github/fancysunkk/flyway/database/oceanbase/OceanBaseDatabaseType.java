package com.github.fancysunkk.flyway.database.oceanbase;

import org.flywaydb.core.api.configuration.Configuration;
import org.flywaydb.core.internal.database.base.Database;
import org.flywaydb.core.internal.database.mysql.MySQLDatabaseType;
import org.flywaydb.core.internal.jdbc.JdbcConnectionFactory;
import org.flywaydb.core.internal.jdbc.StatementInterceptor;
import org.flywaydb.core.internal.util.ClassUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

/**
 * @author Sunk
 * @version 1.0
 */
public class OceanBaseDatabaseType extends MySQLDatabaseType  {
    private static final String OB_JDBC_DRIVER = "com.oceanbase.jdbc.Driver";
    private static final String OB_LEGACY_JDBC_DRIVER = "com.alipay.oceanbase.jdbc.Driver";

    public String getName() {
        return "OceanBase";
    }

    @Override
    public int getPriority() {
        // OceanBase needs to be checked in advance of MySql
        return 1;
    }

    @Override
    public int getNullType() {
        return Types.VARCHAR;
    }

    @Override
    public boolean handlesJDBCUrl(String url) {
        return url.startsWith("jdbc:mysql:") || url.startsWith("jdbc:oceanbase:");
    }

    @Override
    public String getDriverClass(String url, ClassLoader classLoader) {
        return url.startsWith("jdbc:oceanbase:")?
                OB_JDBC_DRIVER :
                super.getDriverClass(url, classLoader);
    }

    @Override
    public String getBackupDriverClass(String url, ClassLoader classLoader) {
        return (url.startsWith("jdbc:oceanbase:") && ClassUtils.isPresent(OB_LEGACY_JDBC_DRIVER, classLoader)?
                OB_LEGACY_JDBC_DRIVER :
                super.getBackupDriverClass(url, classLoader));
    }

    @Override
    public boolean handlesDatabaseProductNameAndVersion(String databaseProductName, String databaseProductVersion, Connection connection) {
        if (!databaseProductName.contains("MySQL") && !databaseProductName.contains("OceanBase")) {
            return false;
        }

        String versionComment;
        try {
            versionComment = OceanBaseJdbcUtils.getVersionComment(connection);
        } catch (SQLException e) {
            return false;
        }
        return versionComment != null && versionComment.contains("OceanBase");
    }

    @Override
    public Database createDatabase(Configuration configuration, JdbcConnectionFactory jdbcConnectionFactory, StatementInterceptor statementInterceptor) {
        return new OceanBaseDatabase(configuration, jdbcConnectionFactory, statementInterceptor);
    }

}
