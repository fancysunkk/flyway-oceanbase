package com.github.fancysunkk.flyway.database.oceanbase;

import org.flywaydb.core.internal.util.StringUtils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Sunk
 * @version 1.0
 */
public class OceanBaseJdbcUtils {
    public static String getVersionComment(Connection connection) throws SQLException {
        return queryVariable(connection, "version_comment");
    }

    public static String getVersionNumber(Connection connection) throws SQLException {
        String versionComment = getVersionComment(connection);
        if (StringUtils.hasText(versionComment)) {
            String[] parts = versionComment.split(" ");
            if (parts.length > 1) {
                return parts[1];
            }
        }
        return null;
    }

    private static String queryVariable(Connection connection, String variable) throws SQLException {
        assert StringUtils.hasText(variable);
        String sql = String.format("SHOW VARIABLES LIKE '%s'", variable);
        try (Statement statement = connection.createStatement()) {
            ResultSet rs = statement.executeQuery(sql);
            if (rs.next()) {
                return rs.getString("VALUE");
            }
        }
        return null;
    }
}
