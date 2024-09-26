package com.github.fancysunkk.flyway.database;

import com.github.fancysunkk.flyway.database.oceanbase.FileUtils;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.extensibility.PluginMetadata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * @author Sunk
 * @version 1.0
 */
public class OceanBaseDatabaseExtension implements PluginMetadata {
    public String getDescription() {
        return "Community-contributed OceanBase database support extension " + readVersion() + " by Redgate";
    }

    public static String readVersion() {
        try {
            return FileUtils.copyToString(
                    OceanBaseDatabaseExtension.class.getClassLoader().getResourceAsStream("org/flywaydb/community/database/oceanbase/version.txt"),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new FlywayException("Unable to read extension version: " + e.getMessage(), e);
        }
    }
}
