package com.github.fancysunkk.flyway.database.oceanbase;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.flywaydb.core.api.FlywayException;
import org.flywaydb.core.internal.util.IOUtils;
import org.flywaydb.core.internal.util.StringUtils;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class FileUtils {
    public static String getFilename(String path) {
        if (StringUtils.hasText(path)) {
            return path.substring(path.replace("/", "\\").lastIndexOf("\\") + 1);
        } else {
            return "";
        }
    }

    /**
     * Copy the contents of the given Reader into a String.
     * Closes the reader when done.
     *
     * @param in the reader to copy from
     *
     * @return the String that has been copied to
     *
     * @throws IOException in case of I/O errors
     */
    public static String copyToString(Reader in) throws IOException {
        StringWriter out = new StringWriter();
        copy(in, out);
        String str = out.toString();

        //Strip UTF-8 BOM if necessary
        if (str.startsWith("\ufeff")) {
            return str.substring(1);
        }

        return str;
    }

    /**
     * Copy the contents of the given InputStream into a new String based on this encoding.
     * Closes the stream when done.
     *
     * @param in       the stream to copy from
     * @param encoding The encoding to use.
     *
     * @return The new String.
     *
     * @throws IOException in case of I/O errors
     */
    public static String copyToString(InputStream in, Charset encoding) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream(4096);
        copy(in, out);
        return out.toString(encoding.name());
    }

    /**
     * Copy the contents of the given Reader to the given Writer.
     * Closes both when done.
     *
     * @param in  the Reader to copy from
     * @param out the Writer to copy to
     *
     * @throws IOException in case of I/O errors
     */
    public static void copy(Reader in, Writer out) throws IOException {
        try {
            char[] buffer = new char[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
            out.flush();
        } finally {
            IOUtils.close(in);
            IOUtils.close(out);
        }
    }

    /**
     * Copy the contents of the given InputStream to the given OutputStream.
     * Closes both streams when done.
     *
     * @param in  the stream to copy from
     * @param out the stream to copy to
     *
     * @return the number of bytes copied
     *
     * @throws IOException in case of I/O errors
     */
    public static int copy(InputStream in, OutputStream out) throws IOException {
        try {
            int byteCount = 0;
            byte[] buffer = new byte[4096];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
                byteCount += bytesRead;
            }
            out.flush();
            return byteCount;
        } finally {
            IOUtils.close(in);
            IOUtils.close(out);
        }
    }

    private static String readAsString(Path path, Charset charset) {
        try {
            return String.join(System.lineSeparator(), Files.readAllLines(path.toAbsolutePath(), charset));
        } catch (IOException ioe) {
            throw new FlywayException("Unable to read " + path.toAbsolutePath() + " from disk", ioe);
        }
    }

    public static String readAsString(Path path) {
        return readAsString(path, StandardCharsets.UTF_8);
    }

    public static String readResourceAsString(String path) {
        return readResourceAsString(FileUtils.class.getClassLoader(), path);
    }

    public static String readResourceAsString(ClassLoader classLoader, String path) {
        try (InputStream inputStream = classLoader.getResourceAsStream(path);
             BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {

            String result = "";
            while (reader.ready()) {
                result += reader.readLine() + System.lineSeparator();
            }
            return result;
        } catch (IOException ioe) {
            throw new FlywayException("Unable to read " + path + " from resources", ioe);
        }
    }

    public static String readAsStringFallbackToResource(String parent, String path) {
        try {
            return readAsString(Paths.get(parent,path), Charset.defaultCharset()) + System.lineSeparator();
        } catch (FlywayException fe) {
            return readResourceAsString(path);
        }
    }

    public static void createDirIfNotExists(File file) {

        File dir = file.getParentFile();

        if (dir == null || dir.exists()) {
            throw new UnsupportedOperationException("No need to create report file directory");
        }

        if (!dir.mkdirs()) {
            throw new FlywayException("Unable to creat report file directory");
        }
    }

    public static File getAppDataLocation() {
        boolean isWindows = System.getProperty("os.name").toLowerCase(Locale.ENGLISH).contains("win");
        return isWindows ? new File(System.getenv("APPDATA"), "Redgate") : new File(System.getProperty("user.home"), ".config/Redgate");
    }

    public static File getAppDataFlywayCLILocation() {
        File redgateAppData = getAppDataLocation();
        return new File(redgateAppData, "Flyway CLI");
    }

    public static void writeToFile(File file, String content) {
        try (FileWriter fileWriter = new FileWriter(file)) {
            fileWriter.write(content);
        } catch (IOException e) {
            throw new FlywayException("Unable to write to " + file.getAbsolutePath(), e);
        }
    }
}