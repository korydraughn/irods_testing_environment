import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.authentication.NativeAuthPlugin;

import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.*;

/**
 * iRODS Performance Test - Using actual irods4j API
 */
public class IrodsPerformanceTest2 {

    private static final int TEST_RUNS = 25;
    private static final String TEST_FILES_DIR = "C:\\Users\\maxxm\\OneDrive\\Desktop\\testfiles";
    private static final boolean ENABLE_COMPRESSION = false;
    private static final String RESULTS_DIR = "./performance_results";
    private static final int BUFFER_SIZE = 64 * 1024;

    private static final String RED = "\033[0;31m", GREEN = "\033[0;32m", 
                                YELLOW = "\033[1;33m", CYAN = "\033[0;36m", NC = "\033[0m";

    public static void main(String[] args) {
        log(GREEN, "=".repeat(80));
        log(GREEN, "iRODS Performance Test");
        log(GREEN, "=".repeat(80));

        try {
            Config config = loadConfig();
            List<File> testFiles = getTestFiles();
            
            log(CYAN, String.format("\nFound %d test file(s):", testFiles.size()));
            testFiles.forEach(f -> System.out.println("  - " + f.getName() + " (" + formatSize(f.length()) + ")"));

            IRODSConnection conn = connect(config);
            RcComm rcComm = conn.getRcComm();
            
            log(GREEN, "✓ Connected as " + config.username + "@" + config.zone);
            verifyConnection(rcComm, config);
            
            List<Result> results = runTests(rcComm, config, testFiles);
            String outputFile = saveResults(config, results);
            
            displaySummary(results, outputFile);
            
            conn.disconnect();
            
        } catch (Exception e) {
            log(RED, "Error: " + e.getMessage());
            e.printStackTrace();
        }

        log(GREEN, "\n" + "=".repeat(80));
        log(GREEN, "Test completed!");
        log(GREEN, "=".repeat(80));
    }

    private static List<Result> runTests(RcComm rcComm, Config config, List<File> testFiles) {
        List<Result> results = new ArrayList<>();

        log(YELLOW, String.format("\nRunning %d test iterations...", TEST_RUNS));

        for (int run = 1; run <= TEST_RUNS; run++) {
            for (File testFile : testFiles) {
                try {
                    Result result = performTest(rcComm, config, testFile, run);
                    results.add(result);
                    log(GREEN, String.format("  Run %d/%d [%s]: Upload %.3fs, Download %.3fs", 
                        run, TEST_RUNS, testFile.getName(), result.uploadTime, result.downloadTime));
                } catch (Exception e) {
                    log(RED, String.format("  Run %d/%d [%s]: FAILED - %s", 
                        run, TEST_RUNS, testFile.getName(), e.getMessage()));
                }
            }
        }
        return results;
    }

    private static Result performTest(RcComm rcComm, Config config, File testFile, int run) throws Exception {
        String filename = testFile.getName();
        long originalSize = testFile.length();
        
        File uploadFile = testFile;
        double compressTime = 0;
        if (ENABLE_COMPRESSION) {
            long start = System.nanoTime();
            uploadFile = compress(testFile);
            compressTime = (System.nanoTime() - start) / 1e9;
        }

        long transferSize = uploadFile.length();
        String irodsPath = config.homeDir + "/benchmark_" + filename + "_" + System.currentTimeMillis();

        // Upload
        long uploadStart = System.nanoTime();
        try (FileInputStream in = new FileInputStream(uploadFile);
             IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(rcComm, irodsPath, false, false)) {
            copy(in, out);
        }
        double uploadTime = (System.nanoTime() - uploadStart) / 1e9;

        // Download
        File downloadFile = File.createTempFile("irods_dl", ".tmp");
        long downloadStart = System.nanoTime();
        try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(rcComm, irodsPath);
             FileOutputStream out = new FileOutputStream(downloadFile)) {
            copy(in, out);
        }
        double downloadTime = (System.nanoTime() - downloadStart) / 1e9;

        // Decompression
        double decompressTime = 0;
        if (ENABLE_COMPRESSION) {
            long start = System.nanoTime();
            File decompressed = decompress(downloadFile);
            decompressTime = (System.nanoTime() - start) / 1e9;
            decompressed.delete();
        }

        // Cleanup
        downloadFile.delete();
        if (ENABLE_COMPRESSION && !uploadFile.equals(testFile)) {
            uploadFile.delete();
        }
        IRODSFilesystem.remove(rcComm, irodsPath);

        return new Result(run, filename, originalSize, transferSize, 
                         uploadTime, downloadTime, compressTime, decompressTime);
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        int len;
        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
        }
    }

    private static File compress(File input) throws IOException {
        File output = File.createTempFile("irods_gz", ".gz");
        try (FileInputStream in = new FileInputStream(input);
             GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(output))) {
            copy(in, out);
        }
        return output;
    }

    private static File decompress(File input) throws IOException {
        File output = File.createTempFile("irods_decomp", ".tmp");
        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(input));
             FileOutputStream out = new FileOutputStream(output)) {
            copy(in, out);
        }
        return output;
    }

    private static IRODSConnection connect(Config config) throws Exception {
        IRODSConnection conn = new IRODSConnection();
        QualifiedUsername quser = new QualifiedUsername(config.username, config.zone);
        conn.connect(config.host, config.port, quser);
        conn.authenticate(new NativeAuthPlugin(), config.password);
        return conn;
    }

    private static void verifyConnection(RcComm rcComm, Config config) throws Exception {
        log(YELLOW, "\nVerifying connection...");
        
        String testPath = config.homeDir + "/.test_" + System.currentTimeMillis();
        
        try (IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(rcComm, testPath, false, false)) {
            out.write("test".getBytes());
        }
        
        IRODSFilesystem.remove(rcComm, testPath);
        log(GREEN, "✓ Connection verified");
    }

    private static Config loadConfig() throws IOException {
        String envFile = System.getenv().getOrDefault("IRODS_ENVIRONMENT_FILE",
            System.getProperty("user.home") + "/.irods/irods_environment.json");
        
        Properties props = new Properties();
        String content = new String(Files.readAllBytes(Paths.get(envFile)))
            .replaceAll("[{}\"]", "").replaceAll("\\s+", "");
        
        for (String line : content.split(",")) {
            if (line.contains(":")) {
                String[] parts = line.split(":", 2);
                props.setProperty(parts[0], parts[1]);
            }
        }
        
        Config config = new Config();
        config.host = props.getProperty("irods_host");
        config.port = Integer.parseInt(props.getProperty("irods_port", "1247"));
        config.username = props.getProperty("irods_user_name");
        config.zone = props.getProperty("irods_zone_name");
        config.homeDir = props.getProperty("irods_home", 
            "/" + config.zone + "/home/" + config.username);
        config.password = readPassword();
        
        return config;
    }

    private static String readPassword() throws IOException {
        String authFile = System.getProperty("user.home") + "/.irods/.irodsA";
        if (new File(authFile).exists()) {
            return new String(Files.readAllBytes(Paths.get(authFile))).trim();
        }
        Console console = System.console();
        if (console != null) {
            return new String(console.readPassword("Enter iRODS password: "));
        }
        throw new IOException("Cannot read password");
    }

    private static List<File> getTestFiles() {
        File dir = new File(TEST_FILES_DIR);
        if (!dir.exists()) {
            throw new RuntimeException("Test files directory not found: " + TEST_FILES_DIR);
        }
        
        List<File> files = Arrays.stream(dir.listFiles())
            .filter(File::isFile)
            .collect(Collectors.toList());
        
        if (files.isEmpty()) {
            throw new RuntimeException("No files found in " + TEST_FILES_DIR);
        }
        return files;
    }

    private static String saveResults(Config config, List<Result> results) throws IOException {
        new File(RESULTS_DIR).mkdirs();
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        String filename = String.format("%s/irods4j_benchmark_%s_%s.txt", 
            RESULTS_DIR, ENABLE_COMPRESSION ? "compressed" : "uncompressed", timestamp);

        try (PrintWriter w = new PrintWriter(filename)) {
            w.println("iRODS Performance Test Results");
            w.println("=".repeat(80));
            w.println("Date: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()));
            w.println("User: " + config.username + "@" + config.zone);
            w.println("Host: " + config.host + ":" + config.port);
            w.println("Compression: " + (ENABLE_COMPRESSION ? "ENABLED" : "DISABLED"));
            w.println("Total runs: " + results.size());
            w.println();

            double avgUpload = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
            double avgDownload = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
            double avgUpTput = results.stream().mapToDouble(r -> r.uploadThroughput()).average().orElse(0);
            double avgDownTput = results.stream().mapToDouble(r -> r.downloadThroughput()).average().orElse(0);

            w.println("Summary:");
            w.println("-".repeat(80));
            w.printf("Average upload:   %.3fs (%.2f MB/s)%n", avgUpload, avgUpTput);
            w.printf("Average download: %.3fs (%.2f MB/s)%n", avgDownload, avgDownTput);
            w.println();

            w.println("Detailed Results:");
            w.println("-".repeat(80));
            w.println("Run | File | Size | Upload | Download | U-Tput | D-Tput");
            w.println("-".repeat(80));
            
            for (Result r : results) {
                w.printf("%3d | %-20s | %10s | %6.3fs | %6.3fs | %7.2f | %7.2f%n",
                    r.run, truncate(r.filename, 20), formatSize(r.originalSize),
                    r.uploadTime, r.downloadTime, r.uploadThroughput(), r.downloadThroughput());
            }
        }
        return filename;
    }

    private static void displaySummary(List<Result> results, String outputFile) {
        System.out.println();
        log(GREEN, "=".repeat(80));
        log(GREEN, "Test Summary");
        log(GREEN, "=".repeat(80));
        
        double avgUpload = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
        double avgDownload = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
        double avgUpTput = results.stream().mapToDouble(r -> r.uploadThroughput()).average().orElse(0);
        double avgDownTput = results.stream().mapToDouble(r -> r.downloadThroughput()).average().orElse(0);
        
        System.out.println("Successful runs: " + results.size());
        System.out.printf("Average upload:   %.3fs (%.2f MB/s)%n", avgUpload, avgUpTput);
        System.out.printf("Average download: %.3fs (%.2f MB/s)%n", avgDownload, avgDownTput);
        log(GREEN, "\nResults saved to: " + outputFile);
    }

    private static void log(String color, String message) {
        System.out.println(color + message + NC);
    }

    private static String formatSize(long bytes) {
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        double size = bytes;
        int i = 0;
        while (size >= 1024 && i < units.length - 1) {
            size /= 1024;
            i++;
        }
        return String.format("%.2f %s", size, units[i]);
    }

    private static String truncate(String s, int len) {
        return s.length() > len ? s.substring(0, len) : s;
    }

    static class Config {
        String host, username, password, zone, homeDir;
        int port;
    }

    static class Result {
        int run;
        String filename;
        long originalSize, transferSize;
        double uploadTime, downloadTime, compressTime, decompressTime;

        Result(int run, String filename, long originalSize, long transferSize,
               double uploadTime, double downloadTime, double compressTime, double decompressTime) {
            this.run = run;
            this.filename = filename;
            this.originalSize = originalSize;
            this.transferSize = transferSize;
            this.uploadTime = uploadTime;
            this.downloadTime = downloadTime;
            this.compressTime = compressTime;
            this.decompressTime = decompressTime;
        }

        double uploadThroughput() {
            return (transferSize / 1024.0 / 1024.0) / uploadTime;
        }

        double downloadThroughput() {
            return (transferSize / 1024.0 / 1024.0) / downloadTime;
        }
    }
}
