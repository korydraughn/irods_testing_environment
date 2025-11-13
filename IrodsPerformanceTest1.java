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
 * iRODS Performance Test - Optimized for large files (100MB+)
 */
public class IrodsPerformanceTest1 {

    private static final int TEST_RUNS = 3;
    private static final String TEST_FILES_DIR = "/home/mmuramoto/testfiles";
    private static final boolean ENABLE_COMPRESSION = false;
    private static final String RESULTS_DIR = "./performance_results";
    
    // Optimized buffer settings for large files
    private static final int BUFFER_SIZE = 256 * 1024; // 256 KB - larger buffer for throughput
    private static final long PROGRESS_UPDATE_INTERVAL_MS = 500;
    private static final int PROGRESS_BAR_WIDTH = 50;
    
    // Filter out problem files
    private static final String[] EXCLUDE_PATTERNS = {".Identifier", ".DS_Store", "Thumbs.db"};

    private static final String RED = "\033[0;31m", GREEN = "\033[0;32m",
                                YELLOW = "\033[1;33m", CYAN = "\033[0;36m", 
                                BLUE = "\033[0;34m", NC = "\033[0m";

    public static void main(String[] args) {
        log(GREEN, "=".repeat(80));
        log(GREEN, "iRODS Performance Test - Large File Optimized");
        log(GREEN, "=".repeat(80));

        try {
            Config config = loadConfig();
            List<File> testFiles = getTestFiles();

            log(CYAN, String.format("\nFound %d test file(s):", testFiles.size()));
            testFiles.forEach(f -> System.out.println("  - " + f.getName() + " (" + formatSize(f.length()) + ")"));
            
            long totalSize = testFiles.stream().mapToLong(File::length).sum();
            System.out.println("  Total size: " + formatSize(totalSize));

            IRODSConnection conn = connect(config);
            RcComm rcComm = conn.getRcComm();

            log(GREEN, "✓ Connected as " + config.username + "@" + config.zone);
            verifyConnection(rcComm, config);

            List<Result> results = runTests(rcComm, config, testFiles);
            
            if (results.isEmpty()) {
                log(RED, "\nNo successful tests completed!");
            } else {
                String outputFile = saveResults(config, results);
                displaySummary(results, outputFile);
            }

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

        log(YELLOW, String.format("\nRunning %d test iterations (sequential for large files)...", TEST_RUNS));

        for (int run = 1; run <= TEST_RUNS; run++) {
            log(CYAN, String.format("\n--- Run %d/%d ---", run, TEST_RUNS));
            
            for (File testFile : testFiles) {
                try {
                    System.out.println(); // Space before each test
                    Result result = performTest(rcComm, config, testFile, run);
                    results.add(result);
                    
                    log(GREEN, String.format("✓ Run %d/%d [%s]: Upload %.2fs (%.2f MB/s), Download %.2fs (%.2f MB/s)", 
                        run, TEST_RUNS, truncate(testFile.getName(), 30), 
                        result.uploadTime, result.uploadThroughput(), 
                        result.downloadTime, result.downloadThroughput()));
                        
                } catch (Exception e) {
                    log(RED, String.format("✗ Run %d/%d [%s]: FAILED - %s", 
                        run, TEST_RUNS, testFile.getName(), e.getMessage()));
                    e.printStackTrace();
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

        // Only compress small files
        if (ENABLE_COMPRESSION && originalSize < 50 * 1024 * 1024) {
            log(YELLOW, "  Compressing...");
            long start = System.nanoTime();
            uploadFile = compress(testFile);
            compressTime = (System.nanoTime() - start) / 1e9;
            log(GREEN, String.format("  ✓ Compressed: %s -> %s (%.1f%% reduction)", 
                formatSize(originalSize), formatSize(uploadFile.length()),
                (1.0 - (double)uploadFile.length()/originalSize) * 100));
        }

        long transferSize = uploadFile.length();
        String irodsPath = config.homeDir + "/benchmark_" + filename + "_" + System.currentTimeMillis();

        // Upload with progress and flush
        System.out.println(CYAN + "  Uploading " + filename + " (" + formatSize(transferSize) + ")..." + NC);
        long uploadStart = System.nanoTime();
        
        try (FileInputStream in = new FileInputStream(uploadFile);
             IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(rcComm, irodsPath, false, false)) {
            copyWithProgress(in, out, transferSize, "Upload");
            out.flush(); // Ensure all data is sent
        }
        
        double uploadTime = (System.nanoTime() - uploadStart) / 1e9;
        clearProgressLine();

        // Small delay to let server finish processing
        Thread.sleep(100);

        // Download with progress
        System.out.println(CYAN + "  Downloading " + filename + "..." + NC);
        File downloadFile = File.createTempFile("irods_dl_", ".tmp");
        long downloadStart = System.nanoTime();
        
        try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(rcComm, irodsPath);
             FileOutputStream out = new FileOutputStream(downloadFile)) {
            copyWithProgress(in, out, transferSize, "Download");
            out.flush();
        }
        
        double downloadTime = (System.nanoTime() - downloadStart) / 1e9;
        clearProgressLine();

        // Verify file sizes match
        long downloadedSize = downloadFile.length();
        if (downloadedSize != transferSize) {
            throw new IOException(String.format("Size mismatch! Expected %d bytes, got %d bytes", 
                transferSize, downloadedSize));
        }

        // Decompression
        double decompressTime = 0;
        if (ENABLE_COMPRESSION && uploadFile != testFile) {
            log(YELLOW, "  Decompressing...");
            long start = System.nanoTime();
            File decompressed = decompress(downloadFile);
            decompressTime = (System.nanoTime() - start) / 1e9;
            
            // Verify original size
            if (decompressed.length() != originalSize) {
                throw new IOException(String.format("Decompressed size mismatch! Expected %d bytes, got %d bytes",
                    originalSize, decompressed.length()));
            }
            decompressed.delete();
        }

        // Cleanup
        downloadFile.delete();
        if (ENABLE_COMPRESSION && uploadFile != testFile) {
            uploadFile.delete();
        }
        
        // Remove from iRODS
        try {
            IRODSFilesystem.remove(rcComm, irodsPath);
        } catch (Exception e) {
            log(YELLOW, "  Warning: Could not remove " + irodsPath + ": " + e.getMessage());
        }

        return new Result(run, filename, originalSize, transferSize, uploadTime, downloadTime, compressTime, decompressTime);
    }

    private static void copyWithProgress(InputStream in, OutputStream out, long totalSize, String operation) throws IOException {
        byte[] buffer = new byte[BUFFER_SIZE];
        long totalRead = 0;
        int len;
        long lastUpdate = System.currentTimeMillis();
        long startTime = System.currentTimeMillis();
        int stuckCounter = 0;
        long lastTotalRead = 0;

        while ((len = in.read(buffer)) != -1) {
            out.write(buffer, 0, len);
            totalRead += len;

            long now = System.currentTimeMillis();
            if (now - lastUpdate >= PROGRESS_UPDATE_INTERVAL_MS || totalRead == totalSize) {
                double progress = (double) totalRead / totalSize;
                double elapsedSeconds = (now - startTime) / 1000.0;
                double speed = elapsedSeconds > 0 ? (totalRead / 1024.0 / 1024.0) / elapsedSeconds : 0;
                
                // Check if transfer is stuck
                if (totalRead == lastTotalRead) {
                    stuckCounter++;
                    if (stuckCounter > 10) { // Stuck for 5+ seconds
                        throw new IOException("Transfer appears stuck - no progress for 5 seconds");
                    }
                } else {
                    stuckCounter = 0;
                }
                lastTotalRead = totalRead;
                
                printProgress(operation, progress, totalRead, totalSize, speed);
                lastUpdate = now;
            }
        }
        
        // Final progress update
        if (totalRead > 0) {
            double elapsedSeconds = (System.currentTimeMillis() - startTime) / 1000.0;
            double speed = elapsedSeconds > 0 ? (totalRead / 1024.0 / 1024.0) / elapsedSeconds : 0;
            printProgress(operation, 1.0, totalRead, totalSize, speed);
        }
    }

    private static void printProgress(String operation, double progress, long current, long total, double speedMBps) {
        int progressChars = (int) (PROGRESS_BAR_WIDTH * progress);
        int remaining = PROGRESS_BAR_WIDTH - progressChars;

        StringBuilder bar = new StringBuilder();
        bar.append("\r  ").append(operation).append(": [");
        bar.append(GREEN).append("=".repeat(Math.max(0, progressChars)));
        if (remaining > 0 && progress < 1.0) {
            bar.append(">").append(" ".repeat(Math.max(0, remaining - 1)));
        } else if (remaining > 0) {
            bar.append(" ".repeat(remaining));
        }
        bar.append(NC).append("] ");
        bar.append(String.format("%.1f%% ", progress * 100));
        bar.append(String.format("(%s / %s) ", formatSize(current), formatSize(total)));
        bar.append(String.format("%.2f MB/s", speedMBps));
        
        // Add spaces to clear any leftover characters
        bar.append("    ");

        System.out.print(bar.toString());
        System.out.flush();
    }

    private static void clearProgressLine() {
        System.out.print("\r" + " ".repeat(120) + "\r");
        System.out.flush();
    }

    private static File compress(File input) throws IOException {
        File output = File.createTempFile("irods_gz_", ".gz");
        try (FileInputStream in = new FileInputStream(input);
             GZIPOutputStream out = new GZIPOutputStream(new FileOutputStream(output))) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
        }
        return output;
    }

    private static File decompress(File input) throws IOException {
        File output = File.createTempFile("irods_decomp_", ".tmp");
        try (GZIPInputStream in = new GZIPInputStream(new FileInputStream(input));
             FileOutputStream out = new FileOutputStream(output)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int len;
            while ((len = in.read(buffer)) != -1) {
                out.write(buffer, 0, len);
            }
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
            out.flush();
        }
        Thread.sleep(100);
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
        config.homeDir = props.getProperty("irods_home", "/" + config.zone + "/home/" + config.username);
        config.password = "usfusf";
        return config;
    }

    private static List<File> getTestFiles() {
        File dir = new File(TEST_FILES_DIR);
        if (!dir.exists()) throw new RuntimeException("Test files directory not found: " + TEST_FILES_DIR);

        List<File> files = Arrays.stream(dir.listFiles())
            .filter(File::isFile)
            .filter(f -> {
                // Exclude problematic files
                for (String pattern : EXCLUDE_PATTERNS) {
                    if (f.getName().contains(pattern)) {
                        return false;
                    }
                }
                return true;
            })
            .sorted(Comparator.comparingLong(File::length)) // Start with smallest files
            .collect(Collectors.toList());
            
        if (files.isEmpty()) throw new RuntimeException("No files found in " + TEST_FILES_DIR);
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
            w.println("Buffer size: " + formatSize(BUFFER_SIZE));
            w.println("Total runs: " + results.size());
            w.println();

            double avgUpload = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
            double avgDownload = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
            double avgUpTput = results.stream().mapToDouble(Result::uploadThroughput).average().orElse(0);
            double avgDownTput = results.stream().mapToDouble(Result::downloadThroughput).average().orElse(0);

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
                w.printf("%3d | %-30s | %10s | %7.2fs | %7.2fs | %7.2f | %7.2f%n",
                        r.run, truncate(r.filename, 30), formatSize(r.originalSize),
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
        double avgUpTput = results.stream().mapToDouble(Result::uploadThroughput).average().orElse(0);
        double avgDownTput = results.stream().mapToDouble(Result::downloadThroughput).average().orElse(0);

        System.out.println("Successful runs: " + results.size());
        System.out.printf("Average upload:   %.2fs (%.2f MB/s)%n", avgUpload, avgUpTput);
        System.out.printf("Average download: %.2fs (%.2f MB/s)%n", avgDownload, avgDownTput);
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
        return s.length() > len ? s.substring(0, len - 3) + "..." : s;
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
