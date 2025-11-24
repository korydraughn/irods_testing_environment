import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.high_level.io.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.io.IRODSDataObjectOutputStream;
import org.irods.irods4j.high_level.vfs.IRODSFilesystem;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.authentication.NativeAuthPlugin;

import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;

/**
 * iRODS Performance Test - Adaptive Compression with Zstandard
 */
public class IrodsPerformanceTest2 {

    // ==================== CONFIGURATION ====================
    private static final int TEST_RUNS = 5;
    private static final int MAX_THREADS = 4;
    
    // Buffer size - Optimized for Zstd streaming and iRODS TCP packets
    private static final int BUFFER_SIZE = 2 * 1024 * 1024; // 1 MB (optimal for Zstd + high latency)
    
    // Compression settings
    private static final boolean ENABLE_ADAPTIVE = true;  // Set to false for manual compression level
    private static final boolean ENABLE_COMPRESSION = false;
    private static final int MANUAL_COMPRESSION_LEVEL = 3; // Used when ENABLE_ADAPTIVE = false (1=fastest, 22=best)
    
    private static final String RESULTS_DIR = "./performance_results";
    private static final String TEST_FILES_DIR = "/home/mmuramoto/testfiles";
    // =======================================================

    private static final String RED = "\033[0;31m", GREEN = "\033[0;32m",
            YELLOW = "\033[1;33m", CYAN = "\033[0;36m", NC = "\033[0m";

    private static int compressionLevel = 3; // Will be set by adaptive or manual setting

    public static void main(String[] args) {
        log(GREEN, "=".repeat(80));
        log(GREEN, "iRODS Performance Test - Adaptive Zstandard Compression");
        log(GREEN, "Optimized for Cross-Country Transfers (CA → NC)");
        log(GREEN, "=".repeat(80));

        System.out.println("\n📊 Configuration:");
        System.out.println("  Buffer size: " + formatSize(BUFFER_SIZE) + " (optimized for Zstd streaming + high latency)");
        System.out.println("  Compression: " + (ENABLE_COMPRESSION ? "Zstandard (adaptive)" : "DISABLED"));
        System.out.println("  Adaptive mode: " + (ENABLE_ADAPTIVE ? "ENABLED" : "MANUAL level " + MANUAL_COMPRESSION_LEVEL));

        try {
            Config config = loadConfig();
            List<File> testFiles = getTestFiles();

            log(CYAN, String.format("\nFound %d test file(s):", testFiles.size()));
            long totalSize = 0;
            for (File f : testFiles) {
                System.out.println("  - " + f.getName() + " (" + formatSize(f.length()) + ")");
                totalSize += f.length();
            }
            System.out.println("  Total size: " + formatSize(totalSize));

            IRODSConnection conn = connect(config);
            RcComm rcComm = conn.getRcComm();
            log(GREEN, "✓ Connected as " + config.username + "@" + config.zone);
            verifyConnection(rcComm, config);

            // --- Network speed test ---
            NetworkTestResult netTest = null;
            if (ENABLE_ADAPTIVE || !ENABLE_COMPRESSION) {
                netTest = testNetworkSpeed(rcComm, config, 5, 2);
            }

            // --- Adaptive compression ---
            if (ENABLE_ADAPTIVE && ENABLE_COMPRESSION && netTest != null) {
                compressionLevel = selectCompressionLevel(netTest);
            } else if (ENABLE_COMPRESSION) {
                compressionLevel = MANUAL_COMPRESSION_LEVEL;
                log(CYAN, String.format("\nUsing manual compression level: %d", compressionLevel));
            } else {
                log(YELLOW, "\nCompression DISABLED - transferring uncompressed files");
            }

            List<Result> results = runTests(rcComm, config, testFiles);
            String outputFile = saveResults(config, results, netTest != null ? netTest : new NetworkTestResult(0, 0, 0, 5, 2));

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

    // --------------------------------------------------------------------------------------------
    // Adaptive Compression Logic
    // --------------------------------------------------------------------------------------------

    private static NetworkTestResult testNetworkSpeed(RcComm rcComm, Config config, int testFileSizeMB, int samples) throws Exception {
        log(YELLOW, "\nTesting network speed (" + samples + " samples of " + testFileSizeMB + "MB)...");
        double totalUpload = 0, totalDownload = 0, totalLatency = 0;

        for (int i = 0; i < samples; i++) {
            File testFile = File.createTempFile("irods_speedtest_", ".dat");
            byte[] data = new byte[testFileSizeMB * 1024 * 1024];
            new Random().nextBytes(data);
            Files.write(testFile.toPath(), data);

            String irodsPath = config.homeDir + "/.speedtest_" + System.currentTimeMillis();

            // Measure upload
            long startUp = System.nanoTime();
            try (FileInputStream in = new FileInputStream(testFile);
                 IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(rcComm, irodsPath, false, false)) {
                copy(in, out);
            }
            long endUp = System.nanoTime();
            double upTime = (endUp - startUp) / 1e9;
            double upMBps = testFileSizeMB / upTime;
            totalUpload += upMBps;
            
            // Measure latency (time to first byte response)
            long latencyStart = System.nanoTime();
            try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(rcComm, irodsPath)) {
                in.read(); // Read first byte
            }
            long latencyEnd = System.nanoTime();
            double latencyMs = (latencyEnd - latencyStart) / 1e6;
            totalLatency += latencyMs;

            // Measure download
            File download = File.createTempFile("irods_speedtest_dl_", ".dat");
            long startDown = System.nanoTime();
            try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(rcComm, irodsPath);
                 FileOutputStream out = new FileOutputStream(download)) {
                copy(in, out);
            }
            double downTime = (System.nanoTime() - startDown) / 1e9;
            double downMBps = testFileSizeMB / downTime;
            totalDownload += downMBps;

            System.out.printf("Sample %d: Up %.2f MB/s, Down %.2f MB/s, Latency %.0f ms%n", 
                    i + 1, upMBps, downMBps, latencyMs);

            testFile.delete();
            download.delete();
            IRODSFilesystem.remove(rcComm, irodsPath);
        }

        double avgUpload = totalUpload / samples;
        double avgDownload = totalDownload / samples;
        double avgLatency = totalLatency / samples;
        double avgSpeed = (avgUpload + avgDownload) / 2;
        
        System.out.printf("→ Upload speed:   %.2f MB/s%n", avgUpload);
        System.out.printf("→ Download speed: %.2f MB/s%n", avgDownload);
        System.out.printf("→ Average speed:  %.2f MB/s%n", avgSpeed);
        System.out.printf("→ Average latency: %.0f ms%n", avgLatency);
        
        return new NetworkTestResult(avgUpload, avgDownload, avgLatency, testFileSizeMB, samples);
    }

    private static int selectCompressionLevel(NetworkTestResult netTest) {
        double speed = netTest.getAverageSpeed();
        log(CYAN, "\nAdaptive Compression Selection:");
        System.out.printf("  Network speed: %.2f MB/s%n", speed);
        
        int level;
        String rationale;
        if (speed >= 100) {
            level = 1;
            rationale = "Very fast network (>100 MB/s): minimal compression";
        } else if (speed >= 50) {
            level = 3;
            rationale = "Fast network (50-100 MB/s): light compression";
        } else if (speed >= 10) {
            level = 6;
            rationale = "Medium network (10-50 MB/s): balanced compression";
        } else if (speed >= 1) {
            level = 9;
            rationale = "Slow network (1-10 MB/s): high compression";
        } else {
            level = 15;
            rationale = "Very slow network (<1 MB/s): maximum compression";
        }
        
        System.out.printf("  Selected level: %d%n", level);
        System.out.printf("  Rationale: %s%n", rationale);
        return level;
    }

    // --------------------------------------------------------------------------------------------
    // Core Benchmark Logic
    // --------------------------------------------------------------------------------------------

    private static List<Result> runTests(RcComm rcComm, Config config, List<File> testFiles)
            throws InterruptedException, ExecutionException {

        List<Result> results = new ArrayList<>();

        log(YELLOW, String.format("\nRunning %d test iterations...", TEST_RUNS));

        for (int run = 1; run <= TEST_RUNS; run++) {
            log(CYAN, String.format("\n--- Run %d/%d ---", run, TEST_RUNS));
            for (File testFile : testFiles) {
                try {
                    Result r = performTest(rcComm, config, testFile, run);
                    results.add(r);
                    log(GREEN, String.format("✓ Run %d/%d [%s]: Upload %s (%.2f MB/s), Download %s (%.2f MB/s)",
                            r.run, TEST_RUNS, truncate(r.filename, 30), 
                            formatTime(r.uploadTime), r.uploadThroughput(), 
                            formatTime(r.downloadTime), r.downloadThroughput()));
                } catch (Exception e) {
                    log(RED, String.format("✗ Run %d/%d [%s]: FAILED - %s", 
                            run, TEST_RUNS, testFile.getName(), e.getMessage()));
                }
            }
        }

        return results;
    }

    private static Result performTest(RcComm rcComm, Config config, File testFile, int run) throws Exception {
        String filename = testFile.getName();
        long size = testFile.length();
        File uploadFile = testFile;
        double compTime = 0;

        if (ENABLE_COMPRESSION) {
            System.out.println("\n  Compressing (level " + compressionLevel + ")...");
            long start = System.nanoTime();
            uploadFile = compress(testFile);
            compTime = (System.nanoTime() - start) / 1e9;
            log(GREEN, String.format("  ✓ Compressed: %s -> %s (%.1f%% reduction, %s)",
                    formatSize(size), formatSize(uploadFile.length()),
                    (1 - (double) uploadFile.length() / size) * 100,
                    formatTime(compTime)));
        }

        String irodsPath = config.homeDir + "/bench_" + filename + "_" + System.currentTimeMillis();

        System.out.println("  Uploading " + filename + " (" + formatSize(uploadFile.length()) + ")...");
        long startUp = System.nanoTime();
        try (FileInputStream in = new FileInputStream(uploadFile);
             IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(rcComm, irodsPath, false, false)) {
            copyWithProgress(in, out, uploadFile.length(), "Upload");
        }
        double uploadTime = (System.nanoTime() - startUp) / 1e9;
        clearLine();

        System.out.println("  Downloading " + filename + "...");
        File downloadFile = File.createTempFile("irods_dl", ".tmp");
        long startDown = System.nanoTime();
        try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(rcComm, irodsPath);
             FileOutputStream out = new FileOutputStream(downloadFile)) {
            copyWithProgress(in, out, uploadFile.length(), "Download");
        }
        double downloadTime = (System.nanoTime() - startDown) / 1e9;
        clearLine();

        // --- IMPORTANT FIX ---
        // Capture transferSize BEFORE deleting the compressed temp file.
        long transferSize = uploadFile.length();

        // Cleanup (delete temporary files after capturing transfer size)
        downloadFile.delete();
        if (ENABLE_COMPRESSION) {
            // delete the temp compressed file now that we recorded its size
            uploadFile.delete();
        }
        IRODSFilesystem.remove(rcComm, irodsPath);

        // Return Result with accurate transferSize
        return new Result(run, filename, size, transferSize, uploadTime, downloadTime, compTime);
    }

    private static void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        int len;
        while ((len = in.read(buf)) != -1) out.write(buf, 0, len);
    }

    private static void copyWithProgress(InputStream in, OutputStream out, long total, String label) throws IOException {
        byte[] buf = new byte[BUFFER_SIZE];
        long transferred = 0;
        int len;
        long lastPrint = System.currentTimeMillis();
        long startTime = System.currentTimeMillis();

        while ((len = in.read(buf)) != -1) {
            out.write(buf, 0, len);
            transferred += len;
            
            long now = System.currentTimeMillis();
            if (now - lastPrint > 500 || transferred == total) {
                double percent = (100.0 * transferred / total);
                double elapsed = (now - startTime) / 1000.0;
                double speed = elapsed > 0 ? (transferred / 1024.0 / 1024.0) / elapsed : 0;
                
                int barWidth = 50;
                int filled = (int)(percent / 2);
                String bar = "=".repeat(Math.max(0, filled)) + ">" + " ".repeat(Math.max(0, barWidth - filled - 1));
                
                System.out.printf("\r  %s: [%s] %.1f%% (%.2f MB / %.2f MB) %.2f MB/s    ",
                        label, bar, percent,
                        transferred / 1024.0 / 1024.0,
                        total / 1024.0 / 1024.0,
                        speed);
                System.out.flush();
                lastPrint = now;
            }
        }
    }

    private static void clearLine() {
        System.out.print("\r" + " ".repeat(120) + "\r");
        System.out.flush();
    }

    private static File compress(File input) throws IOException {
        File output = File.createTempFile("irods_zst_", ".zst");
        try (FileInputStream in = new FileInputStream(input);
             ZstdOutputStream out = new ZstdOutputStream(new FileOutputStream(output), compressionLevel)) {
            // Set buffer size for Zstd streaming
            out.setCloseFrameOnFlush(false); // Better streaming performance
            copy(in, out);
        }
        return output;
    }
    
    private static File decompress(File input) throws IOException {
        File output = File.createTempFile("irods_decomp_", ".tmp");
        try (ZstdInputStream in = new ZstdInputStream(new FileInputStream(input));
             FileOutputStream out = new FileOutputStream(output)) {
            copy(in, out);
        }
        return output;
    }

    private static String saveResults(Config config, List<Result> results, NetworkTestResult netTest) throws IOException {
        new File(RESULTS_DIR).mkdirs();
        String timestamp = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss").format(new Date());
        String filename = String.format("%s/irods4j_adaptive_%s_%s.txt",
                RESULTS_DIR, ENABLE_COMPRESSION ? "compressed" : "uncompressed", timestamp);

        try (PrintWriter w = new PrintWriter(filename)) {
            w.println("iRODS Adaptive Compression Benchmark Results");
            w.println("=".repeat(80));
            w.println("Date: " + new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date()) + " UTC");
            w.println("Timestamp: " + timestamp);
            w.println("User: " + System.getProperty("user.name"));
            w.println("iRODS User: " + config.username);
            w.println("iRODS Zone: " + config.zone);
            w.println("iRODS Host: " + config.host + ":" + config.port);
            w.println("Method: Java iRODS Client (irods4j) with Adaptive Compression");
            w.println("Compression Algorithm: Zstandard");
            w.println("Compression Level: " + compressionLevel);
            w.println("Total test runs: " + results.size());
            
            // List files tested
            Set<String> uniqueFiles = new LinkedHashSet<>();
            for (Result r : results) uniqueFiles.add(r.filename);
            w.println("Files tested: " + String.join(", ", uniqueFiles));
            w.println();

            // Network Speed Test Results
            if (netTest != null && netTest.samples > 0) {
                w.println("Network Speed Test Results:");
                w.println("-".repeat(80));
                w.println("Test file size: " + netTest.testFileSizeMB + " MB");
                w.println("Test samples: " + netTest.samples);
                w.printf("Upload speed:   %.2f MB/s%n", netTest.avgUploadSpeed);
                w.printf("Download speed: %.2f MB/s%n", netTest.avgDownloadSpeed);
                w.printf("Average speed:  %.2f MB/s%n", netTest.getAverageSpeed());
                w.printf("Average latency: %.0f ms%n", netTest.avgLatency);
                w.println("Strategy selected: " + getStrategyName());
                w.println("Rationale: " + getStrategyRationale());
                w.println();
            }

            // Calculate totals
            double totalUploadTime = results.stream().mapToDouble(r -> r.uploadTime).sum();
            double totalDownloadTime = results.stream().mapToDouble(r -> r.downloadTime).sum();
            double totalCompressTime = results.stream().mapToDouble(r -> r.compressTime).sum();
            
            double avgUpload = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
            double avgDownload = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
            double avgUpTput = results.stream().mapToDouble(Result::uploadThroughput).average().orElse(0);
            double avgDownTput = results.stream().mapToDouble(Result::downloadThroughput).average().orElse(0);

            w.println("Aggregate Statistics:");
            w.println("-".repeat(80));
            w.printf("Total upload time:        %.3fs%n", totalUploadTime);
            w.printf("Total download time:      %.3fs%n", totalDownloadTime);
            w.printf("Total compression time:   %.3fs%n", totalCompressTime);
            w.printf("Total decompression time: 0.000s%n");
            w.printf("Average upload time:      %.3fs%n", avgUpload);
            w.printf("Average download time:    %.3fs%n", avgDownload);
            w.printf("Average upload throughput:   %.2f MB/s%n", avgUpTput);
            w.printf("Average download throughput: %.2f MB/s%n", avgDownTput);
            w.println();

            // Per-file statistics
            w.println("Per-File Statistics:");
            w.println("-".repeat(80));
            w.println();
            
            // Group by filename
            Map<String, List<Result>> fileGroups = new LinkedHashMap<>();
            for (Result r : results) {
                fileGroups.computeIfAbsent(r.filename, k -> new ArrayList<>()).add(r);
            }
            
            for (Map.Entry<String, List<Result>> entry : fileGroups.entrySet()) {
                String fname = entry.getKey();
                List<Result> fileResults = entry.getValue();
                
                long avgOrigSize = (long) fileResults.stream().mapToLong(r -> r.originalSize).average().orElse(0);
                long avgTransferSize = (long) fileResults.stream().mapToLong(r -> r.transferSize).average().orElse(0);
                double avgCompRatio = avgOrigSize > 0 ? (1.0 - (double)avgTransferSize / avgOrigSize) * 100 : 0;
                double avgUpTime = fileResults.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
                double avgDownTime = fileResults.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
                
                w.printf("File: %s%n", fname);
                w.printf("  Original size: %s%n", formatSize(avgOrigSize));
                w.printf("  Avg compressed size: %s%n", formatSize(avgTransferSize));
                w.printf("  Avg compression ratio: %.1f%%%n", avgCompRatio);
                w.printf("  Runs: %d%n", fileResults.size());
                w.printf("  Avg upload time: %.3fs%n", avgUpTime);
                w.printf("  Avg download time: %.3fs%n", avgDownTime);
                w.println();
            }

            w.println("Detailed Results:");
            w.println("-".repeat(80));
            w.println("Run | File | Size | Compressed | Upload | Download | Ratio");
            w.println("-".repeat(80));

            for (Result r : results) {
                double compressionRatio = r.originalSize > 0 ? (1.0 - (double)r.transferSize / r.originalSize) * 100 : 0;
                w.printf("%3d | %-20s | %10s | %10s | %7.3fs | %7.3fs | %5.1f%%%n",
                        r.run, truncate(r.filename, 20), formatSize(r.originalSize),
                        formatSize(r.transferSize), r.uploadTime, r.downloadTime, compressionRatio);
            }
        }
        return filename;
    }

    private static void displaySummary(List<Result> results, String outputFile) {
        System.out.println();
        log(GREEN, "=".repeat(80));
        log(GREEN, "BENCHMARK RESULTS SUMMARY");
        log(GREEN, "=".repeat(80));

        double avgUpload = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
        double avgDownload = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
        double avgCompress = results.stream().mapToDouble(r -> r.compressTime).average().orElse(0);
        double avgUpTput = results.stream().mapToDouble(Result::uploadThroughput).average().orElse(0);
        double avgDownTput = results.stream().mapToDouble(Result::downloadThroughput).average().orElse(0);

        System.out.println("Buffer size: " + formatSize(BUFFER_SIZE));
        System.out.println("Compression level used: " + compressionLevel);
        System.out.println("Successful runs: " + results.size());
        System.out.printf("Average compression time: %s%n", formatTime(avgCompress));
        System.out.printf("Average upload time:      %s (%.2f MB/s)%n", formatTime(avgUpload), avgUpTput);
        System.out.printf("Average download time:    %s (%.2f MB/s)%n", formatTime(avgDownload), avgDownTput);
        System.out.printf("Total round-trip time:    %s%n", formatTime(avgCompress + avgUpload + avgDownload));
        log(GREEN, "\nResults saved to: " + outputFile);
    }

    // --------------------------------------------------------------------------------------------
    // Utility Methods
    // --------------------------------------------------------------------------------------------

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

        String content = new String(Files.readAllBytes(Paths.get(envFile)))
                .replaceAll("[{}\"]", "").replaceAll("\\s+", "");
        Properties props = new Properties();
        for (String line : content.split(",")) {
            if (line.contains(":")) {
                String[] parts = line.split(":", 2);
                props.setProperty(parts[0], parts[1]);
            }
        }

        Config c = new Config();
        c.host = props.getProperty("irods_host");
        c.port = Integer.parseInt(props.getProperty("irods_port", "1247"));
        c.username = props.getProperty("irods_user_name");
        c.zone = props.getProperty("irods_zone_name");
        c.homeDir = props.getProperty("irods_home", "/" + c.zone + "/home/" + c.username);
        c.password = "usfusf";
        return c;
    }

    private static List<File> getTestFiles() {
        File dir = new File(TEST_FILES_DIR);
        if (!dir.exists()) throw new RuntimeException("Test files directory not found: " + TEST_FILES_DIR);
        File[] files = dir.listFiles(File::isFile);
        if (files == null || files.length == 0) throw new RuntimeException("No files found in " + TEST_FILES_DIR);
        
        // Filter out system files
        List<File> filtered = new ArrayList<>();
        for (File f : files) {
            if (!f.getName().contains(".Identifier") && !f.getName().startsWith(".")) {
                filtered.add(f);
            }
        }
        
        // Sort by size (smallest first)
        filtered.sort(Comparator.comparingLong(File::length));
        return filtered;
    }

    private static String formatSize(long bytes) {
        String[] units = {"B", "KB", "MB", "GB"};
        double size = bytes;
        int i = 0;
        while (size >= 1024 && i < units.length - 1) {
            size /= 1024;
            i++;
        }
        return String.format("%.2f %s", size, units[i]);
    }

    private static String formatTime(double seconds) {
        if (seconds < 60) {
            return String.format("%.2fs", seconds);
        } else if (seconds < 3600) {
            int mins = (int)(seconds / 60);
            double secs = seconds % 60;
            return String.format("%dm %.2fs", mins, secs);
        } else {
            int hours = (int)(seconds / 3600);
            int mins = (int)((seconds % 3600) / 60);
            double secs = seconds % 60;
            return String.format("%dh %dm %.2fs", hours, mins, secs);
        }
    }

    private static String truncate(String s, int len) {
        return s.length() > len ? s.substring(0, len - 3) + "..." : s;
    }

    private static void log(String color, String message) {
        System.out.println(color + message + NC);
    }

    // Helper methods for strategy info
    private static String getStrategyName() {
        if (compressionLevel <= 3) return "fast";
        if (compressionLevel <= 6) return "medium";
        if (compressionLevel <= 9) return "slow";
        return "very_slow";
    }

    private static String getStrategyRationale() {
        if (compressionLevel == 1) return "Very fast network (>100 MB/s): minimal compression";
        if (compressionLevel == 3) return "Fast network (50-100 MB/s): light compression";
        if (compressionLevel == 6) return "Medium network (10-50 MB/s): balanced compression";
        if (compressionLevel == 9) return "Slow network (1-10 MB/s): high compression";
        return "Very slow network (<1 MB/s): maximum compression";
    }

    // --------------------------------------------------------------------------------------------
    // Data Classes
    // --------------------------------------------------------------------------------------------

    static class Config {
        String host, username, password, zone, homeDir;
        int port;
    }

    static class Result {
        int run;
        String filename;
        long originalSize, transferSize;
        double uploadTime, downloadTime, compressTime;

        Result(int run, String filename, long originalSize, long transferSize, 
               double uploadTime, double downloadTime, double compressTime) {
            this.run = run;
            this.filename = filename;
            this.originalSize = originalSize;
            this.transferSize = transferSize;
            this.uploadTime = uploadTime;
            this.downloadTime = downloadTime;
            this.compressTime = compressTime;
        }

        double uploadThroughput() { 
            // Use original size to show effective throughput (what user cares about)
            return (originalSize / 1024.0 / 1024.0) / uploadTime; 
        }
        
        double downloadThroughput() { 
            // Use original size to show effective throughput
            return (originalSize / 1024.0 / 1024.0) / downloadTime; 
        }
    }

    static class NetworkTestResult {
        double avgUploadSpeed;
        double avgDownloadSpeed;
        double avgLatency;
        int testFileSizeMB;
        int samples;
        
        NetworkTestResult(double avgUploadSpeed, double avgDownloadSpeed, double avgLatency, int testFileSizeMB, int samples) {
            this.avgUploadSpeed = avgUploadSpeed;
            this.avgDownloadSpeed = avgDownloadSpeed;
            this.avgLatency = avgLatency;
            this.testFileSizeMB = testFileSizeMB;
            this.samples = samples;
        }
        
        double getAverageSpeed() {
            return (avgUploadSpeed + avgDownloadSpeed) / 2;
        }
    }
}

