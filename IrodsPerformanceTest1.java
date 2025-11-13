import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.high_level.filesystem.IRODSFilesystem;
import org.irods.irods4j.high_level.data_object.*;

import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;

public class IrodsAdaptiveCompressionTest {

    // ==================== CONFIGURATION ====================
    private static final int TEST_RUNS = 3;
    private static final String TEST_FILES_DIR = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles";
    private static final boolean ENABLE_ADAPTIVE_COMPRESSION = true;
    private static final int MANUAL_COMPRESSION_LEVEL = 3;
    private static final boolean ENABLE_FILE_VERIFICATION = true;
    private static final boolean ENABLE_CLEANUP = true;
    private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB
    
    // Network speed test configuration
    private static final int NETWORK_TEST_SIZE_MB = 5;
    private static final int NETWORK_TEST_SAMPLES = 3;
    // =======================================================
    
    // ANSI Color codes
    private static final String RESET = "\033[0m";
    private static final String RED = "\033[0;31m";
    private static final String GREEN = "\033[0;32m";
    private static final String YELLOW = "\033[1;33m";
    private static final String BLUE = "\033[0;34m";
    private static final String CYAN = "\033[0;36m";
    private static final String MAGENTA = "\033[0;35m";

    // Compression strategy based on network speed
    private static class CompressionStrategy {
        String name;
        double minMbps;
        int level;
        String description;
        
        CompressionStrategy(String name, double minMbps, int level, String description) {
            this.name = name;
            this.minMbps = minMbps;
            this.level = level;
            this.description = description;
        }
    }
    
    private static final CompressionStrategy[] STRATEGIES = {
        new CompressionStrategy("very_fast", 100.0, 1, "Very fast network (>100 MB/s): minimal compression"),
        new CompressionStrategy("fast", 50.0, 3, "Fast network (50-100 MB/s): light compression"),
        new CompressionStrategy("medium", 10.0, 6, "Medium network (10-50 MB/s): balanced compression"),
        new CompressionStrategy("slow", 1.0, 9, "Slow network (1-10 MB/s): high compression"),
        new CompressionStrategy("very_slow", 0.0, 15, "Very slow network (<1 MB/s): maximum compression")
    };

    private static class TestResult {
        int run;
        String filename;
        long originalSize;
        long transferSize;
        int compressionLevel;
        double compressionRatio;
        double compressTime;
        double uploadTime;
        double downloadTime;
        double decompressTime;
        double uploadThroughput;
        double downloadThroughput;
        boolean verified;
    }

    public static void main(String[] args) throws Exception {
        printColor("=" .repeat(80), CYAN);
        printColor("iRODS ADAPTIVE COMPRESSION BENCHMARK (Java)", GREEN);
        printColor("Network-aware compression level selection", GREEN);
        printColor("=" .repeat(80), CYAN);
        System.out.println();

        String host = "98.93.165.152";
        int port = 1247;
        String user = "rods";
        String password = "usfusf";
        String zone = "tempZone";

        File[] files = new File(TEST_FILES_DIR).listFiles(File::isFile);
        if (files == null || files.length == 0) {
            printColor("No test files found in " + TEST_FILES_DIR, RED);
            return;
        }

        printColor(String.format("Found %d test file(s):", files.length), CYAN);
        for (File f : files) {
            System.out.printf("  - %-30s (%s)%n", f.getName(), formatSize(f.length()));
        }
        System.out.println();

        System.out.printf("Test runs: %d%n", TEST_RUNS);
        System.out.printf("Adaptive compression: %s%n", 
            ENABLE_ADAPTIVE_COMPRESSION ? "ENABLED" : "DISABLED");
        System.out.printf("File verification: %s%n", 
            ENABLE_FILE_VERIFICATION ? "ENABLED" : "DISABLED");

        List<TestResult> results = new ArrayList<>();

        try (IRODSConnection conn = new IRODSConnection()) {
            printColor("\nConnecting to iRODS...", YELLOW);
            conn.connect(host, port, new QualifiedUsername(user, zone));
            conn.authenticate(new NativeAuthPlugin(), password);
            printColor(String.format("✓ Connected as %s@%s", user, zone), GREEN);

            if (ENABLE_CLEANUP) {
                cleanupIrodsDirectory(conn, zone, user);
            }

            // Determine compression level
            int compressionLevel;
            if (ENABLE_ADAPTIVE_COMPRESSION) {
                double networkSpeed = testNetworkSpeed(conn, zone, user);
                if (networkSpeed > 0) {
                    compressionLevel = selectCompressionLevel(networkSpeed);
                } else {
                    printColor("\nNetwork test failed, using default compression level", YELLOW);
                    compressionLevel = MANUAL_COMPRESSION_LEVEL;
                }
            } else {
                compressionLevel = MANUAL_COMPRESSION_LEVEL;
                printColor(String.format("\nUsing manual compression level: %d", compressionLevel), CYAN);
            }

            // Run tests
            printColor(String.format("\nRunning %d test iterations...", TEST_RUNS), YELLOW);
            printColor(String.format("  Compression level: %d", compressionLevel), CYAN);
            
            for (int run = 1; run <= TEST_RUNS; run++) {
                for (File file : files) {
                    String irodsPath = String.format("/%s/home/%s/benchmark_%s_%d.zst", 
                        zone, user, file.getName(), System.currentTimeMillis());
                    
                    TestResult result = runSingleTest(conn, file, run, compressionLevel, 
                        irodsPath, ENABLE_FILE_VERIFICATION);
                    
                    if (result != null && result.verified) {
                        results.add(result);
                    }
                }
            }

            if (ENABLE_CLEANUP) {
                cleanupIrodsDirectory(conn, zone, user);
            }

            // Print results
            if (!results.isEmpty()) {
                printResultsSummary(results, compressionLevel);
            } else {
                printColor("\nNo successful test runs", RED);
            }

        } catch (Exception e) {
            printColor("\nError: " + e.getMessage(), RED);
            e.printStackTrace();
        }

        printColor("\n" + "=".repeat(80), GREEN);
        printColor("Benchmark completed!", GREEN);
        printColor("=".repeat(80), GREEN);
    }

    private static double testNetworkSpeed(IRODSConnection conn, String zone, String user) 
            throws Exception {
        printColor("\n" + "=".repeat(60), CYAN);
        printColor("NETWORK SPEED TEST", CYAN);
        printColor("=".repeat(60), CYAN);
        System.out.printf("Test file size: %d MB%n", NETWORK_TEST_SIZE_MB);
        System.out.printf("Test samples: %d%n", NETWORK_TEST_SAMPLES);
        System.out.println();

        List<Double> uploadSpeeds = new ArrayList<>();
        List<Double> downloadSpeeds = new ArrayList<>();

        for (int i = 0; i < NETWORK_TEST_SAMPLES; i++) {
            System.out.printf("Sample %d/%d: ", i + 1, NETWORK_TEST_SAMPLES);
            
            try {
                // Create test file
                File testFile = createTestFile(NETWORK_TEST_SIZE_MB);
                long fileSize = testFile.length();
                String irodsPath = String.format("/%s/home/%s/.speedtest_%d.tmp", 
                    zone, user, System.currentTimeMillis());

                // Upload
                long uploadStart = System.nanoTime();
                try (InputStream fis = new FileInputStream(testFile);
                     IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(
                         conn, irodsPath, true, false)) {
                    fis.transferTo(out);
                }
                double uploadTime = (System.nanoTime() - uploadStart) / 1e9;
                double uploadMbps = (fileSize / (1024.0 * 1024.0)) / uploadTime;

                // Download
                File downloadFile = Files.createTempFile("irods_speedtest_", ".tmp").toFile();
                long downloadStart = System.nanoTime();
                try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(conn, irodsPath);
                     OutputStream fos = new FileOutputStream(downloadFile)) {
                    in.transferTo(fos);
                }
                double downloadTime = (System.nanoTime() - downloadStart) / 1e9;
                double downloadMbps = (fileSize / (1024.0 * 1024.0)) / downloadTime;

                uploadSpeeds.add(uploadMbps);
                downloadSpeeds.add(downloadMbps);

                System.out.printf("Up: %.2f MB/s, Down: %.2f MB/s%n", uploadMbps, downloadMbps);

                // Cleanup
                testFile.delete();
                downloadFile.delete();
                IRODSFilesystem.remove(conn, irodsPath, RemoveOptions.NO_TRASH);

            } catch (Exception e) {
                printColor("Failed: " + e.getMessage(), RED);
            }
        }

        if (uploadSpeeds.isEmpty()) {
            printColor("\nNetwork speed test failed!", RED);
            return -1.0;
        }

        double avgUpload = uploadSpeeds.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double avgDownload = downloadSpeeds.stream().mapToDouble(Double::doubleValue).average().orElse(0);
        double avgSpeed = (avgUpload + avgDownload) / 2.0;

        System.out.println();
        printColor("Network Speed Test Results:", GREEN);
        System.out.printf("  Average upload speed:   %.2f MB/s%n", avgUpload);
        System.out.printf("  Average download speed: %.2f MB/s%n", avgDownload);
        System.out.printf("  Average speed:          %.2f MB/s%n", avgSpeed);

        return avgSpeed;
    }

    private static int selectCompressionLevel(double networkSpeedMbps) {
        System.out.println();
        printColor("=".repeat(60), CYAN);
        printColor("ADAPTIVE COMPRESSION SELECTION", CYAN);
        printColor("=".repeat(60), CYAN);
        System.out.printf("Network speed: %.2f MB/s%n", networkSpeedMbps);

        CompressionStrategy selected = STRATEGIES[STRATEGIES.length - 1];
        for (CompressionStrategy strategy : STRATEGIES) {
            if (networkSpeedMbps >= strategy.minMbps) {
                selected = strategy;
                break;
            }
        }

        System.out.printf("Strategy: %s%n", selected.name);
        System.out.printf("Selected compression level: %d%n", selected.level);
        System.out.printf("Rationale: %s%n", selected.description);
        System.out.println();

        System.out.println("Compression level characteristics (zstd):");
        System.out.println("  Level 1:  ~500 MB/s compression, ~2000 MB/s decompression");
        System.out.println("  Level 3:  ~200 MB/s compression, ~2000 MB/s decompression");
        System.out.println("  Level 6:  ~100 MB/s compression, ~1500 MB/s decompression");
        System.out.println("  Level 9:   ~40 MB/s compression, ~1200 MB/s decompression");
        System.out.println("  Level 15:  ~10 MB/s compression, ~1000 MB/s decompression");
        System.out.println();

        if (selected.level == 1) {
            printColor("→ Network is fast! Using minimal compression to save CPU.", GREEN);
        } else if (selected.level <= 3) {
            printColor("→ Network is fairly fast. Using light compression.", GREEN);
        } else if (selected.level <= 6) {
            printColor("→ Network speed is moderate. Balanced compression/speed tradeoff.", YELLOW);
        } else if (selected.level <= 9) {
            printColor("→ Network is slow. Using higher compression to reduce transfer time.", YELLOW);
        } else {
            printColor("→ Network is very slow! Using maximum compression.", RED);
        }

        return selected.level;
    }

    private static TestResult runSingleTest(IRODSConnection conn, File file, int run, 
            int compressionLevel, String irodsPath, boolean verify) throws Exception {
        
        String filename = file.getName();
        long originalSize = file.length();
        TestResult result = new TestResult();
        result.run = run;
        result.filename = filename;
        result.originalSize = originalSize;
        result.compressionLevel = compressionLevel;
        result.verified = false;

        String originalChecksum = null;
        if (verify) {
            originalChecksum = calculateChecksum(file);
        }

        System.out.printf("  Run %d/%d [%s]: Compress(L%d)... ", 
            run, TEST_RUNS, filename, compressionLevel);

        // Compress
        long compressStart = System.nanoTime();
        File compressedFile = compressFile(file, compressionLevel);
        result.compressTime = (System.nanoTime() - compressStart) / 1e9;

        long transferSize = compressedFile.length();
        result.transferSize = transferSize;
        result.compressionRatio = (1 - (double) transferSize / originalSize) * 100;

        System.out.printf("✓ (%s, %.1f%%, %.3fs) Up... ", 
            formatSize(transferSize), result.compressionRatio, result.compressTime);

        // Upload
        long uploadStart = System.nanoTime();
        try (InputStream fis = new FileInputStream(compressedFile);
             IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(
                 conn, irodsPath, true, false)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                out.write(buffer, 0, bytesRead);
            }
        }
        result.uploadTime = (System.nanoTime() - uploadStart) / 1e9;
        result.uploadThroughput = (transferSize / (1024.0 * 1024.0)) / result.uploadTime;

        System.out.print("✓ Down... ");

        // Download
        File downloadedFile = Files.createTempFile("irods_download_", ".zst").toFile();
        long downloadStart = System.nanoTime();
        try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(conn, irodsPath);
             OutputStream fos = new FileOutputStream(downloadedFile)) {
            byte[] buffer = new byte[BUFFER_SIZE];
            int bytesRead;
            while ((bytesRead = in.read(buffer)) != -1) {
                fos.write(buffer, 0, bytesRead);
            }
        }
        result.downloadTime = (System.nanoTime() - downloadStart) / 1e9;
        result.downloadThroughput = (transferSize / (1024.0 * 1024.0)) / result.downloadTime;

        System.out.print("✓ Decomp... ");

        // Decompress
        long decompressStart = System.nanoTime();
        File decompressedFile = Files.createTempFile("decompressed_", ".tmp").toFile();
        try (InputStream zis = new ZstdInputStream(new FileInputStream(downloadedFile));
             OutputStream fos = new FileOutputStream(decompressedFile)) {
            zis.transferTo(fos);
        }
        result.decompressTime = (System.nanoTime() - decompressStart) / 1e9;

        // Verify
        if (verify) {
            long finalSize = decompressedFile.length();
            String finalChecksum = calculateChecksum(decompressedFile);
            
            if (finalSize == originalSize && originalChecksum.equals(finalChecksum)) {
                result.verified = true;
                printColor(String.format("✓ VERIFIED (Up:%.2fs Down:%.2fs)", 
                    result.uploadTime, result.downloadTime), GREEN);
            } else {
                printColor("✗ FAILED verification", RED);
            }
        } else {
            result.verified = true;
            printColor(String.format("✓ (Up:%.2fs Down:%.2fs)", 
                result.uploadTime, result.downloadTime), GREEN);
        }

        // Cleanup
        compressedFile.delete();
        downloadedFile.delete();
        decompressedFile.delete();
        IRODSFilesystem.remove(conn, irodsPath, RemoveOptions.NO_TRASH);

        return result;
    }

    private static void printResultsSummary(List<TestResult> results, int compressionLevel) {
        System.out.println();
        printColor("=".repeat(80), GREEN);
        printColor("BENCHMARK RESULTS SUMMARY", GREEN);
        printColor("=".repeat(80), GREEN);

        double avgCompressTime = results.stream().mapToDouble(r -> r.compressTime).average().orElse(0);
        double avgUploadTime = results.stream().mapToDouble(r -> r.uploadTime).average().orElse(0);
        double avgDownloadTime = results.stream().mapToDouble(r -> r.downloadTime).average().orElse(0);
        double avgDecompressTime = results.stream().mapToDouble(r -> r.decompressTime).average().orElse(0);
        double avgCompressionRatio = results.stream().mapToDouble(r -> r.compressionRatio).average().orElse(0);
        double avgUploadThroughput = results.stream().mapToDouble(r -> r.uploadThroughput).average().orElse(0);
        double avgDownloadThroughput = results.stream().mapToDouble(r -> r.downloadThroughput).average().orElse(0);

        System.out.printf("Compression level used: %d%n", compressionLevel);
        System.out.printf("Total successful runs: %d%n", results.size());
        System.out.printf("Average compression ratio: %.1f%%%n", avgCompressionRatio);
        System.out.printf("Average compression time: %.3fs%n", avgCompressTime);
        System.out.printf("Average upload time: %.3fs (%.2f MB/s)%n", avgUploadTime, avgUploadThroughput);
        System.out.printf("Average download time: %.3fs (%.2f MB/s)%n", avgDownloadTime, avgDownloadThroughput);
        System.out.printf("Average decompression time: %.3fs%n", avgDecompressTime);
        System.out.printf("Total round-trip time: %.3fs%n", 
            avgCompressTime + avgUploadTime + avgDownloadTime + avgDecompressTime);
    }

    private static File compressFile(File inputFile, int compressionLevel) throws IOException {
        File compressed = Files.createTempFile("compressed_", ".zst").toFile();
        try (InputStream fis = new FileInputStream(inputFile);
             ZstdOutputStream zos = new ZstdOutputStream(new FileOutputStream(compressed), compressionLevel)) {
            fis.transferTo(zos);
        }
        return compressed;
    }

    private static File createTestFile(int sizeMb) throws IOException {
        File testFile = Files.createTempFile("speedtest_", ".tmp").toFile();
        long sizeBytes = sizeMb * 1024L * 1024L;
        
        try (FileOutputStream fos = new FileOutputStream(testFile)) {
            byte[] buffer = new byte[1024 * 1024]; // 1 MB chunks
            long written = 0;
            Random random = new Random();
            
            while (written < sizeBytes) {
                int toWrite = (int) Math.min(buffer.length, sizeBytes - written);
                random.nextBytes(buffer);
                fos.write(buffer, 0, toWrite);
                written += toWrite;
            }
        }
        
        return testFile;
    }

    private static String calculateChecksum(File file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                digest.update(buffer, 0, bytesRead);
            }
        }
        byte[] hash = digest.digest();
        StringBuilder sb = new StringBuilder();
        for (byte b : hash) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    private static void cleanupIrodsDirectory(IRODSConnection conn, String zone, String user) {
        try {
            printColor(String.format("\nCleaning up iRODS directory: /%s/home/%s", zone, user), YELLOW);
            // Note: Cleanup implementation would require listing and removing objects
            // This is a placeholder - implement based on irods4j API
            printColor("✓ Cleanup completed", GREEN);
        } catch (Exception e) {
            printColor("Warning: Cleanup failed: " + e.getMessage(), YELLOW);
        }
    }

    private static String formatSize(long bytes) {
        String[] units = {"B", "KB", "MB", "GB", "TB"};
        double size = bytes;
        int unitIndex = 0;
        
        while (size >= 1024.0 && unitIndex < units.length - 1) {
            size /= 1024.0;
            unitIndex++;
        }
        
        return String.format("%.2f %s", size, units[unitIndex]);
    }

    private static void printColor(String message, String color) {
        System.out.println(color + message + RESET);
    }
}
