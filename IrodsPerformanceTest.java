import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSErrorCodes;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.common.Reference;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class IrodsPerformanceTest {

    private static final int TEST_RUNS = 5;
    private static final String TEST_FILES_DIR = "C:\\Users\\maxxm\\OneDrive\\Desktop\\testfiles";
    private static final boolean ENABLE_COMPRESSION = false;
    private static final String RESULTS_DIR = "./performance_results";

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 1247;
        String user = "rods";
        String password = "rods";
        String zone = "tempZone";

        new File(RESULTS_DIR).mkdirs();
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss"));

        File dir = new File(TEST_FILES_DIR);
        File[] files = dir.listFiles(File::isFile);
        if (files == null || files.length == 0) {
            System.out.println("No test files found in " + TEST_FILES_DIR);
            return;
        }

        System.out.printf("Running %d test runs on %d file(s)%n", TEST_RUNS, files.length);

        try (IRODSConnection conn = new IRODSConnection()) {
            conn.connect(host, port, new QualifiedUsername(user, zone));
            conn.authenticate(new NativeAuthPlugin(), password);

            List<Map<String, Object>> results = new ArrayList<>();

            for (int run = 1; run <= TEST_RUNS; run++) {
                for (File file : files) {
                    runSingleTest(conn, file, run, user, zone, results);
                }
            }

            saveResults(results, timestamp, user, zone, host, port);
        }
    }

    private static void runSingleTest(IRODSConnection conn, File file, int run,
                                      String user, String zone, List<Map<String, Object>> results) throws IOException {
        String fileName = file.getName();
        long originalSize = file.length();
        String irodsPath = String.format("/%s/home/%s/bench_%s_%d", zone, user, fileName, System.currentTimeMillis());

        File uploadFile = file;
        long transferSize = originalSize;
        double compressTime = 0, decompressTime = 0;
        double compressionRatio = 0;

        // Optional compression
        if (ENABLE_COMPRESSION) {
            long startCompress = System.nanoTime();
            uploadFile = compressFile(file);
            compressTime = (System.nanoTime() - startCompress) / 1e9;
            transferSize = uploadFile.length();
            compressionRatio = (1 - (double) transferSize / originalSize) * 100;
        }

        // Upload
        DataObjInp_PI putInput = new DataObjInp_PI();
        putInput.objPath = irodsPath;
        putInput.KeyValPair_PI = new KeyValPair_PI();
        Reference<Integer> putOutput = new Reference<>();
        long startUpload = System.nanoTime();
        int uploadCode = IRODSApi.rcDataObjPut(conn.getRcComm(), uploadFile.getAbsolutePath(), putInput, putOutput);
        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        if (uploadCode != 0) {
            System.out.printf("Upload failed with code: %d%n", uploadCode);
            return;
        }

        // Download
        File downloadFile = Files.createTempFile("download_", ".tmp").toFile();
        DataObjInp_PI getInput = new DataObjInp_PI();
        getInput.objPath = irodsPath;
        getInput.KeyValPair_PI = new KeyValPair_PI();
        Reference<Integer> getOutput = new Reference<>();
        long startDownload = System.nanoTime();
        int downloadCode = IRODSApi.rcDataObjGet(conn.getRcComm(), getInput, downloadFile.getAbsolutePath(), getOutput);
        double downloadTime = (System.nanoTime() - startDownload) / 1e9;
        if (downloadCode != 0) {
            System.out.printf("Download failed with code: %d%n", downloadCode);
            return;
        }

        // Optional decompression
        if (ENABLE_COMPRESSION) {
            long startDecompress = System.nanoTime();
            File decompressedFile = Files.createTempFile("decompressed_", ".dat").toFile();
            decompressFile(downloadFile, decompressedFile);
            decompressTime = (System.nanoTime() - startDecompress) / 1e9;
            long finalSize = decompressedFile.length();
            if (finalSize != originalSize) {
                System.out.printf("Warning: size mismatch %d != %d%n", finalSize, originalSize);
            }
            decompressedFile.delete();
        }

        // Clean up
        IRODSApi.rcDataObjUnlink(conn.getRcComm(), irodsPath);
        downloadFile.delete();
        if (ENABLE_COMPRESSION && uploadFile != file) uploadFile.delete();

        double uploadMBs = (transferSize / 1e6) / uploadTime;
        double downloadMBs = (transferSize / 1e6) / downloadTime;

        System.out.printf("Run %d [%s]: Upload %.2fs (%.2f MB/s), Download %.2fs (%.2f MB/s)%n",
                run, fileName, uploadTime, uploadMBs, downloadTime, downloadMBs);

        // Store results
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("run", run);
        row.put("filename", fileName);
        row.put("original_size_bytes", originalSize);
        row.put("transfer_size_bytes", transferSize);
        row.put("compression_ratio", compressionRatio);
        row.put("compress_time", compressTime);
        row.put("upload_time", uploadTime);
        row.put("download_time", downloadTime);
        row.put("decompress_time", decompressTime);
        row.put("upload_throughput_mbps", uploadMBs);
        row.put("download_throughput_mbps", downloadMBs);
        results.add(row);
    }

    private static File compressFile(File input) throws IOException {
        File compressed = Files.createTempFile("compressed_", ".gz").toFile();
        try (FileInputStream fis = new FileInputStream(input);
             FileOutputStream fos = new FileOutputStream(compressed);
             GZIPOutputStream gos = new GZIPOutputStream(fos)) {
            fis.transferTo(gos);
        }
        return compressed;
    }

    private static void decompressFile(File input, File output) throws IOException {
        try (GZIPInputStream gis = new GZIPInputStream(new FileInputStream(input));
             FileOutputStream fos = new FileOutputStream(output)) {
            gis.transferTo(fos);
        }
    }

    private static void saveResults(List<Map<String, Object>> results, String timestamp,
                                    String user, String zone, String host, int port) throws IOException {
        File resultFile = new File(RESULTS_DIR, "irods4j_benchmark_" + timestamp + ".txt");
        try (PrintWriter out = new PrintWriter(new FileWriter(resultFile))) {
            out.printf("iRODS Performance Test Results (irods4j)%n");
            out.println("=".repeat(80));
            out.printf("Date: %s%n", LocalDateTime.now());
            out.printf("User: %s%nZone: %s%nHost: %s:%d%n", user, zone, host, port);
            out.printf("Compression: %s%n%n", ENABLE_COMPRESSION ? "ENABLED (gzip)" : "DISABLED");

            out.println("Run | File | Upload(s) | Download(s) | U MB/s | D MB/s | Ratio");
            out.println("-".repeat(80));
            for (Map<String, Object> r : results) {
                out.printf("%3d | %-20s | %8.3f | %10.3f | %7.2f | %7.2f | %5.1f%%%n",
                        r.get("run"),
                        r.get("filename"),
                        r.get("upload_time"),
                        r.get("download_time"),
                        r.get("upload_throughput_mbps"),
                        r.get("download_throughput_mbps"),
                        r.get("compression_ratio"));
            }
        }
        System.out.println("Results saved to " + resultFile.getAbsolutePath());
    }
}

