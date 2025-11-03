import org.irods.irods4j.core.*;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.connection.RcComm;
import org.irods.irods4j.low_level.types.*;
import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.text.DecimalFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class IrodsPerformanceTest1 {

    // CONFIGURATION
    private static final int TEST_RUNS = 3;
    private static final boolean ENABLE_COMPRESSION = true;
    private static final String TEST_DIR = "/home/demetrius/Desktop/testFiles";
    private static final String RESULTS_DIR = "./performance_results";

    public static void main(String[] args) {
        try {
            new File(RESULTS_DIR).mkdirs();

            System.out.println("Running " + TEST_RUNS + " compression test runs on files in " + TEST_DIR);
            List<Path> files = listFiles(TEST_DIR);
            if (files.isEmpty()) {
                System.err.println("No files found in " + TEST_DIR);
                return;
            }

            RcComm rcComm = IrodsConnection.connectFromEnv();
            List<ResultRecord> results = new ArrayList<>();

            for (int run = 1; run <= TEST_RUNS; run++) {
                for (Path file : files) {
                    String fileName = file.getFileName().toString();
                    System.out.println("\nRun " + run + " | Testing file: " + fileName);

                    long originalSize = Files.size(file);
                    Path uploadFile = file;

                    // Compression step
                    long transferSize = originalSize;
                    double compressionRatio = 0;
                    if (ENABLE_COMPRESSION) {
                        uploadFile = compressFile(file);
                        transferSize = Files.size(uploadFile);
                        compressionRatio = 100.0 * (1 - (double) transferSize / originalSize);
                        System.out.printf("Compressed %s (%.2f MB → %.2f MB, %.1f%% saved)%n",
                                fileName, mb(originalSize), mb(transferSize), compressionRatio);
                    }

                    // Upload test
                    long startUpload = System.nanoTime();
                    String irodsPath = "/tempZone/home/" + System.getProperty("user.name") + "/" + fileName + "_" + run;
                    int fd = IRODSApi.rcDataObjCreate(rcComm, new DataObjInp_PI(irodsPath, "w"), new Reference<>());
                    if (fd < 0) {
                        System.err.println("Upload failed: rcDataObjCreate returned " + fd);
                        continue;
                    }

                    try (InputStream fis = Files.newInputStream(uploadFile)) {
                        byte[] buf = new byte[4 * 1024 * 1024];
                        int bytesRead;
                        while ((bytesRead = fis.read(buf)) != -1) {
                            IRODSApi.rcDataObjWrite(rcComm, fd, buf, bytesRead);
                        }
                    }

                    // Close file descriptor properly
                    OpenedDataObjInp_PI closeInp = new OpenedDataObjInp_PI();
                    closeInp.l1descInx = fd;
                    IRODSApi.rcDataObjLseek(rcComm, closeInp, new Reference<>());

                    long endUpload = System.nanoTime();
                    double uploadTimeSec = (endUpload - startUpload) / 1e9;
                    double uploadThroughput = mb(transferSize) / uploadTimeSec;
                    System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s)%n", run, fileName, uploadTimeSec, uploadThroughput);

                    // Download test
                    long startDownload = System.nanoTime();
                    int readFd = IRODSApi.rcDataObjOpen(rcComm, new DataObjInp_PI(irodsPath, "r"), new Reference<>());
                    if (readFd < 0) {
                        System.err.println("Download failed: rcDataObjOpen returned " + readFd);
                        continue;
                    }

                    Path downloadFile = Files.createTempFile("irods_download_", ".tmp");
                    try (OutputStream fos = Files.newOutputStream(downloadFile)) {
                        byte[] buf = new byte[4 * 1024 * 1024];
                        int bytesRead;
                        while ((bytesRead = IRODSApi.rcDataObjRead(rcComm, readFd, buf, buf.length)) > 0) {
                            fos.write(buf, 0, bytesRead);
                        }
                    }

                    OpenedDataObjInp_PI closeInp2 = new OpenedDataObjInp_PI();
                    closeInp2.l1descInx = readFd;
                    IRODSApi.rcDataObjLseek(rcComm, closeInp2, new Reference<>());

                    long endDownload = System.nanoTime();
                    double downloadTimeSec = (endDownload - startDownload) / 1e9;
                    double downloadThroughput = mb(transferSize) / downloadTimeSec;
                    System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s)%n",
                            run, fileName, downloadTimeSec, downloadThroughput);

                    results.add(new ResultRecord(run, fileName, originalSize, transferSize,
                            uploadTimeSec, downloadTimeSec, uploadThroughput, downloadThroughput,
                            ENABLE_COMPRESSION, compressionRatio));

                    Files.deleteIfExists(downloadFile);
                    if (ENABLE_COMPRESSION && !uploadFile.equals(file)) {
                        Files.deleteIfExists(uploadFile);
                    }

                    // Cleanup iRODS
                    IRODSApi.rcDataObjUnlink(rcComm, new DataObjInp_PI(irodsPath, ""), new Reference<>());
                }
            }

            IrodsConnection.disconnect(rcComm);

            saveResults(results);
            System.out.println("\n✅ Test complete. Results saved in: " + RESULTS_DIR);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<Path> listFiles(String dir) throws IOException {
        List<Path> files = new ArrayList<>();
        Files.list(Paths.get(dir)).filter(Files::isRegularFile).forEach(files::add);
        return files;
    }

    private static Path compressFile(Path inputFile) throws IOException {
        Path compressedFile = Files.createTempFile("irods_compress_", ".zst");
        try (InputStream fis = Files.newInputStream(inputFile);
             OutputStream fos = Files.newOutputStream(compressedFile);
             ZstdOutputStream zOut = new ZstdOutputStream(fos)) {
            byte[] buf = new byte[4 * 1024 * 1024];
            int bytesRead;
            while ((bytesRead = fis.read(buf)) != -1) {
                zOut.write(buf, 0, bytesRead);
            }
        }
        return compressedFile;
    }

    private static void saveResults(List<ResultRecord> results) throws IOException {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        Path csvPath = Paths.get(RESULTS_DIR, "java_client_benchmark_" + timestamp + ".csv");
        Path txtPath = Paths.get(RESULTS_DIR, "java_client_benchmark_" + timestamp + ".txt");

        try (BufferedWriter writer = Files.newBufferedWriter(csvPath)) {
            writer.write("Run,File,Original(MB),Transfer(MB),Upload(s),Download(s),Upload(MB/s),Download(MB/s),Compressed,Ratio(%)\n");
            for (ResultRecord r : results) {
                writer.write(String.format(Locale.US,
                        "%d,%s,%.2f,%.2f,%.3f,%.3f,%.2f,%.2f,%b,%.2f%n",
                        r.run, r.filename, mb(r.originalSize), mb(r.transferSize),
                        r.uploadTime, r.downloadTime, r.uploadThroughput,
                        r.downloadThroughput, r.compressed, r.compressionRatio));
            }
        }

        try (BufferedWriter writer = Files.newBufferedWriter(txtPath)) {
            writer.write("iRODS Java Performance Test Results\n");
            writer.write("====================================\n");
            writer.write("Compression: " + (ENABLE_COMPRESSION ? "ENABLED" : "DISABLED") + "\n");
            writer.write("Total runs: " + results.size() + "\n\n");
            for (ResultRecord r : results) {
                writer.write(String.format(Locale.US,
                        "Run %d | %s | U: %.2fs (%.2f MB/s) | D: %.2fs (%.2f MB/s)\n",
                        r.run, r.filename, r.uploadTime, r.uploadThroughput,
                        r.downloadTime, r.downloadThroughput));
            }
        }
    }

    private static double mb(long bytes) {
        return bytes / 1024.0 / 1024.0;
    }

    private record ResultRecord(
            int run,
            String filename,
            long originalSize,
            long transferSize,
            double uploadTime,
            double downloadTime,
            double uploadThroughput,
            double downloadThroughput,
            boolean compressed,
            double compressionRatio
    ) {}
}

