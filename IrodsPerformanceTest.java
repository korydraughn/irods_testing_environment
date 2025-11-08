import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.high_level.filesystem.IRODSFilesystem;
import org.irods.irods4j.high_level.data_object.*;

import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class IrodsPerformanceTest {

private static final int TEST_RUNS = 3;
private static final String TEST_FILES_DIR = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles";
private static final boolean ENABLE_COMPRESSION = true;
private static final int BUFFER_SIZE = 4 * 1024 * 1024; // 4 MB

public static void main(String[] args) throws Exception {
    String host = "98.93.165.152";
    int port = 1247;
    String user = "rods";
    String password = "usfusf";
    String zone = "tempZone";

    File[] files = new File(TEST_FILES_DIR).listFiles(File::isFile);
    if (files == null || files.length == 0) {
        System.out.println("No test files found in " + TEST_FILES_DIR);
        return;
    }

    System.out.printf("Running %d test runs on %d file(s) | Compression: %s%n",
            TEST_RUNS, files.length, ENABLE_COMPRESSION ? "ENABLED" : "DISABLED");

    try (IRODSConnection conn = new IRODSConnection()) {
        conn.connect(host, port, new QualifiedUsername(user, zone));
        conn.authenticate(new NativeAuthPlugin(), password);

        for (int run = 1; run <= TEST_RUNS; run++) {
            for (File file : files) {
                String irodsPath = String.format("/%s/home/%s/%s_%d", zone, user, file.getName(), System.currentTimeMillis());
                runSingleTest(conn, file, run, ENABLE_COMPRESSION, irodsPath);
            }
        }
    }
}

private static void runSingleTest(IRODSConnection conn, File file, int run, boolean compress, String irodsPath) throws Exception {
    // Prepare upload stream (with optional Zstd compression)
    File uploadFile = file;
    if (compress) {
        File tempCompressed = File.createTempFile("compressed_", ".zst");
        try (InputStream fis = new FileInputStream(file);
             OutputStream fos = new ZstdOutputStream(new FileOutputStream(tempCompressed))) {
            fis.transferTo(fos);
        }
        uploadFile = tempCompressed;
    }

    // --- Upload ---
    long startUpload = System.nanoTime();
    try (InputStream fis = new FileInputStream(uploadFile);
         IRODSDataObjectOutputStream out = new IRODSDataObjectOutputStream(conn, irodsPath, true, false)) {
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        long totalUploaded = 0;
        while ((bytesRead = fis.read(buffer)) != -1) {
            out.write(buffer, 0, bytesRead);
            totalUploaded += bytesRead;
        }
        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s)%n",
                run, file.getName(), uploadTime, (totalUploaded / 1e6) / uploadTime);
    }

    // --- Download ---
    File downloadedFile = Files.createTempFile("irods_download_", compress ? ".zst" : ".tmp").toFile();
    long startDownload = System.nanoTime();
    long totalDownloaded = 0;
    try (IRODSDataObjectInputStream in = new IRODSDataObjectInputStream(conn, irodsPath);
         OutputStream fos = new FileOutputStream(downloadedFile)) {
        byte[] buffer = new byte[BUFFER_SIZE];
        int bytesRead;
        while ((bytesRead = in.read(buffer)) != -1) {
            fos.write(buffer, 0, bytesRead);
            totalDownloaded += bytesRead;
        }
    }
    double downloadTime = (System.nanoTime() - startDownload) / 1e9;
    System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s)%n",
            run, file.getName(), downloadTime, (totalDownloaded / 1e6) / downloadTime);

    // --- Decompress if needed ---
    if (compress) {
        File decompressed = File.createTempFile("decompressed_", ".tmp");
        try (InputStream zis = new ZstdInputStream(new FileInputStream(downloadedFile));
             OutputStream fos = new FileOutputStream(decompressed)) {
            zis.transferTo(fos);
        }
        decompressed.delete();
    }

    // --- Cleanup ---
    IRODSFilesystem.remove(conn, irodsPath, RemoveOptions.NO_TRASH);
    if (compress) uploadFile.delete();
    downloadedFile.delete();
}

}

