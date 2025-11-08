import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.high_level.datatypes.IRODSDataObjectInputStream;
import org.irods.irods4j.high_level.datatypes.IRODSDataObjectOutputStream;
import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;

public class IrodsPerformanceTestHighLevel {

    private static final int TEST_RUNS = 3;
    private static final String TEST_FILES_DIR = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles";
    private static final boolean ENABLE_COMPRESSION = true;

    public static void main(String[] args) throws Exception {
        String host = "your-host-here";
        int port = 1247;
        String user = "your-username";
        String password = "your-password";
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
                    String logicalPath = String.format(
                            "/%s/home/%s/%s_%d",
                            zone, user, file.getName(), System.currentTimeMillis()
                    );
                    runSingleTest(conn, file, logicalPath, ENABLE_COMPRESSION);
                }
            }
        }
    }

    private static void runSingleTest(IRODSConnection conn, File file, String logicalPath, boolean compress) throws IOException {
        // --- Upload ---
        File uploadFile = file;
        if (compress) {
            uploadFile = File.createTempFile("compressed_", ".zst");
            try (InputStream fis = new FileInputStream(file);
                 OutputStream fos = new ZstdOutputStream(new FileOutputStream(uploadFile))) {
                fis.transferTo(fos);
            }
            System.out.printf("[DEBUG] Compressed %s (%.2f MB → %.2f MB)%n",
                    file.getName(), file.length() / 1e6, uploadFile.length() / 1e6);
        }

        long startUpload = System.nanoTime();
        try (IRODSDataObjectOutputStream out =
                     new IRODSDataObjectOutputStream(conn.getRcComm(), logicalPath, true, false)) {
            Files.copy(uploadFile.toPath(), out);
        }
        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        System.out.printf("[UPLOAD COMPLETE] %s | %.2f MB/s%n",
                file.getName(), (uploadFile.length() / 1e6) / uploadTime);

        // --- Download ---
        File downloadFile = Files.createTempFile("irods_download_", compress ? ".zst" : ".tmp").toFile();
        long startDownload = System.nanoTime();
        try (IRODSDataObjectInputStream in =
                     new IRODSDataObjectInputStream(conn.getRcComm(), logicalPath);
             OutputStream fos = new FileOutputStream(downloadFile)) {
            in.transferTo(fos);
        }
        double downloadTime = (System.nanoTime() - startDownload) / 1e9;
        System.out.printf("[DOWNLOAD COMPLETE] %s | %.2f MB/s%n",
                file.getName(), (downloadFile.length() / 1e6) / downloadTime);

        // --- Optional decompression ---
        if (compress) {
            File decompressedFile = new File(downloadFile.getParent(), "decompressed_" + file.getName());
            long startDecompress = System.nanoTime();
            try (InputStream zis = new ZstdInputStream(new FileInputStream(downloadFile));
                 OutputStream fos = new FileOutputStream(decompressedFile)) {
                zis.transferTo(fos);
            }
            double decompressTime = (System.nanoTime() - startDecompress) / 1e9;
            System.out.printf("[DECOMPRESS COMPLETE] %s in %.2fs%n",
                    file.getName(), decompressTime);
            decompressedFile.delete();
        }

        // --- Cleanup ---
        conn.getRcComm().removeDataObject(logicalPath);
        if (compress) uploadFile.delete();
        downloadFile.delete();
    }
}

