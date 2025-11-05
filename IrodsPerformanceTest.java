import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.protocol.packing_instructions.*;
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
            System.out.println("[DEBUG] Connecting to iRODS...");
            conn.connect(host, port, new QualifiedUsername(user, zone));
            System.out.println("[DEBUG] Connected, authenticating...");
            conn.authenticate(new NativeAuthPlugin(), password);
            System.out.println("[DEBUG] Authentication successful.\n");

            RcComm rcComm = conn.getRcComm();
            for (int run = 1; run <= TEST_RUNS; run++) {
                for (File file : files) {
                    String irodsPath = String.format("/%s/home/%s/%s_%d", zone, user, file.getName(), System.currentTimeMillis());
                    runSingleTest(rcComm, file, run, ENABLE_COMPRESSION, irodsPath);
                }
            }
        }
    }

    private static void runSingleTest(RcComm rcComm, File file, int run, boolean compress, String irodsPath) throws Exception {

        // Compress file if enabled
        File uploadFile = file;
        if (compress) {
            uploadFile = File.createTempFile("compressed_", ".zst");
            try (InputStream fis = new FileInputStream(file);
                 OutputStream fos = new ZstdOutputStream(new FileOutputStream(uploadFile))) {
                fis.transferTo(fos);
            }
            System.out.printf("[DEBUG] Compressed %s (%.2f MB → %.2f MB)%n", file.getName(),
                    file.length() / 1e6, uploadFile.length() / 1e6);
        }

        // --- Upload ---
        System.out.printf("[DEBUG] Uploading to: %s%n", irodsPath);
        DataObjInp_PI createInp = new DataObjInp_PI();
        createInp.objPath = irodsPath;
        createInp.dataSize = uploadFile.length();
        createInp.oprType = 0;
        createInp.KeyValPair_PI = new KeyValPair_PI();

        long startUpload = System.nanoTime();
        int fd = IRODSApi.rcDataObjCreate(rcComm, createInp);
        if (fd < 0) throw new IOException("rcDataObjCreate failed: " + fd);

        byte[] buffer = new byte[BUFFER_SIZE];
        long totalUploaded = 0;
        try (InputStream fis = new FileInputStream(uploadFile)) {
            int bytesRead;
            OpenedDataObjInp_PI writeInp = new OpenedDataObjInp_PI();
            writeInp.l1descInx = fd;
            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] chunk = Arrays.copyOf(buffer, bytesRead);
                int status = IRODSApi.rcDataObjWrite(rcComm, writeInp, chunk);
                if (status < 0) throw new IOException("rcDataObjWrite failed: " + status);
                totalUploaded += bytesRead;
            }
        }
        IRODSApi.rcDataObjClose(rcComm, fd);
        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s), total bytes: %d%n",
                run, file.getName(), uploadTime, (totalUploaded / 1e6) / uploadTime, totalUploaded);

        // --- Download ---
        System.out.printf("[DEBUG] Downloading from: %s%n", irodsPath);
        DataObjInp_PI openInp = new DataObjInp_PI();
        openInp.objPath = irodsPath;
        openInp.oprType = 0;
        openInp.KeyValPair_PI = new KeyValPair_PI();

        long startDownload = System.nanoTime();
        int readFd = IRODSApi.rcDataObjOpen(rcComm, openInp);
        if (readFd < 0) throw new IOException("rcDataObjOpen failed: " + readFd);

        File downloadFile = Files.createTempFile("irods_download_", compress ? ".zst" : ".tmp").toFile();
        OpenedDataObjInp_PI readInp = new OpenedDataObjInp_PI();
        readInp.l1descInx = readFd;
        IRODSApi.ByteArrayReference ref = new IRODSApi.ByteArrayReference();
        ref.data = new byte[BUFFER_SIZE];

        long totalDownloaded = 0;
        try (OutputStream fos = compress ? new ZstdOutputStream(new FileOutputStream(downloadFile)) : new FileOutputStream(downloadFile)) {
            int bytesRead;
            while ((bytesRead = IRODSApi.rcDataObjRead(rcComm, readInp, ref)) > 0) {
                fos.write(ref.data, 0, bytesRead);
                totalDownloaded += bytesRead;
            }
        }
        IRODSApi.rcDataObjClose(rcComm, readFd);
        double downloadTime = (System.nanoTime() - startDownload) / 1e9;
        System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s), total bytes: %d%n",
                run, file.getName(), downloadTime, (totalDownloaded / 1e6) / downloadTime, totalDownloaded);

        // --- Cleanup ---
        DataObjInp_PI unlinkInp = new DataObjInp_PI();
        unlinkInp.objPath = irodsPath;
        unlinkInp.KeyValPair_PI = new KeyValPair_PI();
        IRODSApi.rcDataObjUnlink(rcComm, unlinkInp);

        if (compress) uploadFile.delete();
        downloadFile.delete();
    }
}

