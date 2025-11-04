import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.api.IRODSApi.ByteArrayReference;
import org.irods.irods4j.low_level.protocol.packing_instructions.OpenedDataObjInp_PI;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class IrodsPerformanceTest {
    public static void main(String[] args) throws Exception {
        String host = "98.93.165.152";
        int port = 1247;
        String user = "rods";
        String password = "usfusf";
        String zone = "tempZone";

        String testDirPath = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles"; // adjust if needed
        File testDir = new File(testDirPath);
        if (!testDir.exists() || !testDir.isDirectory()) {
            throw new IllegalArgumentException("Invalid test directory: " + testDirPath);
        }

        System.out.println("Running 3 test runs on " + testDir.listFiles().length + " file(s) | Compression: DISABLED");
        System.out.println();

        System.out.println("[DEBUG] Connecting to iRODS...");
        RcComm rcComm = IRODSApi.rcConnect(
                host,
                port,
                user,
                zone,
                java.util.Optional.of(password),
                java.util.Optional.empty(),
                java.util.Optional.empty(),
                java.util.Optional.empty()
        );

        System.out.println("[DEBUG] Connected, authenticating...");
        int status = IRODSApi.clientLogin(rcComm, password);
        if (status != 0) throw new RuntimeException("Authentication failed: " + status);
        System.out.println("[DEBUG] Authentication successful.");
        System.out.println();

        for (File file : testDir.listFiles()) {
            if (!file.isFile()) continue;
            for (int run = 1; run <= 3; run++) {
                System.out.printf("[DEBUG] ===== Starting test for: %s (Run %d) =====%n", file.getName(), run);
                runSingleTest(rcComm, file, run);
                System.out.println();
            }
        }

        IRODSApi.rcDisconnect(rcComm);
        System.out.println("[DEBUG] Disconnected cleanly.");
    }

    private static void runSingleTest(RcComm rcComm, File file, int run) throws Exception {
        long transferSize = file.length();
        String remotePath = "/tempZone/home/rods/" + file.getName() + "_" + System.currentTimeMillis();
        String localDownloadPath = "/tmp/download_" + file.getName();

        // Upload
        long startUpload = System.nanoTime();
        OpenedDataObjInp_PI openInp = new OpenedDataObjInp_PI();
        openInp.objPath = remotePath;
        openInp.createMode = 0644;
        openInp.openFlags = 2; // O_WRONLY | O_CREAT

        int fd = IRODSApi.rcDataObjCreate(rcComm, openInp);
        if (fd < 0) throw new java.io.IOException("rcDataObjCreate failed: " + fd);
        System.out.println("[DEBUG] Upload fd: " + fd);

        try (FileInputStream fis = new FileInputStream(file)) {
            byte[] buffer = new byte[65536];
            ByteArrayReference ref = new ByteArrayReference(buffer);
            int bytesRead;
            long totalUploaded = 0;

            while ((bytesRead = fis.read(buffer)) != -1) {
                ref.data = buffer;
                int status = IRODSApi.rcDataObjWrite(rcComm, fd, ref, bytesRead);
                if (status < 0) throw new java.io.IOException("rcDataObjWrite failed: " + status);
                totalUploaded += bytesRead;
            }
            System.out.println("[DEBUG] Total bytes uploaded: " + totalUploaded);
        }

        IRODSApi.rcDataObjClose(rcComm, fd);
        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        double uploadRate = (transferSize / 1e6) / uploadTime;
        System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s), total bytes: %d%n",
                run, file.getName(), uploadTime, uploadRate, transferSize);

        // Download
        long startDownload = System.nanoTime();
        OpenedDataObjInp_PI readInp = new OpenedDataObjInp_PI();
        readInp.objPath = remotePath;
        readInp.openFlags = 0; // O_RDONLY

        int readFd = IRODSApi.rcDataObjOpen(rcComm, readInp);
        if (readFd < 0) throw new java.io.IOException("rcDataObjOpen failed: " + readFd);
        System.out.println("[DEBUG] Download fd: " + readFd);

        long totalDownloaded = 0;
        ByteArrayReference ref = new ByteArrayReference(new byte[65536]);
        try (OutputStream fos = new FileOutputStream(localDownloadPath)) {
            int bytesRead;
            while ((bytesRead = IRODSApi.rcDataObjRead(rcComm, readInp, ref)) > 0) {
                fos.write(ref.data, 0, bytesRead);
                totalDownloaded += bytesRead;
                System.out.printf("[DEBUG] Downloaded chunk: %d bytes%n", bytesRead);
            }
        }

        IRODSApi.rcDataObjClose(rcComm, readFd);
        double downloadTime = (System.nanoTime() - startDownload) / 1e9;
        double downloadRate = (transferSize / 1e6) / downloadTime;
        System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s), total bytes: %d%n",
                run, file.getName(), downloadTime, downloadRate, totalDownloaded);
    }
}

