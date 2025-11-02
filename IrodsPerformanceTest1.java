import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.protocol.packing_instructions.*;
import org.irods.irods4j.common.Reference;

import java.io.*;
import java.nio.file.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class IrodsPerformanceTest1 {

    private static final int TEST_RUNS = 3;
    private static final String TEST_FILES_DIR = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles";
    private static final String RESULTS_DIR = "./performance_results";

    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 1247;
        String user = "rods";
        String password = "rods";
        String zone = "tempZone";

        new File(RESULTS_DIR).mkdirs();

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
            RcComm rcComm = conn.getRcComm();

            for (int run = 1; run <= TEST_RUNS; run++) {
                for (File file : files) {
                    runSingleTest(rcComm, file, run, user, zone);
                }
            }
        }
    }

    private static void runSingleTest(RcComm rcComm, File file, int run, String user, String zone) throws Exception {
        String irodsPath = String.format("/%s/home/%s/%s_%d", zone, user, file.getName(), System.currentTimeMillis());
        long fileSize = file.length();

        // --- Upload ---
        DataObjInp_PI createInput = new DataObjInp_PI();
        createInput.objPath = irodsPath;
        createInput.dataSize = fileSize;
        createInput.oprType = 0;
        createInput.KeyValPair_PI = new KeyValPair_PI();

        long startUpload = System.nanoTime();
        int fd = IRODSApi.rcDataObjCreate(rcComm, createInput);
        if (fd < 0) throw new IOException("rcDataObjCreate failed: " + fd);

        byte[] buffer = new byte[4 * 1024 * 1024];
        try (InputStream fis = new FileInputStream(file)) {
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                OpenedDataObjInp_PI writeInp = new OpenedDataObjInp_PI();
                writeInp.l1descInx = fd;
                byte[] chunk = Arrays.copyOf(buffer, bytesRead);
                int writeStatus = IRODSApi.rcDataObjWrite(rcComm, writeInp, chunk);
                if (writeStatus < 0) throw new IOException("rcDataObjWrite failed: " + writeStatus);
            }
        }

        // Close file descriptor (simply lseek to 0)
        OpenedDataObjInp_PI closeInp = new OpenedDataObjInp_PI();
        closeInp.l1descInx = fd;
        IRODSApi.rcDataObjLseek(rcComm, closeInp, new Reference<>());

        double uploadTime = (System.nanoTime() - startUpload) / 1e9;
        double uploadRate = (fileSize / 1e6) / uploadTime;
        System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s)%n",
                run, file.getName(), uploadTime, uploadRate);

        // --- Download ---
        DataObjInp_PI openInput = new DataObjInp_PI();
        openInput.objPath = irodsPath;
        openInput.oprType = 0;
        openInput.KeyValPair_PI = new KeyValPair_PI();

        long startDownload = System.nanoTime();
        int readFd = IRODSApi.rcDataObjCreate(rcComm, openInput); // some builds may require rcDataObjOpen
        if (readFd < 0) throw new IOException("rcDataObjCreate (open) failed: " + readFd);

        File downloadFile = Files.createTempFile("irods_download_", ".tmp").toFile();
        try (OutputStream fos = new FileOutputStream(downloadFile)) {
            OpenedDataObjInp_PI readInp = new OpenedDataObjInp_PI();
            readInp.l1descInx = readFd;
            IRODSApi.ByteArrayReference ref = new IRODSApi.ByteArrayReference();

            int bytesRead;
            while ((bytesRead = IRODSApi.rcDataObjRead(rcComm, readInp, ref)) > 0) {
                fos.write(ref.data, 0, bytesRead);
            }
        }

        IRODSApi.rcDataObjLseek(rcComm, closeInp, new Reference<>());
        double downloadTime = (System.nanoTime() - startDownload) / 1e9;
        double downloadRate = (fileSize / 1e6) / downloadTime;

        System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s)%n",
                run, file.getName(), downloadTime, downloadRate);

        // --- Cleanup ---
        DataObjInp_PI unlinkInput = new DataObjInp_PI();
        unlinkInput.objPath = irodsPath;
        unlinkInput.KeyValPair_PI = new KeyValPair_PI();
        IRODSApi.rcDataObjUnlink(rcComm, unlinkInput);
        downloadFile.delete();
    }
}

