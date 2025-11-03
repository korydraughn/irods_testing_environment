import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.api.IRODSApi.RcComm;
import org.irods.irods4j.low_level.protocol.packing_instructions.*;
import org.irods.irods4j.common.Reference;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class IrodsPerformanceTest1 {

private static final int TEST_RUNS = 3;  
private static final String TEST_FILES_DIR = "/mnt/c/Users/maxxm/OneDrive/Desktop/testfiles";  
private static final String RESULTS_DIR = "./performance_results";  

// Toggle compression on/off  
private static final boolean ENABLE_COMPRESSION = false;  

public static void main(String[] args) throws Exception {  
    String host = "localhost";  
    int port = 1247;  
    String user = "rods";  
    String password = "rods";  
    String zone = "tempZone";  

    new File(RESULTS_DIR).mkdirs();  
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
        RcComm rcComm = conn.getRcComm();  

        for (int run = 1; run <= TEST_RUNS; run++) {  
            for (File file : files) {  
                runSingleTest(rcComm, file, run, user, zone, ENABLE_COMPRESSION);  
            }  
        }  
    }  
}  

private static void runSingleTest(RcComm rcComm, File file, int run, String user, String zone, boolean compress) throws Exception {  
    String irodsPath = String.format("/%s/home/%s/%s_%d", zone, user, file.getName(), System.currentTimeMillis());  
    File uploadFile = compress ? compressFile(file) : file;  

    long transferSize = uploadFile.length();  
    if (compress) {  
        System.out.printf("Compressed %s (%.2f MB → %.2f MB)%n", file.getName(), file.length()/1e6, transferSize/1e6);  
    }  

    // Upload  
    DataObjInp_PI createInp = new DataObjInp_PI();  
    createInp.objPath = irodsPath;  
    createInp.dataSize = transferSize;  
    createInp.oprType = 0;  
    createInp.KeyValPair_PI = new KeyValPair_PI();  

    long startUpload = System.nanoTime();  
    int fd = IRODSApi.rcDataObjCreate(rcComm, createInp);  
    if (fd < 0) throw new IOException("rcDataObjCreate failed: " + fd);  

    byte[] buffer = new byte[4 * 1024 * 1024];  
    try (InputStream fis = new FileInputStream(uploadFile)) {  
        int bytesRead;  
        while ((bytesRead = fis.read(buffer)) != -1) {  
            OpenedDataObjInp_PI writeInp = new OpenedDataObjInp_PI();  
            writeInp.l1descInx = fd;  
            byte[] chunk = Arrays.copyOf(buffer, bytesRead);  
            int writeStatus = IRODSApi.rcDataObjWrite(rcComm, writeInp, chunk);  
            if (writeStatus < 0) throw new IOException("rcDataObjWrite failed: " + writeStatus);  
        }  
    }  

    double uploadTime = (System.nanoTime() - startUpload) / 1e9;  
    double uploadRate = (transferSize / 1e6) / uploadTime;  
    System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s)%n", run, file.getName(), uploadTime, uploadRate);  

    // Download  
    DataObjInp_PI openInp = new DataObjInp_PI();  
    openInp.objPath = irodsPath;  
    openInp.oprType = 0;  
    openInp.KeyValPair_PI = new KeyValPair_PI();  

    long startDownload = System.nanoTime();
    int readFd = IRODSApi.rcDataObjOpen(rcComm, openInp);  
    if (readFd < 0) throw new IOException("rcDataObjCreate (open) failed: " + readFd);  

    File downloadFile = Files.createTempFile("irods_download_", compress ? ".zst" : ".tmp").toFile();  
    try (OutputStream fos = new FileOutputStream(downloadFile)) {  
        OpenedDataObjInp_PI readInp = new OpenedDataObjInp_PI();  
        readInp.l1descInx = readFd;  
        IRODSApi.ByteArrayReference ref = new IRODSApi.ByteArrayReference();  

        int bytesRead;  
        while ((bytesRead = IRODSApi.rcDataObjRead(rcComm, readInp, ref)) > 0) {  
            fos.write(ref.data, 0, bytesRead);  
        }  
    }  

    double downloadTime = (System.nanoTime() - startDownload) / 1e9;  
    double downloadRate = (transferSize / 1e6) / downloadTime;  
    System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s)%n", run, file.getName(), downloadTime, downloadRate);  

    // Decompress if compressed  
    if (compress) {  
        File decompressed = decompressFile(downloadFile);  
        System.out.printf("Decompressed back to %.2f MB%n", decompressed.length()/1e6);  
        decompressed.delete();  
    }  

    // Cleanup  
    DataObjInp_PI unlinkInp = new DataObjInp_PI();  
    unlinkInp.objPath = irodsPath;  
    unlinkInp.KeyValPair_PI = new KeyValPair_PI();  
    IRODSApi.rcDataObjUnlink(rcComm, unlinkInp);  

    if (compress) uploadFile.delete();  
    downloadFile.delete();  
}  

private static File compressFile(File input) throws IOException {  
    File compressed = File.createTempFile("compressed_", ".zst");  
    try (InputStream fis = new FileInputStream(input);  
         OutputStream fos = new FileOutputStream(compressed);  
         ZstdOutputStream zOut = new ZstdOutputStream(fos)) {  
        fis.transferTo(zOut);  
    }  
    return compressed;  
}  

private static File decompressFile(File input) throws IOException {  
    File decompressed = File.createTempFile("decompressed_", ".tmp");  
    try (InputStream fis = new FileInputStream(input);  
         ZstdInputStream zIn = new ZstdInputStream(fis);  
         OutputStream fos = new FileOutputStream(decompressed)) {  
        zIn.transferTo(fos);  
    }  
    return decompressed;  
}

}
