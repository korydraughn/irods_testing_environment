import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.high_level.file.IRODSFile;
import org.irods.irods4j.high_level.file.IRODSFileFactory;
import com.github.luben.zstd.ZstdOutputStream;
import com.github.luben.zstd.ZstdInputStream;

import java.io.*;
import java.nio.file.*;
import java.util.*;

public class IrodsPerformanceTest {

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
    File[] files = new File(TEST_FILES_DIR).listFiles(File::isFile);  
    if (files == null || files.length == 0) {  
        System.out.println("No test files found in " + TEST_FILES_DIR);  
        return;  
    }  

    System.out.printf("Running %d compression test runs on %d file(s)%n", TEST_RUNS, files.length);  

    try (IRODSConnection conn = new IRODSConnection()) {  
        conn.connect(host, port, new QualifiedUsername(user, zone));  
        conn.authenticate(new NativeAuthPlugin(), password);  

        IRODSFileFactory fileFactory = conn.getIRODSFileFactory();  

        for (int run = 1; run <= TEST_RUNS; run++) {  
            for (File file : files) {  
                runSingleCompressionTest(conn, fileFactory, file, run, user, zone);  
            }  
        }  
    }  
}  

private static void runSingleCompressionTest(IRODSConnection conn, IRODSFileFactory fileFactory, File file, int run, String user, String zone) throws Exception {  
    String irodsPath = String.format("/%s/home/%s/%s_compressed_%d", zone, user, file.getName(), System.currentTimeMillis());  

    // Compress file
    File compressedFile = compressFile(file);  
    long compressedSize = compressedFile.length();  
    System.out.printf("Compressed %s (%.2f MB → %.2f MB)%n", file.getName(), file.length() / 1e6, compressedSize / 1e6);  

    // Upload using high-level API
    IRODSFile remoteFile = fileFactory.instanceIRODSFile(irodsPath);  
    long startUpload = System.nanoTime();  
    try (InputStream fis = new FileInputStream(compressedFile);  
         OutputStream out = fileFactory.instanceIRODSFileOutputStream(remoteFile)) {  
        byte[] buffer = new byte[4 * 1024 * 1024];  
        int bytesRead;  
        while ((bytesRead = fis.read(buffer)) != -1) {  
            out.write(buffer, 0, bytesRead);  
        }  
    }  
    double uploadTime = (System.nanoTime() - startUpload) / 1e9;  
    double uploadRate = (compressedSize / 1e6) / uploadTime;  
    System.out.printf("Run %d | Uploaded %s in %.2fs (%.2f MB/s)%n", run, file.getName(), uploadTime, uploadRate);  

    // Download using high-level API
    File downloadFile = Files.createTempFile("irods_download_", ".zst").toFile();  
    long startDownload = System.nanoTime();  
    try (InputStream in = fileFactory.instanceIRODSFileInputStream(remoteFile);  
         OutputStream out = new FileOutputStream(downloadFile)) {  
        byte[] buffer = new byte[4 * 1024 * 1024];  
        int bytesRead;  
        while ((bytesRead = in.read(buffer)) != -1) {  
            out.write(buffer, 0, bytesRead);  
        }  
    }  
    double downloadTime = (System.nanoTime() - startDownload) / 1e9;  
    double downloadRate = (downloadFile.length() / 1e6) / downloadTime;  
    System.out.printf("Run %d | Downloaded %s in %.2fs (%.2f MB/s)%n", run, file.getName(), downloadTime, downloadRate);  

    // Decompress to verify
    File decompressed = decompressFile(downloadFile);  
    System.out.printf("Decompressed back to %.2f MB%n", decompressed.length() / 1e6);  

    // Cleanup
    remoteFile.delete();  
    compressedFile.delete();  
    downloadFile.delete();  
    decompressed.delete();  
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

