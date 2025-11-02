import org.irods.irods4j.high_level.connection.IRODSConnection;
import org.irods.irods4j.high_level.connection.QualifiedUsername;
import org.irods.irods4j.authentication.NativeAuthPlugin;
import org.irods.irods4j.low_level.api.IRODSApi;
import org.irods.irods4j.low_level.protocol.packing_instructions.DataObjInp_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.KeyValPair_PI;
import org.irods.irods4j.low_level.protocol.packing_instructions.OpenedDataObjInp_PI;
import org.irods.irods4j.common.Reference;

import java.io.*;
import java.nio.file.*;

public class IrodsPerformanceTest {
    public static void main(String[] args) throws Exception {
        String host = "localhost";
        int port = 1247;
        String user = "rods";
        String password = "rods";
        String zone = "tempZone";
        String localPath = "C:/Users/maxxm/OneDrive/Desktop/testfiles/test1.bin";
        String irodsPath = "/tempZone/home/rods/test1.bin";

        byte[] data = Files.readAllBytes(Path.of(localPath));

        try (IRODSConnection conn = new IRODSConnection()) {
            conn.connect(host, port, new QualifiedUsername(user, zone));
            conn.authenticate(new NativeAuthPlugin(), password);
            System.out.println("✅ Connected to iRODS");

            // ----- UPLOAD -----
            var createInp = new DataObjInp_PI();
            createInp.objPath = irodsPath;
            createInp.KeyValPair_PI = new KeyValPair_PI();
            createInp.createMode = 0644;
            createInp.openFlags = 0x0001; // O_WRONLY

            var fdRef = new Reference<Integer>();
            IRODSApi.rcDataObjCreate(conn.getRcComm(), createInp, fdRef);
            int fd = fdRef.value;

            var writeInp = new OpenedDataObjInp_PI();
            writeInp.l1descInx = fd;

            long startUpload = System.nanoTime();
            IRODSApi.rcDataObjWrite(conn.getRcComm(), writeInp, data);
            IRODSApi.rcDataObjClose(conn.getRcComm(), writeInp);
            System.out.printf("Upload time: %.2f s%n", (System.nanoTime() - startUpload) / 1e9);

            // ----- DOWNLOAD -----
            var readInp = new DataObjInp_PI();
            readInp.objPath = irodsPath;
            readInp.KeyValPair_PI = new KeyValPair_PI();
            readInp.openFlags = 0x0000; // O_RDONLY

            var fdRef2 = new Reference<Integer>();
            IRODSApi.rcDataObjOpen(conn.getRcComm(), readInp, fdRef2);
            int readFd = fdRef2.value;

            var readOdi = new OpenedDataObjInp_PI();
            readOdi.l1descInx = readFd;

            long startDownload = System.nanoTime();
            IRODSApi.rcDataObjRead(conn.getRcComm(), readOdi);
            IRODSApi.rcDataObjClose(conn.getRcComm(), readOdi);
            System.out.printf("Download time: %.2f s%n", (System.nanoTime() - startDownload) / 1e9);

            // ----- CLEANUP -----
            IRODSApi.rcDataObjUnlink(conn.getRcComm(), irodsPath);
            System.out.println("✅ File removed from iRODS");
        }
    }
}

