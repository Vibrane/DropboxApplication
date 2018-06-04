package surfstore;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.MetadataStore.MetadataStoreImpl.FileInfoStruct;
import surfstore.MetadataStoreGrpc.MetadataStoreBlockingStub;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Empty;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.SimpleAnswer;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;

public final class MetadataStore {
    private static final Logger logger = Logger.getLogger(MetadataStore.class.getName());
    private Server server;
    private static ManagedChannel blockChannel; // had to remove the final
    private static BlockStoreGrpc.BlockStoreBlockingStub blockStub; // had to remove final
    private static ConfigReader config;

    private static int metadataID; // this server's id
    private static boolean leader; // gets the leader number
    private static boolean distributed; // checks whether we are dealign with distributed or centralized

    private static ManagedChannel metadataChannelA;
    private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStubA;

    private static ManagedChannel metadataChannelB;
    private static MetadataStoreGrpc.MetadataStoreBlockingStub metadataStubB;

    private static ConcurrentLinkedDeque<FileInfoStruct> logA; // log of operations for follower A
    private static ConcurrentLinkedDeque<FileInfoStruct> logB; // log of operations for follower B

    public MetadataStore(ConfigReader config) {

        blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true).build();
        blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

        MetadataStore.config = config;
        leader = (MetadataStore.metadataID == MetadataStore.config.getLeaderNum()) ? true : false;

        distributed = (config.getNumMetadataServers() > 1) ? true : false;

        if (distributed) // check if we are dealing with a distributed system
        {
            if (leader) // if this server is the leader
            {
                if (metadataID == 1) // checks id of the current server
                {
                    metadataChannelA = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                            .usePlaintext(true).build();
                    metadataStubA = MetadataStoreGrpc.newBlockingStub(metadataChannelA);

                    metadataChannelB = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                            .usePlaintext(true).build();
                    metadataStubB = MetadataStoreGrpc.newBlockingStub(metadataChannelB);
                }

                else if (metadataID == 2) {
                    metadataChannelA = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                            .usePlaintext(true).build();
                    metadataStubA = MetadataStoreGrpc.newBlockingStub(metadataChannelA);

                    metadataChannelB = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
                            .usePlaintext(true).build();
                    metadataStubB = MetadataStoreGrpc.newBlockingStub(metadataChannelB);
                }

                else if (metadataID == 3) {
                    metadataChannelA = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
                            .usePlaintext(true).build();
                    metadataStubA = MetadataStoreGrpc.newBlockingStub(metadataChannelA);

                    metadataChannelB = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
                            .usePlaintext(true).build();
                    metadataStubB = MetadataStoreGrpc.newBlockingStub(metadataChannelB);
                }

                logA = new ConcurrentLinkedDeque<>();
                logB = new ConcurrentLinkedDeque<>();
            }

            else // not leader in distributed system
            {
                metadataChannelA = null;
                metadataStubA = null;
                metadataChannelB = null;
                metadataStubB = null;
                logA = null;
                logB = null;
            }
        }

        else // centralized so there will not be back-up servers
        {
            metadataChannelA = null;
            metadataStubA = null;
            metadataChannelB = null;
            metadataStubB = null;
            logA = null;
            logB = null;
        }

    }

    private void start(int port, int numThreads) throws IOException {
        server = ServerBuilder.forPort(port).addService(new MetadataStoreImpl())
                .executor(Executors.newFixedThreadPool(numThreads)).build().start();
        logger.info("Server started, listening on " + port);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                MetadataStore.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    private static Namespace parseArgs(String[] args) {
        ArgumentParser parser = ArgumentParsers.newFor("MetadataStore").build()
                .description("MetadataStore server for SurfStore");
        parser.addArgument("config_file").type(String.class).help("Path to configuration file");
        parser.addArgument("-n", "--number").type(Integer.class).setDefault(1).help("Set which number this server is");
        parser.addArgument("-t", "--threads").type(Integer.class).setDefault(10)
                .help("Maximum number of concurrent threads");

        Namespace res = null;
        try {
            res = parser.parseArgs(args);
        } catch (ArgumentParserException e) {
            parser.handleError(e);
        }

        return res;
    }

    public static void main(String[] args) throws Exception {

        Namespace c_args = parseArgs(args);
        if (c_args == null) {
            throw new RuntimeException("Argument parsing failed");
        }

        File configf = new File(c_args.getString("config_file"));
        ConfigReader config = new ConfigReader(configf);

        if (c_args.getInt("number") > config.getNumMetadataServers()) {
            throw new RuntimeException(String.format("metadata%d not in config file", c_args.getInt("number")));
        }
        metadataID = c_args.getInt("number"); // this should get the current metadata id
        final MetadataStore server = new MetadataStore(config);
        server.start(config.getMetadataPort(c_args.getInt("number")), c_args.getInt("threads"));

        if (leader && distributed) {
            // this is going to be the facilitator
            facilitator(metadataStubA, logA); // independent thread that handles the log for follower A
            facilitator(metadataStubB, logB); // independent thread that handles the log for follower B
        }

        server.blockUntilShutdown();
    }
    
    private static void facilitator(final MetadataStoreBlockingStub stub,
            final ConcurrentLinkedDeque<FileInfoStruct> log) {
        new Thread(new Runnable() {
            public void run() {
                while (true) {
                    if (stub.isCrashed(Empty.newBuilder().build()).getAnswer() == false) // means it is active
                    {
                        while (!log.isEmpty()) // while there are updates left
                        {
                            FileInfoStruct fileInfo = log.pop(); // first request
                            FileInfo.Builder builder = FileInfo.newBuilder();

                            builder.setFilename(fileInfo.fileName);
                            builder.setVersion(fileInfo.version);
                            builder.addAllBlocklist(fileInfo.hashList);

                            FileInfo request = builder.build();

                            if (stub.updateFollower(request).getAnswer() == false) // means update did not occur
                            {
                                log.push(fileInfo); // the update didnt happen
                                break; // breaks outside of the loop
                            }
                        }
                    }
                    try {
                        Thread.sleep(500); // sleep for a while before performing check again
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    static class MetadataStoreImpl extends MetadataStoreGrpc.MetadataStoreImplBase {

        private Map<String, Integer> fileVersionMap;
        private Map<String, ArrayList<String>> hashListMap;
        private boolean alive;

        /**
         * Constructor for this server
         */
        public MetadataStoreImpl() {
            super();
            fileVersionMap = new ConcurrentHashMap<>();
            hashListMap = new ConcurrentHashMap<>();
            alive = true;
        }

        @Override
        public void ping(Empty req, final StreamObserver<Empty> responseObserver) {
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Read the requested file.
         * The client only needs to supply the "filename" argument of FileInfo.
         * The server only needs to fill the "version" and "blocklist" fields.
         * If the file does not exist, "version" should be set to 0.  This
         * call should return the result even if the server it is invoked on
         * is not the leader.
         *
         * For part 2, this call should return the status of the file
         * including the block list and version number *even if the server is
         * in a crashed state*.  This is so we can test your code.
         * </pre>
         */

        public void readFile(surfstore.SurfStoreBasic.FileInfo request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.FileInfo> responseObserver) {
            String fileName = request.getFilename(); // only one that needs to be given
            int version = Integer.MIN_VALUE;
            ArrayList<String> hashList = new ArrayList<>();

            if (fileVersionMap.containsKey(fileName)) {
                version = fileVersionMap.get(fileName);
                hashList = hashListMap.get(fileName);
            } else
                version = 0;

            FileInfo.Builder builder = FileInfo.newBuilder();
            builder.setFilename(fileName);
            builder.setVersion(version);
            builder.addAllBlocklist(hashList); // this should add the entire contents of ArrayList

            FileInfo response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Write a file.
         * The client must specify all fields of the FileInfo message.
         * The server returns the result of the operation in the "result" field.
         *
         * The server ALWAYS sets "current_version", regardless of whether
         * the command was successful. If the write succeeded, it will be the
         * version number provided by the client. Otherwise, it is set to the
         * version number in the MetadataStore.
         *
         * If the result is MISSING_BLOCKS, "missing_blocks" contains a
         * list of blocks that are not present in the BlockStore.
         *
         * This command should return an error if it is called on a server
         * that is not the leader
         * </pre>
         */
        public void modifyFile(surfstore.SurfStoreBasic.FileInfo request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {

            String fileName = request.getFilename(); // fileName given
            int version = request.getVersion(); // version number given
            ArrayList<String> hashList = new ArrayList<>(); // hashLists which should exist
            for (int i = 0; i < request.getBlocklistCount(); i++)
                hashList.add(request.getBlocklist(i));
            ArrayList<String> missingBlocks = new ArrayList<>();
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (leader) {
                if (fileVersionMap.containsKey(fileName)) // that means file did exist in the past
                {
                    // first check for missing blocks
                    for (String hash : hashList) // check for missing blocks
                        if (MetadataStore.blockStub.hasBlock(blockRequest(hash)).getAnswer() == false)
                            missingBlocks.add(hash);
                    if (missingBlocks.size() > 0)
                        builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                    else if (version != (fileVersionMap.get(fileName) + 1)) // check for version number
                        builder.setResult(WriteResult.Result.OLD_VERSION); // incorrect version number
                    else // everything seems to be good for the new updated file
                    {
                        fileVersionMap.put(fileName, version); // version number updated
                        hashListMap.put(fileName, hashList); // hashList updated
                        builder.setResult(WriteResult.Result.OK);

                        // since this is okay, time to create FileInfoStruct
                        fileInfoCreator(fileName, version, hashList);

                    }

                    builder.setCurrentVersion(fileVersionMap.get(fileName)); // set the fileVersion Number
                    builder.addAllMissingBlocks(missingBlocks); // add all the missing blocks even if none

                } // done with if file existed in the meta prior

                else // file did not exist in meta
                {
                    // first check for missing blocks
                    for (String hash : hashList) // check for missing blocks
                        if (MetadataStore.blockStub.hasBlock(blockRequest(hash)).getAnswer() == false)
                            missingBlocks.add(hash);
                    if (missingBlocks.size() > 0) // check to make all blocks of original file exists
                    {
                        builder.setResult(WriteResult.Result.MISSING_BLOCKS);
                        builder.setCurrentVersion(0); // set the fileVersion Number to 0
                    } // done with missing blocks
                    else if (version != 1) { // incorrect version number for new file
                        builder.setResult(WriteResult.Result.OLD_VERSION);
                        builder.setCurrentVersion(0);
                    } // done with incorrect version
                    else // everything version number and hashlist are correct for new file
                    {
                        fileVersionMap.put(fileName, version); // version number is now 1
                        hashListMap.put(fileName, hashList); // hashList updated
                        builder.setResult(WriteResult.Result.OK);
                        builder.setCurrentVersion(1); // set the fileVersion Number

                        // since this is okay, time to create FileInfoStruct
                        fileInfoCreator(fileName, version, hashList);
                    }

                    builder.addAllMissingBlocks(missingBlocks); // add all the missing blocks even if none

                } // done with original file

            } // done with yes, leader

            else // not leader
            {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                if (fileVersionMap.containsKey(fileName))
                    builder.setCurrentVersion(fileVersionMap.get(fileName));
                else
                    builder.setCurrentVersion(0); // file does not exist
                builder.addAllMissingBlocks(new ArrayList<String>());
            }

            WriteResult response = builder.build();
            if (response.getResult() == Result.OK && distributed)
                consensus(); // consensus checking
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Delete a file.
         * This has the same semantics as ModifyFile, except that both the
         * client and server will not specify a blocklist or missing blocks.
         * As in ModifyFile, this call should return an error if the server
         * it is called on isn't the leader
         * </pre>
         */
        public void deleteFile(surfstore.SurfStoreBasic.FileInfo request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.WriteResult> responseObserver) {
            String fileName = request.getFilename(); // fileName in question
            int version = request.getVersion(); // version number in question
            WriteResult.Builder builder = WriteResult.newBuilder();

            if (leader) {
                if (fileVersionMap.containsKey(fileName)) // that means file did exist in the past
                {
                    if (version != (fileVersionMap.get(fileName) + 1)) // check for version number
                        builder.setResult(WriteResult.Result.OLD_VERSION);
                    else // everything seems to be good
                    {
                        fileVersionMap.put(fileName, version); // version number updated
                        ArrayList<String> zeroList = new ArrayList<>();
                        zeroList.add("0");
                        hashListMap.put(fileName, zeroList); // hashList set to null
                        builder.setResult(WriteResult.Result.OK);

                        // since this is okay, time to create FileInfoStruct
                        fileInfoCreator(fileName, version, zeroList);
                    }

                    builder.setCurrentVersion(fileVersionMap.get(fileName)); // set the fileVersion Number
                } // done with if file existed in the meta prior

                else // file does not exist
                {
                    builder.setResult(WriteResult.Result.OLD_VERSION);
                    builder.setCurrentVersion(0);
                }
            } // done with yes, leader

            else // not leader
            {
                builder.setResult(WriteResult.Result.NOT_LEADER);
                if (fileVersionMap.containsKey(fileName))
                    builder.setCurrentVersion(fileVersionMap.get(fileName));
                else
                    builder.setCurrentVersion(0); // file does not exist
            }

            WriteResult response = builder.build();
            if (distributed && response.getResult() == Result.OK)
                consensus(); // consensus checking
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Query whether the MetadataStore server is currently the leader.
         * This call should work even when the server is in a "crashed" state
         * </pre>
         */
        public void isLeader(surfstore.SurfStoreBasic.Empty request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {
            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(leader).build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * "Crash" the MetadataStore server.
         * Until Restore() is called, the server should reply to all RPCs
         * with an error (except Restore) and not send any RPCs to other servers.
         * </pre>
         */
        public void crash(surfstore.SurfStoreBasic.Empty request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            alive = false;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * "Restore" the MetadataStore server, allowing it to start
         * sending and responding to all RPCs once again.
         * </pre>
         */
        public void restore(surfstore.SurfStoreBasic.Empty request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.Empty> responseObserver) {
            alive = true;
            Empty response = Empty.newBuilder().build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         * Find out if the node is crashed or not
         * (should always work, even if the node is crashed)
         * </pre>
         */
        public void isCrashed(surfstore.SurfStoreBasic.Empty request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            SimpleAnswer response = SimpleAnswer.newBuilder().setAnswer(!alive).build(); // oppositive of alive state
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        /**
         * <pre>
         *added this RPC
         * </pre>
         */
        public void updateFollower(surfstore.SurfStoreBasic.FileInfo request,
                io.grpc.stub.StreamObserver<surfstore.SurfStoreBasic.SimpleAnswer> responseObserver) {

            String fileName = request.getFilename();
            int version = request.getVersion();
            ArrayList<String> hashList = new ArrayList<>(); // hashLists which should exist
            for (int i = 0; i < request.getBlocklistCount(); i++)
                hashList.add(request.getBlocklist(i)); // this adds all the blocks to the blockserver

            if (alive) {
                logger.info("File updated: " + fileName + " with version: " + version);
                fileVersionMap.put(fileName, version); // update the follower version
                hashListMap.put(fileName, hashList); // update the follower hashList
            }

            SimpleAnswer.Builder builder = SimpleAnswer.newBuilder();
            builder.setAnswer(alive); // if alive, updated happened otherwise no update

            SimpleAnswer response = builder.build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }

        protected class FileInfoStruct {
            protected String fileName;
            protected int version;
            protected ArrayList<String> hashList;
        }

        // stall in the following:
        // 1) both back-ups are down
        // 2) either back-up is alive and its log count is > 0
        private void consensus() {
            while (metadataStubA.isCrashed(Empty.newBuilder().build()).getAnswer()
                    && metadataStubB.isCrashed(Empty.newBuilder().build()).getAnswer()
                    || !metadataStubA.isCrashed(Empty.newBuilder().build()).getAnswer() && logA.size() > 0
                    || !metadataStubB.isCrashed(Empty.newBuilder().build()).getAnswer() && logB.size() > 0)
                continue;
        }

        private Block blockRequest(String hash) {

            return Block.newBuilder().setHash(hash).build();
        }

        // made this function synchronized
        private void fileInfoCreator(String fileName, int version, ArrayList<String> hashList) {
            FileInfoStruct fileInfo = new FileInfoStruct();
            fileInfo.fileName = fileName;
            fileInfo.version = version;
            fileInfo.hashList = hashList;
            if (distributed) {
                logA.add(fileInfo);
                logB.add(fileInfo);
            }
        }
    }
}