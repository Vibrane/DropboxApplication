package surfstore;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import surfstore.SurfStoreBasic.Block;
import surfstore.SurfStoreBasic.Block.Builder;
import surfstore.SurfStoreBasic.FileInfo;
import surfstore.SurfStoreBasic.WriteResult;
import surfstore.SurfStoreBasic.WriteResult.Result;

public final class Client {
	// private static final Logger logger =
	// Logger.getLogger(Client.class.getName());

	private final ManagedChannel metadataChannel;
	private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub;

	private final ManagedChannel metadataChannel2;
	private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub2;

	private final ManagedChannel metadataChannel3;
	private final MetadataStoreGrpc.MetadataStoreBlockingStub metadataStub3;

	private final ManagedChannel blockChannel;
	private final BlockStoreGrpc.BlockStoreBlockingStub blockStub;

	private final ConfigReader config;

	public Client(ConfigReader config) {
		this.metadataChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(1))
				.usePlaintext(true).build();
		this.metadataStub = MetadataStoreGrpc.newBlockingStub(metadataChannel);

		if (config.getNumMetadataServers() > 1) {
			this.metadataChannel2 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(2))
					.usePlaintext(true).build();
			this.metadataStub2 = MetadataStoreGrpc.newBlockingStub(metadataChannel2);

			this.metadataChannel3 = ManagedChannelBuilder.forAddress("127.0.0.1", config.getMetadataPort(3))
					.usePlaintext(true).build();
			this.metadataStub3 = MetadataStoreGrpc.newBlockingStub(metadataChannel3);
		} else {
			this.metadataChannel2 = null;
			this.metadataStub2 = null;
			this.metadataChannel3 = null;
			this.metadataStub3 = null;
		}

		this.blockChannel = ManagedChannelBuilder.forAddress("127.0.0.1", config.getBlockPort()).usePlaintext(true)
				.build();
		this.blockStub = BlockStoreGrpc.newBlockingStub(blockChannel);

		this.config = config;
	}

	public void shutdown() throws InterruptedException {
		metadataChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		blockChannel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
	}

	private static Namespace parseArgs(String[] args) {
		ArgumentParser parser = ArgumentParsers.newFor("Client").build().description("Client for SurfStore");
		parser.addArgument("config_file").type(String.class).help("Path to configuration file");
		parser.addArgument("command").type(String.class).help("Command client is trying to execute");
		parser.addArgument("fileName").type(String.class).help("File we are dealing with");
		if (args.length == 4)
			parser.addArgument("downloadFolder").type(String.class).help("Path to download folder");

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
		if (c_args == null)
			throw new RuntimeException("Argument parsing failed");

		File configf = new File(c_args.getString("config_file"));
		ConfigReader config = new ConfigReader(configf);

		Client client = new Client(config);
		String fileName = c_args.getString("fileName");
		String storeFilePath = c_args.getString("downloadFolder");
		String command = c_args.getString("command");

		try {
			switch (command) {
			case "upload":
				client.upload(fileName);
				break;
			case "download":
				client.download(fileName, storeFilePath);
				break;
			case "delete":
				client.delete(fileName);
				break;
			case "getversion":
				client.getVersion(fileName);
				break;
			}
		} finally {
			client.shutdown();
		}
	}

	private String simplifyFileName(String string) {

		int lastSlashLoc = string.lastIndexOf('/');
		if (lastSlashLoc > -1)
			string = string.substring(++lastSlashLoc);
		return string;
	}

	// Returns the current version of the given file. If SurfStore is centralized,
	// then it should return a single value. If SurfStore is distributed, it should
	// return three values separated by spaces.
	private void getVersion(String fileName) {
		FileInfo.Builder fileBuilder = FileInfo.newBuilder();
		fileBuilder.setFilename(simplifyFileName(fileName));
		FileInfo fileRequest = fileBuilder.build();

		if (config.getNumMetadataServers() == 1) {
			int version = metadataStub.readFile(fileRequest).getVersion();
			System.out.println(version);
		} else {
			int[] list = { metadataStub.readFile(fileRequest).getVersion(),
					metadataStub2.readFile(fileRequest).getVersion(),
					metadataStub3.readFile(fileRequest).getVersion() };
			System.out.println(list[0] + " " + list[1] + " " + list[2]);
		}
	}

	// Signals the MetadataStore to delete a file.
	private void delete(String fileName) {
		fileName = simplifyFileName(fileName);
		FileInfo.Builder fileBuilder = FileInfo.newBuilder();
		fileBuilder.setFilename(simplifyFileName(fileName));
		FileInfo fileInfo = fileBuilder.build();

		fileInfo = metadataStub.readFile(fileInfo);

		if (fileInfo.getVersion() == 0)
			System.out.println("Not Found");
		else if (fileInfo.getBlocklistCount() == 1 && fileInfo.getBlocklist(0).equals("0"))
			System.out.println("ALREADY DELETED");
		else {
			fileBuilder = FileInfo.newBuilder();
			fileBuilder.setFilename(simplifyFileName(fileName));
			fileBuilder.setVersion(fileInfo.getVersion() + 1);
			fileInfo = fileBuilder.build();
			Result res = metadataStub.deleteFile(fileInfo).getResult();
			if (res == WriteResult.Result.OK)
				System.out.println("OK");
			else if (res == WriteResult.Result.NOT_LEADER)
				System.out.println("NOT LEADER");
			else if (res == WriteResult.Result.OLD_VERSION)
				System.out.println("OLD VERSION");
		}
	}

	private void download(String fileName, String storeFilePath) {

		if (storeFilePath.charAt(storeFilePath.length() - 1) != '/')
			storeFilePath = storeFilePath + "/";

		String[] fileList = new File(storeFilePath).list(); // could be empty or have some files already in

		for (int i = 0; i < fileList.length; i++)
			fileList[i] = storeFilePath + fileList[i];

		// STEP 1
		// traverse through all the files in the download directory and create a
		// blockMap

		Map<String, byte[]> blockMap = new ConcurrentHashMap<>();

		for (String file : fileList) { // for each file in the download directory
			ArrayList<byte[]> filePartList = splitFile(file); // split the file into parts
			for (byte[] chunkFile : filePartList)
				blockMap.put(HashUtils.sha256(chunkFile), chunkFile);
		} // done indexing the download directory

		// STEP 2
		// read from metadatastore, what the hashList is

		FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
		fileInfoBuilder.setFilename(simplifyFileName(fileName));
		FileInfo fileResponse = metadataStub.readFile(fileInfoBuilder.build());

		if (fileResponse.getVersion() == 0
				|| fileResponse.getBlocklistCount() == 1 && fileResponse.getBlocklist(0).equals("0"))
			System.out.println("Not Found");
		else {
			ArrayList<String> hashList = new ArrayList<>(); // hashLists which should exist
			for (int i = 0; i < fileResponse.getBlocklistCount(); i++)
				hashList.add(fileResponse.getBlocklist(i));

			for (String hashData : hashList)
				if (blockMap.containsKey(hashData) == false) // meaning blockMap does not have it
				{
					Block block = blockStub.getBlock(Block.newBuilder().setHash(hashData).build());
					blockMap.put(block.getHash(), block.getData().toByteArray());
				} // done adding the blocks to local blockMap for the blocks we did not have

			mergeFile(blockMap, hashList, storeFilePath + fileName);
			System.out.println("OK");
		}
	}

	private void mergeFile(Map<String, byte[]> blockMap, ArrayList<String> hashList, String file) {

		FileOutputStream outputFile = null;
		try {
			outputFile = new FileOutputStream(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}

		for (String hash : hashList)
			try {
				outputFile.write(blockMap.get(hash));
			} catch (IOException e) {
				e.printStackTrace();
			}

	}

	// upload : Reads the local file, creates a set of hashed blocks and uploads
	// them onto the MetadataStore (and potentially the BlockStore if they were not
	// already present there).
	private void upload(String fileName) {

		ArrayList<byte[]> fileList = splitFile(fileName);
		// now we have all the files
		ArrayList<Block> blockList = new ArrayList<>();
		ArrayList<String> blockHashList = new ArrayList<String>();
		boolean modified = false;

		// first convert all the files to the corresponding blocks
		for (byte[] file : fileList)
			blockList.add(fileToBlock(file));

		// now for each block in the blocklist
		for (Block block : blockList) {
			// upload block if does not exist
			if (blockStub.hasBlock(block).getAnswer() == false)
				blockStub.storeBlock(block);
			blockHashList.add(block.getHash()); // add block to the hashList
		}

		// first checking the current hashList of the file in the metaDataServer

		FileInfo.Builder fileInfoBuilder = FileInfo.newBuilder();
		fileInfoBuilder.setFilename(simplifyFileName(fileName));

		FileInfo fileResponse = metadataStub.readFile(fileInfoBuilder.build());

		ArrayList<String> metaHashList = new ArrayList<>(); // hashLists which should exist
		for (int i = 0; i < fileResponse.getBlocklistCount(); i++)
			metaHashList.add(fileResponse.getBlocklist(i));

		// metaHashList = the list from the metaServer for the associated File
		// blockHashList = the list of hashes which to upload for the associated file

		if (metaHashList.size() != blockHashList.size())
			modified = true;
		else
			for (int i = 0; i < metaHashList.size(); i++)
				if (metaHashList.get(i).equals(blockHashList.get(i)) == false) {
					modified = true; // that means the hashList has been changed
					break;
				}
		if (modified) // if the file has indeed been modified
		{
			fileInfoBuilder = FileInfo.newBuilder();
			fileInfoBuilder.setFilename(simplifyFileName(fileName));
			fileInfoBuilder.setVersion(fileResponse.getVersion() + 1); // set to 1 version number higher
			fileInfoBuilder.addAllBlocklist(blockHashList);
			WriteResult result = metadataStub.modifyFile(fileInfoBuilder.build()); // modify the file then
			if (result.getResult() == Result.OK)
				System.out.println("OK");
		} else
			System.out.println("NOT UPLOADED");
	}

	private ArrayList<byte[]> splitFile(String fileName) {

		byte[] fileInBytes = null;
		try {
			fileInBytes = Files.readAllBytes(Paths.get(fileName));
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<byte[]> chunksofFile = new ArrayList<>();
		byte[] chunk; // 4096 or less file part
		int fileSize = fileInBytes.length; // complete File size
		int fileIndex = 0; // current index in File

		while (fileIndex < fileSize) {
			int difference = fileSize - fileIndex;

			if (difference > 4096) // if more than 4096 bytes left to parse
				difference = 4096; // set byte lenght to 5096

			chunk = new byte[difference]; // create a byte [] of size 'difference'

			for (int chunkIndex = 0; chunkIndex < difference; chunkIndex++, fileIndex++)
				chunk[chunkIndex] = fileInBytes[fileIndex];

			chunksofFile.add(chunk);
		}

		return chunksofFile;
	}

	private Block fileToBlock(byte[] fileArray) {

		Builder builder = Block.newBuilder();
		builder.setData(ByteString.copyFrom(fileArray));
		builder.setHash(HashUtils.sha256(fileArray));
		return builder.build();
	}

}
