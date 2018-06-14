package surfstore;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class HashUtils {

	public static String sha256(String string) {

		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(2);
		}
		byte[] encodedhash = digest.digest(string.getBytes(StandardCharsets.UTF_8));
		String encoded = Base64.getEncoder().encodeToString(encodedhash);
		return encoded;
	}

	public static String sha256(byte[] bytesArray) {

		MessageDigest digest = null;
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			System.exit(2);
		}
		byte[] encodedhash = digest.digest(bytesArray);
		String encoded = Base64.getEncoder().encodeToString(encodedhash);
		return encoded;
		
	}

}
