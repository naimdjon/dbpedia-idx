package no.westerdals.dbpedia_idx;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5 {

    public static String hash(final String input) {
        try {
            final MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.reset();
            md5.update(input.getBytes("UTF-8"));
            final byte[] digest = md5.digest();
            return byteArrToHexString(digest);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            e.printStackTrace();
            return input;
        }
    }

    private static String byteArrToHexString(final byte[] bytes) {
        final StringBuilder buf = new StringBuilder();
        for (final byte aBArr : bytes) {
            final int unsigned = aBArr & 0xff;
            if (unsigned < 0x10)
                buf.append("0");
            buf.append(Integer.toHexString(unsigned));
        }
        return buf.toString();
    }
}
