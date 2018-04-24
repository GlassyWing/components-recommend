import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

@RunWith(JUnit4.class)
public class SimpleTest {

    @Test
    public void getCpuInfo() throws NoSuchAlgorithmException, UnsupportedEncodingException {
        String compsName = "ARRATYUITLS.insertData";
        String hdigest = new DigestUtils(MessageDigestAlgorithms.MD5).digestAsHex(compsName);
        System.out.println(hdigest.substring(0, 16));
    }

    @Test
    public void sortTest() {
        List<byte[]> sorted = Arrays.asList("822a79790f5c6f8b-ARRAYUTILS.insertData".getBytes()
                , "822a79790f5c6f8b-ARRAYUTILS.insertOneData".getBytes()
                , "822a79790f5c6f8b-ARRAYUTILS.insertTwoData".getBytes()
                , "822a79790f5c6f8b-ARRAYUTILS.deleteData".getBytes()
                , "822a79790f5c6f8b-ARRAYUTILS.deleteOneData".getBytes()
                , "822a79790f5c6f8b-ARRAYUTILS.deleteTwoData".getBytes()
        );
        sorted.sort(Bytes::compareTo);
        sorted.forEach(bytes -> System.out.println(Bytes.toString(bytes)));
    }
}
