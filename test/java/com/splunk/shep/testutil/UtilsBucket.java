package com.splunk.shep.testutil;

import java.io.File;

import org.apache.commons.lang.math.RandomUtils;
import org.testng.AssertJUnit;

import com.splunk.shep.archiver.model.Bucket;

/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class UtilsBucket {

    /**
     * @return A bucket with random bucket and index names.
     * @see #createTestBucketWithName(String, String)
     */
    public static Bucket createTestBucket() {
	return createTestBucketWithIndexAndName(randomIndexName(),
		randomBucketName());

    }

    private static String randomIndexName() {
	return "index-" + System.currentTimeMillis();
    }

    private static String randomBucketName() {
	long latest = System.currentTimeMillis();
	long earliest = latest - RandomUtils.nextInt(10000);
	return String.format("db_%d_%d_%d", earliest, latest,
		RandomUtils.nextInt(1000));
    }

    /**
     * @return A bucket with specified index and bucket names.
     */
    public static Bucket createTestBucketWithIndexAndName(String index,
	    String bucketName) {
	Bucket testBucket = null;
	File bucketDir = createFileFormatedAsBucket(bucketName);
	try {
	    testBucket = new Bucket(index, bucketDir);
	} catch (Exception e) {
	    UtilsTestNG.failForException("Couldn't create a test bucket", e);
	    throw new RuntimeException(
		    "There was a UtilsTestNG.failForException() method call above me that stoped me from happening. Where did it go?");
	}
	AssertJUnit.assertNotNull(testBucket);

	return testBucket;

    }

    /**
     * @return A directory formated as a bucket.
     */
    public static File createFileFormatedAsBucket(String bucketName) {
	File bucketDir = UtilsFile.createTmpDirectoryWithName(bucketName);
	UtilsFile.createDirectoryInParent(bucketDir, "rawdata");
	return bucketDir;
    }

}
