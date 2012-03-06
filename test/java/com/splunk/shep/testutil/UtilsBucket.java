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
	File bucketDir = createFileFormatedAsBucket(bucketName);
	return createBucketWithIndexInDirectory(index, bucketDir);
    }

    private static Bucket createBucketWithIndexInDirectory(String index,
	    File bucketDir) {
	Bucket testBucket = null;
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
	return formatDirectoryToBeABucket(bucketDir);
    }

    private static File formatDirectoryToBeABucket(File bucketDir) {
	UtilsFile.createDirectoryInParent(bucketDir, "rawdata");
	return bucketDir;
    }

    /**
     * @param parent
     *            directory to create bucket in.
     * @return bucket created in parent.
     */
    public static Bucket createBucketInDirectory(File parent) {
	return createBucketInDirectoryWithIndex(parent, randomIndexName());
    }

    /**
     * @param parent
     *            directory to create bucket in.
     * @return bucket created in parent.
     */
    public static Bucket createBucketInDirectoryWithIndex(File parent,
	    String index) {
	File bucketDir = createFileFormatedAsBucketInDirectory(parent);
	return createBucketWithIndexInDirectory(index, bucketDir);
    }

    /**
     * @param parent
     *            to create the bucket in.
     * @return File with the format of a bucket
     */
    private static File createFileFormatedAsBucketInDirectory(File parent) {
	File child = UtilsFile.createDirectoryInParent(parent,
		randomBucketName());
	return formatDirectoryToBeABucket(child);
    }
}
