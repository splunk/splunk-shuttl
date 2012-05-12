package com.splunk.shep.testutil;

import static com.splunk.shep.testutil.UtilsFile.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Date;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.math.RandomUtils;
import org.testng.AssertJUnit;

import com.splunk.shep.archiver.archive.BucketExporterIntegrationTest;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class UtilsBucket {

    /* package-private */static final URL REAL_BUCKET_URL = BucketExporterIntegrationTest.class
	    .getResource("/splunk-buckets/db_1336330530_1336330530_0");

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
	File rawdata = UtilsFile.createDirectoryInParent(bucketDir, "rawdata");
	File slices = UtilsFile.createFileInParent(rawdata, "slices.dat");
	UtilsFile.populateFileWithRandomContent(slices);
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
	return createFileFormatedAsBucketInDirectoryWithName(parent,
		randomBucketName());
    }

    private static File createFileFormatedAsBucketInDirectoryWithName(
	    File parent, String bucketName) {
	File child = UtilsFile.createDirectoryInParent(parent, bucketName);
	return formatDirectoryToBeABucket(child);
    }

    /**
     * Creates test bucket with earliest and latest times in its name.
     */
    public static Bucket createBucketWithTimes(Date earliest, Date latest) {
	return createBucketWithIndexAndTimeRange(randomIndexName(), earliest,
		latest);
    }

    /**
     * Creates test bucket with earliest and latest times in its name and index.
     */
    public static Bucket createBucketWithIndexAndTimeRange(String index,
	    Date earliest, Date latest) {
	String name = getNameWithEarliestAndLatestTime(earliest, latest);
	return createTestBucketWithIndexAndName(index, name);
    }

    private static String getNameWithEarliestAndLatestTime(Date earliest,
	    Date latest) {
	return "db_" + latest.getTime() + "_" + earliest.getTime() + "_"
		+ randomIndexName();
    }

    /**
     * Creates test bucket with earliest and latest time in a directory.
     */
    public static Bucket createBucketInDirectoryWithTimes(File parent,
	    Date earliest, Date latest) {
	String bucketName = getNameWithEarliestAndLatestTime(earliest, latest);
	File bucketDir = createFileFormatedAsBucketInDirectoryWithName(parent,
		bucketName);
	return createBucketWithIndexInDirectory(randomIndexName(), bucketDir);
    }

    /**
     * Creates test bucket with specified name and random index.
     */
    public static Bucket createBucketWithName(String name) {
	return createTestBucketWithIndexAndName(randomIndexName(), name);
    }

    /**
     * @return
     */
    public static Bucket copyRealBucket() {
	try {
	    return doCopyRealBucket();
	} catch (Exception e) {
	    UtilsTestNG.failForException("Could not create real bucket", e);
	    return null;
	}
    }

    private static Bucket doCopyRealBucket() throws URISyntaxException,
	    FileNotFoundException, FileNotDirectoryException {
	File realBucketDir = new File(REAL_BUCKET_URL.toURI())
		.getAbsoluteFile();
	File tempDirectory = createTempDirectory();
	File copyBucketDir = createDirectoryInParent(tempDirectory,
		realBucketDir.getName());
	doTheCopy(realBucketDir, copyBucketDir);
	return new Bucket("index", copyBucketDir);
    }

    private static void doTheCopy(File realBucketDir, File copyBucketDir) {
	try {
	    FileUtils.copyDirectory(realBucketDir, copyBucketDir);
	} catch (IOException e) {
	    UtilsTestNG.failForException("Couldn't copy: " + realBucketDir
		    + ", to: " + copyBucketDir, e);
	}
    }
}
