package com.splunk.shep.testutil;

import java.io.File;

import com.splunk.shep.archiver.model.Bucket;

/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class UtilsBucket {

    public static File createBucketDirectoriesInDirectory(File parent) {
	File indexDir = UtilsFile.createDirectoryInParent(parent, getIndex());
	File dbDir = UtilsFile.createDirectoryInParent(indexDir, getDB());
	File bucketDir = UtilsFile.createDirectoryInParent(dbDir,
		getBucketName());
	return bucketDir;
    }

    public static String getIndex() {
	return "index";
    }

    public static String getDB() {
	return "db";
    }

    public static String getBucketName() {
	return "db_1326857236_1300677707_0";
    }

    public static Bucket createBucketInDirectory(File parent) {
	try {
	    return new Bucket(createBucketDirectoriesInDirectory(parent));
	} catch (Exception e) {
	    UtilsTestNG.failForException("Could not create bucket in parent: "
		    + parent, e);
	    return null;
	}
    }

    public static String createBucketPathInDirectory(File parent) {
	return createBucketDirectoriesInDirectory(parent).getAbsolutePath();
    }
}
