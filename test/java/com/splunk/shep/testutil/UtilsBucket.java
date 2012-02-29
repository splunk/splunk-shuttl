package com.splunk.shep.testutil;

import java.io.File;


/**
 * Util for creating a physical and valid bucket on the file system.
 */
public class UtilsBucket {

    public static File createBucketInDirectory(File parent) {
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
}
