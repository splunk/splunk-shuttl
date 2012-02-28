package com.splunk.shep.archiver.archive;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

public class BucketFreezer {

    public static final String DEFAULT_SAFE_LOCATION = System
	    .getProperty("user.home") + "/" + BucketFreezer.class.getName();

    private final String safeLocationForBuckets;

    public BucketFreezer() {
	this.safeLocationForBuckets = DEFAULT_SAFE_LOCATION;
    }

    protected BucketFreezer(String safeLocationForBuckets) {
	this.safeLocationForBuckets = safeLocationForBuckets;
    }

    public int freezeBucket(String path) {
	try {
	    moveAndArchiveBucket(path);
	    return 0;
	} catch (IOException e) {
	    System.err.println(e.getMessage());
	    return 3;
	}
    }

    private void moveAndArchiveBucket(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = Bucket.createWithAbsolutePath(path);
	Bucket safeBucket = bucket.moveBucketToDir(createSafeLocation());
	doRestCall(safeBucket);
    }

    private File createSafeLocation() {
	File safeLocation = new File(safeLocationForBuckets);
	safeLocation.mkdirs();
	return safeLocation;
    }

    private void doRestCall(Bucket bucket) {
	// TODO Auto-generated method stub

    }

    public static void main(String[] args) {
	if (args.length == 0)
	    System.exit(1);
	if (args.length >= 2)
	    System.exit(2);
	BucketFreezer archiveBucket = new BucketFreezer();
	int exitStatus = archiveBucket.freezeBucket(args[0]);
	System.exit(exitStatus);
    }

}
