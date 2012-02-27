package com.splunk.shep.archiver.archive;

import java.io.File;

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
	int status = 0;
	File directory = new File(path);
	if (!directory.isDirectory()) {
	    status = 3;
	} else {
	    File safeLocation = moveDirectoryToASafeLocation(directory);
	    if (safeLocation != null) {
		doRestCall(safeLocation);
	    } else {
		status = 4;
	    }
	}
	return status;
    }

    private void doRestCall(File safeLocation) {
	// TODO Auto-generated method stub

    }

    private File moveDirectoryToASafeLocation(File directory) {
	File safeLocation = createSafeLocation();
	File locationToMoveTo = new File(safeLocation, directory.getName());
	boolean isMoved = directory.renameTo(locationToMoveTo);
	if (!isMoved) {
	    locationToMoveTo = null;
	}
	return locationToMoveTo;

    }

    private File createSafeLocation() {
	File safeLocation = new File(safeLocationForBuckets);
	safeLocation.mkdirs();
	return safeLocation;
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
