package com.splunk.shep.archiver.archive;

import java.io.File;

public class BucketFreezer {

    public static final String DEFAULT_SAFE_LOCATION = System
	    .getProperty("user.home") + "/" + BucketFreezer.class.getName();

    private final String safeLocationForBuckets;
    private int exit = -1;

    public BucketFreezer() {
	this.safeLocationForBuckets = DEFAULT_SAFE_LOCATION;
    }

    protected BucketFreezer(String safeLocationForBuckets) {
	this.safeLocationForBuckets = safeLocationForBuckets;
    }

    public void freezeBucket(String path) {
	File directory = new File(path);
	if (!directory.isDirectory())
	    exit = 3;
	else
	    moveDirectoryToASafeLocation(directory);
    }

    private void moveDirectoryToASafeLocation(File directory) {
	File safeLocation = createSafeLocation();
	File locationToMoveTo = new File(safeLocation, directory.getName());
	boolean isMoved = directory.renameTo(locationToMoveTo);
	if (!isMoved)
	    exit = 4;
	else
	    exit = 0;
    }

    private File createSafeLocation() {
	File safeLocation = new File(safeLocationForBuckets);
	safeLocation.mkdirs();
	return safeLocation;
    }

    public int getExitStatus() {
	return exit;
    }

    public static void main(String[] args) {
	if (args.length == 0)
	    System.exit(1);
	if (args.length >= 2)
	    System.exit(2);
	BucketFreezer archiveBucket = new BucketFreezer();
	archiveBucket.freezeBucket(args[0]);
	int exitStatus = archiveBucket.getExitStatus();
	System.exit(exitStatus);
    }

}
