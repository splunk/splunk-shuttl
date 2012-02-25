package com.splunk.shep.archiver.archive;

import java.io.File;

public class ArchiveBucket {

    public static final String SAFE_LOCATION = System.getProperty("user.home")
	    + "/SafeBucketArchive";

    public static void main(String[] args) {
	if (args.length == 0)
	    System.exit(1);
	if (args.length >= 2)
	    System.exit(2);

	File directory = new File(args[0]);
	if (!directory.isDirectory())
	    System.exit(3);

	moveDirectoryToASafeLocation(directory);

	System.exit(0);
    }

    private static void moveDirectoryToASafeLocation(File directory) {
	File safeLocation = new File(SAFE_LOCATION);
	safeLocation.mkdirs();
	boolean renamed = directory.renameTo(new File(safeLocation, directory
		.getName()));
	// System.exit(4) isn't testable since SAFE_LOCATION is hard coded,
	// and can't be configured.
	if (!renamed)
	    System.exit(4);
    }

}
