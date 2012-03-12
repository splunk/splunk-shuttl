package com.splunk.shep.archiver.archive;

public class ArchiveConfiguration {

    private static final String ARCHIVING_ROOT = "archiving_root";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String SERVER_NAME = "server_name";

    public BucketFormat getArchiveFormat() {
	return BucketFormat.SPLUNK_BUCKET;
    }

    public String getArchivingRoot() {
	return ARCHIVING_ROOT;
    }

    public String getClusterName() {
	return CLUSTER_NAME;
    }

    public String getServerName() {
	return SERVER_NAME;
    }
}
