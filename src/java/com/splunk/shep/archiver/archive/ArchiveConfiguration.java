package com.splunk.shep.archiver.archive;

import java.util.Arrays;
import java.util.List;

public class ArchiveConfiguration {

    private static final String ARCHIVING_ROOT = "archiving_root";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String SERVER_NAME = "server_name";


    public BucketFormat getArchiveFormat() {
	return BucketFormat.SPLUNK_BUCKET; // CONFIG
    }

    public String getArchivingRoot() {
	return ARCHIVING_ROOT; // CONFIG
    }

    public String getClusterName() {
	return CLUSTER_NAME; // CONFIG
    }

    public String getServerName() {
	return SERVER_NAME; // CONFIG
    }

    /**
     * List of bucket formats, where lower index means it has higher priority. <br/>
     * {@link ArchiveConfiguration#getBucketFormatPriority()}.get(0) has the
     * highest priority, while .get(length-1) has the least priority.
     */
    public List<BucketFormat> getBucketFormatPriority() {
	return Arrays.asList(BucketFormat.SPLUNK_BUCKET); // CONFIG
    }
}
