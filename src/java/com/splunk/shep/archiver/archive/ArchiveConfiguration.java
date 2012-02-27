package com.splunk.shep.archiver.archive;

public class ArchiveConfiguration {

    public BucketFormat getArchiveFormat() {
	return BucketFormat.SPLUNK_BUCKET;
    }

    public String getArchivingRoot() {
	throw new UnsupportedOperationException();
    }

    public String getClusterName() {
	throw new UnsupportedOperationException();
    }

    public String getServerName() {
	throw new UnsupportedOperationException();
    }

}
