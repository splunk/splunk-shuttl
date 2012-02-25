package com.splunk.shep.archiver.mbeans;

import java.io.File;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;

/**
 * REST end point for archiving a bucket.
 */
public interface BucketArchiverMBean {

    /**
     * Archive bucket in the {@link ArchiveFileSystem}.
     * 
     * @param bucket
     *            to be archived. Currently represented as a {@link File}, which
     *            might change at any time. The bucket should be a directory.
     */
    void archiveBucket(File bucket);

}
