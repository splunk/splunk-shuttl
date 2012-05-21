package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.FileOverwriteException;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Class for transferring buckets
 */
public class ArchiveBucketTransferer {

    private final ArchiveFileSystem archiveFileSystem;
    private final static Logger logger = Logger
	    .getLogger(ArchiveBucketTransferer.class);

    public ArchiveBucketTransferer(ArchiveFileSystem archive) {
	archiveFileSystem = archive;
    }

    /**
     * Transfers the bucket and its content to the archive.
     * 
     * @param bucket
     *            to transfer to {@link ArchiveFileSystem}
     * @param destination
     *            on the {@link ArchiveFileSystem}
     */
    public void transferBucketToArchive(Bucket bucket, URI destination) {
	logger.info(will("attempting to transfer bucket to archive", "bucket",
		bucket, "destination", destination));
	try {
	    archiveFileSystem.putFileAtomically(bucket.getDirectory(), destination);
	} catch (FileNotFoundException e) {
	    logger.error(did("attempted to transfer bucket to archive",
		    "bucket path does not exist", "success", "bucket", bucket,
		    "destination", destination, "exception", e));
	    throw new RuntimeException(e);
	} catch (FileOverwriteException e) {
	    logger.error(did(
		    "attempted to transfer bucket to archive",
		    "a bucket with the same path already exists on the filesystem",
		    "success", "bucket", bucket, "destination", destination,
		    "exception", e));
	    throw new RuntimeException(e);
	} catch (IOException e) {
	    logger.error(did("attempted to transfer bucket to archive",
		    "IOException raised", "success", "bucket", bucket,
		    "destination", destination, "exception", e));
	    throw new RuntimeException(e);
	}
    }

}
