package com.splunk.shep.archiver.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.FileOverwriteException;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Class for transferring buckets
 */
public class ArchiveBucketTransferer {

    private final ArchiveFileSystem archiveFileSystem;

    public ArchiveBucketTransferer(ArchiveFileSystem archive) {
	archiveFileSystem = archive;
    }

    /**
     * Transfers the bucket and its content to the archive.
     * 
     * @param bucket
     *            to transfer to {@link ArchiveFileSystem}
     * @param uri
     *            on the {@link ArchiveFileSystem}
     */
    public void transferBucketToArchive(Bucket bucket, URI uri) {
	try {
	    archiveFileSystem.putFileAtomically(bucket.getDirectory(), uri);
	} catch (FileNotFoundException e) {

	    throw new RuntimeException(e);
	} catch (FileOverwriteException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

}
