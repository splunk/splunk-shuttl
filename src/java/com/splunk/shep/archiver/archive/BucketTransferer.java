package com.splunk.shep.archiver.archive;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.FileOverwriteException;
import com.splunk.shep.archiver.model.Bucket;

public class BucketTransferer {

    private final ArchiveFileSystem archiveFileSystem;

    public BucketTransferer(ArchiveFileSystem archive) {
	archiveFileSystem = archive;
    }

    public void transferBucketToArchive(Bucket bucket, URI path) {
	try {
	    archiveFileSystem.putFile(bucket.getDirectory(), path);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (FileOverwriteException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    /**
     * Thaw an archived bucket.
     */
    public void transferBucketToThaw(Bucket bucket) {
	throw new UnsupportedOperationException();
    }
}
