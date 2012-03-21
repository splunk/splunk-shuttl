package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.testng.annotations.Test;

import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast" })
public class ArchiveBucketTransfererTest {

    @Test(groups = { "fast" })
    public void transferBucketToArchive_givenValidBucketAndUri_putBucketWithArchiveFileSystem() {
	ArchiveFileSystem archive = mock(ArchiveFileSystem.class);
	Bucket bucket = mock(Bucket.class);
	ArchiveBucketTransferer archiveBucketTransferer = new ArchiveBucketTransferer(
		archive);
	archiveBucketTransferer.transferBucketToArchive(bucket, getURI());
    }

    private URI getURI() {
	String uri = "file:/some/path";
	try {
	    return new URI(uri);
	} catch (URISyntaxException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not create uri: " + uri, e);
	    return null;
	}
    }
}
