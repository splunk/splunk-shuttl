package com.splunk.shep.archiver.archive;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsMockito;

@Test(groups = { "fast" })
public class BucketFreezerTest {

    private HttpClient okReturningHttpClient = null;

    @BeforeClass
    public void beforeClass() throws ClientProtocolException, IOException {
	okReturningHttpClient = UtilsMockito
		.createAlwaysOKReturningHTTPClientMock();
    }

    @AfterMethod(groups = { "fast" })
    public void tearDownFast() {
	FileUtils.deleteQuietly(new File(getSafeLocation()));
    }

    public void createWithDeafultSafeLocationAndHTTPClient_initialize_nonNullValue() {
	assertNotNull(BucketFreezer
		.createWithDeafultSafeLocationAndHTTPClient());
    }

    public void should_moveDirectoryToaSafeLocation_when_givenPath()
	    throws IOException {
	File safeLocationDirectory = UtilsFile.createTempDirectory();

	BucketFreezer bucketFreezer = new BucketFreezer(
		safeLocationDirectory.getAbsolutePath(), okReturningHttpClient,
		null);
	File dirToBeMoved = createTempDirectory();

	// Test
	int exitStatus = bucketFreezer.freezeBucket("index-name",
		dirToBeMoved.getAbsolutePath());
	assertEquals(0, exitStatus);

	// Verify
	assertTrue(!dirToBeMoved.exists());
	assertTrue(safeLocationDirectory.exists());
	assertTrue(!isDirectoryEmpty(safeLocationDirectory));
    }

    public void freezeBucket_givenNonExistingSafeLocation_createSafeLocation()
	    throws IOException {
	String notCreatedSafeLocation = getSafeLocation();
	File nonExistingSafeLocation = new File(notCreatedSafeLocation);
	assertTrue(!nonExistingSafeLocation.exists());
	BucketFreezer bucketFreezer = new BucketFreezer(notCreatedSafeLocation,
		okReturningHttpClient, null);
	File dirToBeMoved = createTempDirectory();

	// Test
	bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

	// Verify
	assertTrue(!dirToBeMoved.exists());
	assertTrue(nonExistingSafeLocation.exists());
    }

    public void freezeBucket_internalServerError_moveBucketToFailedBucketLocation()
	    throws IOException {
	File failedBucketLocation = UtilsFile.createTempDirectory();
	HttpClient failingHttpClient = UtilsMockito
		.createInternalServerErrorHttpClientMock();
	BucketFreezer bucketFreezer = new BucketFreezer(getSafeLocation(),
		failingHttpClient, failedBucketLocation.getAbsolutePath());
	File dirToBeMoved = createTempDirectory();
	assertFalse(!isDirectoryEmpty(failedBucketLocation));

	// Test
	bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

	assertTrue(!isDirectoryEmpty(failedBucketLocation));
    }

    public void freezeBucket_internalServerError_createFailedBucketLocation()
	    throws IOException {
	File nonExistingLocation = null;
	try {
	    nonExistingLocation = new File("nonExistingLocation");
	    HttpClient failingHttpClient = UtilsMockito
		    .createInternalServerErrorHttpClientMock();
	    BucketFreezer bucketFreezer = new BucketFreezer(getSafeLocation(),
		    failingHttpClient, nonExistingLocation.getAbsolutePath());
	    assertTrue(!nonExistingLocation.exists());

	    // Test
	    bucketFreezer.freezeBucket("index", createTempDirectory()
		    .getAbsolutePath());

	    assertTrue(nonExistingLocation.exists());

	} finally {
	    FileUtils.deleteQuietly(nonExistingLocation);
	}
    }

    /**
     * This location is torn down by the AfterMethod annotation.
     */
    private String getSafeLocation() {
	return FileUtils.getUserDirectoryPath() + "/" + getClass().getName();
    }

}
