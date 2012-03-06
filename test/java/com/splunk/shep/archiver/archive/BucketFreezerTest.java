package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

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

    private Runtime runtimeMock;
    private HttpClient okReturningHttpClient = null;

    @BeforeClass
    public void beforeClass() throws ClientProtocolException, IOException {
	okReturningHttpClient = UtilsMockito
		.createAlwaysOKReturningHTTPClientMock();
	runtimeMock = mock(Runtime.class);
    }

    @AfterMethod(groups = { "fast" })
    public void tearDownFast() {
	reset(runtimeMock);
	deleteDirectory(getTestDirectory());
	deleteDirectory(new File(getSafeLocation()));
    }

    private void deleteDirectory(File dir) {
	try {
	    FileUtils.deleteDirectory(dir);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    public void testDirectory_shouldNot_exist() {
	assertFalse(getTestDirectory().exists());
    }

    public void main_existingDirecotry_returnCode0() throws IOException {
	File directory = createTestDirectory();
	runMainWithDepentencies_withArguments("index-name",
		directory.getAbsolutePath());
	verify(runtimeMock).exit(0);
	// This deletion assumes and knows too much. When we remove
	// BucketFreezer, this will be ok tho.
	FileUtils
		.deleteDirectory(new File(BucketFreezer.DEFAULT_SAFE_LOCATION));
    }

    public void main_noArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments();
	verify(runtimeMock).exit(-1);
    }

    public void main_oneArgument_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name");
	verify(runtimeMock).exit(-1);
    }

    public void main_threeArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name", "/path/to/file",
		"too-many-arguments");
	verify(runtimeMock).exit(-1);
    }

    public void main_tooManyArguments_returnCodeMinus1() {
	runMainWithDepentencies_withArguments("index-name", "/path/to/file",
		"too-many-arguments", "too", "many", "arguments");
	verify(runtimeMock).exit(-1);
    }

    public void main_fileNotADirectory_returnCodeMinus2() throws IOException {
	File file = File.createTempFile("ArchiveTest", ".tmp");
	file.deleteOnExit();
	assertTrue(!file.isDirectory());
	runMainWithDepentencies_withArguments("index-name",
		file.getAbsolutePath());
	verify(runtimeMock).exit(-2);
    }

    public void main_nonExistingFile_returnMinus3() {
	File file = UtilsFile.createTestFilePath();
	runMainWithDepentencies_withArguments("index-name",
		file.getAbsolutePath());
	verify(runtimeMock).exit(-3);
    }

    private void runMainWithDepentencies_withArguments(String... args) {
	BucketFreezer bucketFreezer = BucketFreezer
		.createWithDeafultSafeLocationAndHTTPClient();
	bucketFreezer.httpClient = okReturningHttpClient;
	BucketFreezer.runMainWithDepentencies(runtimeMock, bucketFreezer, args);
    }

    public void createWithDeafultSafeLocationAndHTTPClient_initialize_nonNullValue() {
	assertNotNull(BucketFreezer
		.createWithDeafultSafeLocationAndHTTPClient());
    }

    public void should_moveDirectoryToaSafeLocation_when_givenPath()
	    throws IOException {
	File safeLocationDirectory = null;
	try {
	    safeLocationDirectory = UtilsFile.createTempDirectory();

	    BucketFreezer bucketFreezer = new BucketFreezer(
		    safeLocationDirectory.getAbsolutePath(),
		    okReturningHttpClient, null);
	    File dirToBeMoved = createTestDirectory();

	    // Test
	    int exitStatus = bucketFreezer.freezeBucket("index-name",
		    dirToBeMoved.getAbsolutePath());
	    assertEquals(0, exitStatus);

	    // Verify
	    assertTrue(!dirToBeMoved.exists());
	    assertTrue(safeLocationDirectory.exists());
	    assertTrue(isDirectoryWithChildren(safeLocationDirectory));
	} finally {
	    if (safeLocationDirectory != null) {
		FileUtils.deleteDirectory(safeLocationDirectory);
	    }
	}
    }

    public void freezeBucket_givenNonExistingSafeLocation_createSafeLocation()
	    throws IOException {
	String notCreatedSafeLocation = getSafeLocation();
	File nonExistingSafeLocation = new File(notCreatedSafeLocation);
	assertTrue(!nonExistingSafeLocation.exists());
	BucketFreezer bucketFreezer = new BucketFreezer(notCreatedSafeLocation,
		okReturningHttpClient, null);
	File dirToBeMoved = createTestDirectory();

	// Test
	bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

	// Verify
	assertTrue(!dirToBeMoved.exists());
	assertTrue(nonExistingSafeLocation.exists());
    }

    public void freezeBucket_internalServerError_moveBucketToFailedBucketLocation()
	    throws IOException {
	File failedBucketLocation = null;
	try {
	    failedBucketLocation = UtilsFile.createTempDirectory();
	    HttpClient failingHttpClient = UtilsMockito
		    .createInternalServerErrorHttpClientMock();
	    BucketFreezer bucketFreezer = new BucketFreezer(getSafeLocation(),
		    failingHttpClient, failedBucketLocation.getAbsolutePath());
	    File dirToBeMoved = createTestDirectory();
	    assertFalse(isDirectoryWithChildren(failedBucketLocation));

	    // Test
	    bucketFreezer.freezeBucket("index", dirToBeMoved.getAbsolutePath());

	    assertTrue(isDirectoryWithChildren(failedBucketLocation));
	} finally {
	    if (failedBucketLocation != null) {
		FileUtils.deleteDirectory(failedBucketLocation);
	    }
	}
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
	    bucketFreezer.freezeBucket("index", createTestDirectory()
		    .getAbsolutePath());

	    assertTrue(nonExistingLocation.exists());

	} finally {
	    if (nonExistingLocation != null) {
		FileUtils.deleteDirectory(nonExistingLocation);
	    }
	}
    }

    private boolean isDirectoryWithChildren(File directory) {
	File[] listFiles = directory.listFiles();
	System.out.println(Arrays.toString(listFiles));
	return listFiles.length > 0;
    }

    /**
     * This location is torn down by the AfterMethod annotation.
     */
    private String getSafeLocation() {
	return System.getProperty("user.home") + "/" + getClass().getName();
    }

    /**
     * This location is torn down by the AfterMethod annotation.
     */
    private File getTestDirectory() {
	return new File(getClass().getSimpleName() + "-test-dir");
    }

    private File createTestDirectory() {
	return createDirectory(getTestDirectory());
    }

    private File createDirectory(File dir) {
	dir.mkdir();
	assertTrue(dir.exists());
	return dir;
    }

}
