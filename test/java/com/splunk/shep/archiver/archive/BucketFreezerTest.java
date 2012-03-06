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
	    String safeLocation = System.getProperty("user.home") + "/"
		    + getClass().getName();
	    safeLocationDirectory = new File(safeLocation);
	    assertTrue(!safeLocationDirectory.exists());

	    BucketFreezer bucketFreezer = new BucketFreezer(
		    safeLocationDirectory.getAbsolutePath(),
		    okReturningHttpClient);
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
	    FileUtils.deleteDirectory(safeLocationDirectory);
	}
    }

    private boolean isDirectoryWithChildren(File directory) {
	File[] listFiles = directory.listFiles();
	System.out.println(Arrays.toString(listFiles));
	return listFiles.length > 0;
    }

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
