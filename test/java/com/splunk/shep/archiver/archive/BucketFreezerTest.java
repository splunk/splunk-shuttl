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

@Test(groups = {"fast"})
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

    public void main_existingDirecotry_returnCode0() {
	File directory = createTestDirectory();
	runMainWithDepentencies_withArguments(directory.getAbsolutePath());
	verify(runtimeMock).exit(0);
    }


    public void main_noArguments_returnCode1() {
	runMainWithDepentencies_withArguments();
	verify(runtimeMock).exit(1);
    }

    public void main_moreThanOneArgument_returnCode2() {
	runMainWithDepentencies_withArguments("one", "two");
	verify(runtimeMock).exit(2);
    }

    public void main_fileNotADirectory_returnCode3()
	    throws IOException {
	File file = File.createTempFile("ArchiveTest", ".tmp");
	file.deleteOnExit();
	assertTrue(!file.isDirectory());
	runMainWithDepentencies_withArguments(file.getAbsolutePath());
	verify(runtimeMock).exit(3);
    }

    public void main_nonExistingFile_returnCode4()
	    throws IOException {
	File file = UtilsFile.createTestFilePath();
	runMainWithDepentencies_withArguments(file.getAbsolutePath());
	verify(runtimeMock).exit(4);
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
	    int exitStatus = bucketFreezer.freezeBucket(dirToBeMoved
		    .getAbsolutePath());
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
