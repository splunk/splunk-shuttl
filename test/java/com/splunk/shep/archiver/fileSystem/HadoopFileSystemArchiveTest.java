package com.splunk.shep.archiver.fileSystem;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsFileSystem;
import com.splunk.shep.testutil.UtilsTestNG;

/**
 * Using the method naming convention:
 * [metodNamn]_[stateUnderTest]_[expectedOutcome]
 */
@Test(groups = { "fast" })
public class HadoopFileSystemArchiveTest {

    private FileSystem fileSystem;
    private HadoopFileSystemArchive hadoopFileSystemArchive;
    private HadoopFileSystemPutter hadoopFileSystemPutter;

    @BeforeMethod
    public void beforeMethod() {
	fileSystem = UtilsFileSystem.getLocalFileSystem();
	hadoopFileSystemArchive = new HadoopFileSystemArchive(fileSystem);
	hadoopFileSystemPutter = HadoopFileSystemPutter.create(fileSystem);
    }

    @AfterMethod
    public void afterMethod() {
	hadoopFileSystemPutter.deleteMyFiles();
    }

    public void HadoopFileSystemArchive_notInitialized_aNonNullInstanceIsCreated() {
	assertNotNull(hadoopFileSystemArchive);
    }

    public void getFile_expectedBehavior_noErrors() throws IOException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	URI fileSystemPath = hadoopPath.toUri();
	File retrivedFile = UtilsFile.createTestFilePath();

	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
	UtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getFile_whenRemotefileDoNotExist_FileNotFoundException()
	    throws IOException,
	    URISyntaxException {
	URI fileSystemPath = new URI("file:///random/path/to/non/existing/file");
	File retrivedFile = UtilsFile.createTestFilePath();

	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
    }

    @Test(expectedExceptions = FileOverwriteException.class)
    public void getFile_whenLocalFileAllreadyExist_FileOverwriteException()
	    throws IOException, URISyntaxException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	URI fileSystemPath = hadoopPath.toUri();
	File retrivedFile = UtilsFile.createTestFileWithRandomContent();

	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
    }

    @Test(enabled = false)
    public void listPath() {
	throw new RuntimeException("Test not implemented");
    }

    @Test(enabled = false)
    public void putFile() {
	throw new RuntimeException("Test not implemented");
    }
}
