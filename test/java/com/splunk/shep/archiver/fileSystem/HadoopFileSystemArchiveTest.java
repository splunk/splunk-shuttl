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
import com.splunk.shep.testutil.UtilsPath;
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

    public void getFile_validInput_fileShouldBeRetrived() throws IOException {
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
	    throws IOException, URISyntaxException {
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

    public void getFile_whenLocalFileAllreadyExist_localFileIsNotOverwritten()
	    throws IOException, URISyntaxException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	URI fileSystemPath = hadoopPath.toUri();
	File fileThatCouldBeOverwritten = UtilsFile
		.createTestFileWithRandomContent();
	File originalFile = UtilsFile
		.createTestFileWithContentsOfFile(fileThatCouldBeOverwritten);

	try {
	    // Test
	    hadoopFileSystemArchive.getFile(fileThatCouldBeOverwritten,
		    fileSystemPath);
	} catch (Exception e) { // Intentionally ignoring.
	}

	// Confirm
	UtilsTestNG.assertFileContentsEqual(originalFile,
		fileThatCouldBeOverwritten);

    }

    @Test(enabled = false)
    public void listPath() {
	throw new RuntimeException("Test not implemented");
    }

    public void putFile_validInput_fileShouldBePutToFilesSystem()
	    throws IOException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	Path hadoopPath = UtilsPath.getSafeDirectory(fileSystem);
	URI fileSystemPath = hadoopPath.toUri();

	// Test
	hadoopFileSystemArchive.putFile(testFile, fileSystemPath);

	// Confirm
	File retrivedFile = UtilsFileSystem.getFileFromFileSystem(fileSystem,
		hadoopPath);
	UtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);

    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void putFile_whenLocalFileDoNotExist_FileNotFoundException()
	    throws IOException {
	File testFile = UtilsFile.createTestFilePath();
	Path hadoopPath = UtilsPath.getSafeDirectory(fileSystem);
	URI fileSystemPath = hadoopPath.toUri();

	hadoopFileSystemArchive.putFile(testFile, fileSystemPath);
    }

    @Test(expectedExceptions = FileOverwriteException.class)
    public void putFile_whenRemoteFileExists_FileOverwriteException()
	    throws IOException {
	File fileThatWouldBeOwerwriten = UtilsFile
		.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
	Path hadoopPath = hadoopFileSystemPutter
		.getPathForFile(fileThatWouldBeOwerwriten);
	URI pathToRemoteFile = hadoopPath.toUri();
	File testFile = UtilsFile.createTestFileWithRandomContent();

	hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
    }

    public void putFile_whenRemoteFileExists_RemoteFileShouldNotBeOverwriten()
	    throws IOException {
	File fileThatWouldBeOwerwriten = UtilsFile
		.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
	Path hadoopPath = hadoopFileSystemPutter
		.getPathForFile(fileThatWouldBeOwerwriten);
	URI pathToRemoteFile = hadoopPath.toUri();

	File testFile = UtilsFile.createTestFileWithRandomContent();

	try {
	    hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
	} catch (FileOverwriteException e) {
	    // Intentionally ignoring.
	}
	File fileAfterPut = UtilsFile.createTestFilePath();
	hadoopFileSystemArchive.getFile(fileAfterPut, pathToRemoteFile);
	UtilsTestNG.assertFileContentsEqual(
		"Put shouln't have overwritten the file.",
		fileThatWouldBeOwerwriten, fileAfterPut);

    }
}
