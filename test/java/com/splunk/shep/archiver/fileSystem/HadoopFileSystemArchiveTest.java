package com.splunk.shep.archiver.fileSystem;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

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
	// Test done in before
	// Confirm
	assertNotNull(hadoopFileSystemArchive);
    }

    public void getFile_validInput_fileShouldBeRetrived() throws IOException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	URI fileSystemPath = hadoopPath.toUri();
	File retrivedFile = UtilsFile.createTestFilePath();

	// Test
	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);

	// Confirm
	UtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);
    }

    @Test(expectedExceptions = FileNotFoundException.class)
    public void getFile_whenRemotefileDoNotExist_fileNotFoundException()
	    throws IOException, URISyntaxException {
	URI fileSystemPath = new URI("file:///random/path/to/non/existing/file");
	File retrivedFile = UtilsFile.createTestFilePath();

	// Test
	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
    }

    @Test(expectedExceptions = FileOverwriteException.class)
    public void getFile_whenLocalFileAllreadyExist_fileOverwriteException()
	    throws IOException, URISyntaxException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	URI fileSystemPath = hadoopPath.toUri();
	File retrivedFile = UtilsFile.createTestFileWithRandomContent();

	// Test
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
    public void putFile_whenLocalFileDoNotExist_fileNotFoundException()
	    throws IOException {
	File testFile = UtilsFile.createTestFilePath();
	Path hadoopPath = UtilsPath.getSafeDirectory(fileSystem);
	URI fileSystemPath = hadoopPath.toUri();

	// Test
	hadoopFileSystemArchive.putFile(testFile, fileSystemPath);
    }

    @Test(expectedExceptions = FileOverwriteException.class)
    public void putFile_whenRemoteFileExists_fileOverwriteException()
	    throws IOException {
	File fileThatWouldBeOwerwriten = UtilsFile
		.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
	Path hadoopPath = hadoopFileSystemPutter
		.getPathForFile(fileThatWouldBeOwerwriten);
	URI pathToRemoteFile = hadoopPath.toUri();
	File testFile = UtilsFile.createTestFileWithRandomContent();

	// Test
	hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
    }

    public void putFile_whenRemoteFileExists_remoteFileShouldNotBeOverwriten()
	    throws IOException {
	File fileThatWouldBeOwerwriten = UtilsFile
		.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(fileThatWouldBeOwerwriten);
	Path hadoopPath = hadoopFileSystemPutter
		.getPathForFile(fileThatWouldBeOwerwriten);
	URI pathToRemoteFile = hadoopPath.toUri();

	File testFile = UtilsFile.createTestFileWithRandomContent();

	try {
	    // Test
	    hadoopFileSystemArchive.putFile(testFile, pathToRemoteFile);
	} catch (FileOverwriteException e) {
	    // Intentionally ignoring.
	}

	// Confirm
	File fileAfterPut = UtilsFile.createTestFilePath();
	hadoopFileSystemArchive.getFile(fileAfterPut, pathToRemoteFile);
	UtilsTestNG.assertFileContentsEqual(
		"Put shouln't have overwritten the file.",
		fileThatWouldBeOwerwriten, fileAfterPut);

    }

    public void putFile_withDirectoryContainingAnotherDirectory_bothDirectoriesExistsInTheArchive()
	    throws URISyntaxException, FileNotFoundException,
	    FileOverwriteException, IOException {
	File parent = UtilsFile.createTempDirectory();
	String childFileName = "childDir";
	UtilsFile.createDirectoryInParent(parent, childFileName);
	Path parentPathOnHadoop = hadoopFileSystemPutter.getPathForFile(parent);
	hadoopFileSystemArchive.putFile(parent, parentPathOnHadoop.toUri());
	assertTrue(fileSystem.exists(parentPathOnHadoop));
	Path childPath = new Path(parentPathOnHadoop, childFileName);
	assertTrue(fileSystem.exists(childPath));
    }

    public void listPath_listingAPathThatPointsToADirectory_aListThatContainsThePathsInsideSpecifiedDirectory()
	    throws URISyntaxException, IOException {
	File file1 = UtilsFile.createTestFileWithRandomContent();
	File file2 = UtilsFile.createTestFileWithRandomContent();
	File file3 = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(file1);
	hadoopFileSystemPutter.putFile(file2);
	hadoopFileSystemPutter.putFile(file3);
	URI baseURI = hadoopFileSystemPutter.getPathOfMyFiles().toUri();
	URI uri1 = new URI(baseURI + "/" + file1.getName());
	URI uri2 = new URI(baseURI + "/" + file2.getName());
	URI uri3 = new URI(baseURI + "/" + file3.getName());

	// Test
	List<URI> contents = hadoopFileSystemArchive.listPath(baseURI);

	// Confirm
	assertTrue(contents.contains(uri1));
	assertTrue(contents.contains(uri2));
	assertTrue(contents.contains(uri3));
    }

    public void listPath_listingAnEmptyDirectory_emptyList() throws IOException {
	File testDirectory = UtilsFile.createTempDirectory();
	hadoopFileSystemPutter.putFile(testDirectory);
	URI hadoopPathToTheDirectory = hadoopFileSystemPutter.getPathForFile(
		testDirectory).toUri();

	// Test
	List<URI> contents = hadoopFileSystemArchive
		.listPath(hadoopPathToTheDirectory);

	// Confirm
	assertEquals(0, contents.size());
    }

    public void listPath_listingAPathThatPointsToAFile_aListOnlyContainingThePathToTheFile()
	    throws IOException {
	File file = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(file);
	URI uri = hadoopFileSystemPutter.getPathForFile(file).toUri();

	// Test
	List<URI> contents = hadoopFileSystemArchive.listPath(uri);

	// Confirm
	assertTrue(contents.contains(uri));
    }

    public void listPath_listingAPathThatDoNotExist_emptyList()
	    throws IOException, URISyntaxException {
	URI hadoopPathToTheDirectory = new URI(
		"file:///This/path/should/not/exist");

	// Test
	List<URI> contents = hadoopFileSystemArchive
		.listPath(hadoopPathToTheDirectory);

	// Confirm
	assertEquals(0, contents.size());
    }
}
