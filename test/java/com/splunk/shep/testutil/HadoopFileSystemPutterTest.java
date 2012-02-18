package com.splunk.shep.testutil;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.HadoopFileSystemPutter.LocalFileNotFound;

public class HadoopFileSystemPutterTest {

    private HadoopFileSystemPutter copier;
    private FileSystem fileSystem;

    private File getTempFileThatIsAutomaticallyDeleted() {
	File tempFile = getTempFile();
	tempFile.deleteOnExit();
	return tempFile;
    }

    private File getTempFile() {
	try {
	    return File.createTempFile("fisk", ".tmp");
	} catch (IOException e) {
	    throw new RuntimeException("Unable to create temp file.", e);
	}
    }

    @BeforeTest(groups = { "fast" })
    public void setUp() {
	fileSystem = FileSystemUtils.getLocalFileSystem();
	copier = HadoopFileSystemPutter.get(fileSystem);
    }

    @AfterTest(groups = { "fast" })
    public void tearDown() {
	copier.deleteMyFiles();
    }

    @Test(groups = { "fast" })
    public void createdTempFile_should_exist() {
	File tempFile = getTempFileThatIsAutomaticallyDeleted();
	assertTrue(tempFile.exists());
    }

    @Test(groups = { "fast" })
    public void copyingFileThatExists_should_existInFileSystemCopiedTo()
	    throws IOException {
	File tempFile = getTempFileThatIsAutomaticallyDeleted();
	copier.putFile(tempFile);
	assertTrue(copier.isFileCopiedToFileSystem(tempFile));
    }

    @Test(groups = { "fast" }, expectedExceptions = LocalFileNotFound.class)
    public void copyingFileThatDoesntExist_should_throw_LocalFileNotFound() {
	File nonExistingFile = new File("file-does-not-exist");
	copier.putFile(nonExistingFile);
    }

    @Test(groups = { "fast" })
    public void fileThatIsNotCopied_shouldNot_existInFileSystem() {
	assertFalse(copier.isFileCopiedToFileSystem(new File("somefile")));
    }

    @Test(groups = { "fast" })
    public void should_bePossibleToGetTheDirectory_where_allThisTestCasesFilesAreStored() {
	assertNotNull(copier.getPathWhereMyFilesAreStored());
    }

    @Test(groups = { "fast" })
    public void pathWhereAClassesFilesAreStored_should_differForDifferentClasses() {
	ClassA classA = new ClassA();
	ClassB classB = new ClassB();
	boolean isDifferentClassses = !classA.getClass().getName()
		.equals(classB.getClass().getName());
	assertTrue(isDifferentClassses);

	Path classAStoragePath = classA.getPathWhereFilesAreStored();
	Path classBStoragePath = classB.getPathWhereFilesAreStored();
	assertNotEquals(classAStoragePath, classBStoragePath);
    }

    private class ClassA {

	public Path getPathWhereFilesAreStored() {
	    return copier.getPathWhereMyFilesAreStored();
	}
    }

    private class ClassB {
	public Path getPathWhereFilesAreStored() {
	    return copier.getPathWhereMyFilesAreStored();
	}
    }

    @Test(groups = { "fast" })
    public void after_putFile_then_deleteMyFiles_should_removeTheDirectory_where_thisClassPutFilesOnTheFileSystem()
	    throws IOException {
	Path myFiles = copier.getPathWhereMyFilesAreStored();
	copier.putFile(getTempFileThatIsAutomaticallyDeleted());
	assertTrue(fileSystem.exists(myFiles));
	copier.deleteMyFiles();
	assertFalse(fileSystem.exists(myFiles));
    }

    @Test(groups = { "fast" })
    public void should_beAbleToGetPath_where_fileIsPut() {
	assertNotNull(copier
		.getPathForFile(getTempFileThatIsAutomaticallyDeleted()));
    }

    @Test(groups = { "fast" })
    public void path_where_localFileIsPut_should_differForDifferentFiles() {
	File file1 = getTempFileThatIsAutomaticallyDeleted();
	File file2 = getTempFileThatIsAutomaticallyDeleted();
	assertNotEquals(file1, file2);

	Path path1 = copier.getPathForFile(file1);
	Path path2 = copier.getPathForFile(file2);
	assertNotEquals(path1, path2);
    }
}
