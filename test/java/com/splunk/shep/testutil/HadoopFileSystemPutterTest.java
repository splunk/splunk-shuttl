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

    private Path hadoopDirectoryThatIsRemovedAfterTests = new Path(
	    "/ShepTests/"
		    + HadoopFileSystemPutterTest.class.getSimpleName());

    private void deleteHadoopTestDirectory() {
	try {
	    fileSystem.delete(hadoopDirectoryThatIsRemovedAfterTests, true);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

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
	deleteHadoopTestDirectory();
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
    public void should_beAbleToGetThePath_where_TheFileIsStored() {
	File tempFile = getTempFileThatIsAutomaticallyDeleted();
	copier.putFile(tempFile);
	assertTrue(copier.isFileCopiedToFileSystem(tempFile));
	Path path = copier.getPathWhereFileIsStored(tempFile);
	assertNotNull(path);
    }

    @Test(groups = { "fast" })
    public void pathWhereFileIsStored_for_twoDifferentFiles_should_differ() {
	File file1 = getTempFileThatIsAutomaticallyDeleted();
	File file2 = getTempFileThatIsAutomaticallyDeleted();
	assertNotEquals(file1.getName(), file2.getName());

	copier.putFile(file1);
	copier.putFile(file2);

	Path path1 = copier.getPathWhereFileIsStored(file1);
	Path path2 = copier.getPathWhereFileIsStored(file2);
	assertNotEquals(path1, path2);
    }
}
