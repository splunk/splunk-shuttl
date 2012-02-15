package com.splunk.shep.testutil;

import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.LocalFileToHadoopCopier.LocalFileNotFound;

public class LocalFileToHadoopCopierTest {

    private LocalFileToHadoopCopier copier;
    private FileSystem fileSystem;

    private Path hadoopDirectoryThatIsRemovedAfterTests = new Path(
	    "/ShepTests/" + LocalFileToHadoopCopierTest.class.getSimpleName());

    private void deleteHadoopTestDirectory() {
	try {
	    fileSystem.delete(hadoopDirectoryThatIsRemovedAfterTests, true);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private FileSystem getLocalFileSystem() {
	Configuration configuration = new Configuration();
	try {
	    return FileSystem.getLocal(configuration);
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

    private void copyFileToHadoop(File src, Path dest) {
	copier.copyFileToHadoop(src, dest);
    }

    private Path getPathOnHadoopForFile(File src) {
	return new Path(hadoopDirectoryThatIsRemovedAfterTests.getName() + "/"
		+ src.getName());
    }

    @BeforeTest(groups = { "fast" })
    public void setUp() {
	fileSystem = getLocalFileSystem();
	copier = new LocalFileToHadoopCopier(fileSystem);
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
	Path destination = getPathOnHadoopForFile(tempFile);
	copyFileToHadoop(tempFile, destination);
	assertTrue(fileSystem.exists(destination));
    }

    @Test(groups = { "fast" }, expectedExceptions = LocalFileNotFound.class)
    public void copyingFileThatDoesntExist_should_throw_LocalFileNotFound() {
	File nonExistingFile = new File("file-does-not-exist");
	copyFileToHadoop(nonExistingFile,
		getPathOnHadoopForFile(nonExistingFile));
    }

    // TODO, WHY AM I THROWING AN IOException?
    @Test(groups = { "fast" }, expectedExceptions = InvalidHadoopPath.class)
    public void copyTempFileToAnInvalidHadoopPath_should_throw_InvalidHadoopPath() {
	File tempFile = getTempFileThatIsAutomaticallyDeleted();
	Path invalidPath = new Path("fisk");
    }

    public class InvalidHadoopPath extends RuntimeException {

    }

}
