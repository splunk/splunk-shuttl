package com.splunk.shep.archiver.fileSystem;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.UtilsFile;
import com.splunk.shep.testutil.UtilsTestNG;

@Test(groups = { "fast" })
public class HadoopFileSystemArchiveTest {

    private HadoopFileSystemArchive hadoopFileSystemArchive;
    private HadoopFileSystemPutter hadoopFileSystemPutter;

    @BeforeMethod
    public void beforeMethod() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	hadoopFileSystemArchive = new HadoopFileSystemArchive(fileSystem);
	hadoopFileSystemPutter = HadoopFileSystemPutter.create(fileSystem);
    }

    @AfterMethod
    public void afterMethod() {
	hadoopFileSystemPutter.deleteMyFiles();
    }

    public void HadoopFileSystemArchive() {
	assertNotNull(hadoopFileSystemArchive);
    }

    @Test(enabled = false)
    public void getFile() throws IOException {
	File testFile = UtilsFile.createTestFileWithRandomContent();
	hadoopFileSystemPutter.putFile(testFile);
	Path hadoopPath = hadoopFileSystemPutter.getPathForFile(testFile);
	FileSystemPath fileSystemPath = HadoopFileSystemArchive
		.convertHadoopPathToFilesystemPath(hadoopPath);
	File retrivedFile = UtilsFile.createTestFile();

	hadoopFileSystemArchive.getFile(retrivedFile, fileSystemPath);
	UtilsTestNG.assertFileContentsEqual(testFile, retrivedFile);
    }

    @Test(enabled = false)
    public void listPath() {
	throw new RuntimeException("Test not implemented");
    }

    @Test(enabled = false)
    public void putFile() {
	throw new RuntimeException("Test not implemented");
    }

    public void convertHadoopPathToFilesystemPath() {
	Path hadoopPath = new Path("file:/emre/was/here");
	FileSystemPath fileSystemPath = HadoopFileSystemArchive
		.convertHadoopPathToFilesystemPath(hadoopPath);

	assertEquals("/emre/was/here", fileSystemPath.toString());
    }
}
