package com.splunk.shep.mapreduce.lib.rest;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;

public class SplunkEventReaderTest {

    private static final String TEST_INPUT_FILENAME_1 = "sdata1";
    private static final String TEST_INPUT_FILENAME_2 = "sdata2";
    private HadoopFileSystemPutter putter;

    @BeforeMethod(groups = { "slow" })
    public void setUp() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.get(fileSystem);
    }

    @AfterMethod(groups = { "slow" })
    public void tearDown() {
	putter.deleteMyFiles();
    }

    @Test(groups = { "slow" })
    public void theTest() {
	Path file1 = getPathToFirstFile();
	Path file2 = getPathToSecondFile();
    }

    private Path getPathToFirstFile() {
	return getPathToFile(TEST_INPUT_FILENAME_1);
    }

    private Path getPathToSecondFile() {
	return getPathToFile(TEST_INPUT_FILENAME_2);
    }

    private Path getPathToFile(String fileName) {
	return putter.getPathForFileName(fileName);
    }
}
