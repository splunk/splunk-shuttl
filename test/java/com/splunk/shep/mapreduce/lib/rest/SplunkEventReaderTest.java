package com.splunk.shep.mapreduce.lib.rest;

import java.io.File;

import org.apache.hadoop.fs.FileSystem;
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
	putFilesOnHadoop();

	runMapReduceJob();
    }

    private void putFilesOnHadoop() {
	File file1 = getFirstFile();
	File file2 = getSecondFile();
	putter.putFile(file1);
	putter.putFile(file2);
    }

    private File getFirstFile() {
	return getFileForFileName(TEST_INPUT_FILENAME_1);
    }

    private File getSecondFile() {
	return getFileForFileName(TEST_INPUT_FILENAME_2);
    }

    private File getFileForFileName(String fileName) {
	return new File(MapReduceRestTestConstants.TEST_RESOURCES_PATH + "/"
		+ fileName);
    }

    private void runMapReduceJob() {

    }

}
