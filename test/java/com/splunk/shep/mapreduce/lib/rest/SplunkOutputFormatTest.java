package com.splunk.shep.mapreduce.lib.rest;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.mapreduce.lib.rest.tests.WordCount;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;
import com.splunk.shep.testutil.SplunkTestUtils;

public class SplunkOutputFormatTest {

    private static final String FILENAME_FOR_FILE_WITH_TEST_INPUT = "file01";
    private static final String SOURCE = SplunkOutputFormatTest.class
	    .getSimpleName();

    private HadoopFileSystemPutter putter;

    private File getLocalFileWithTestInput() {
	return SplunkEventsInputFormatTest
		.getFileForFileName(FILENAME_FOR_FILE_WITH_TEST_INPUT);
    }

    @BeforeMethod(groups = { "integration" })
    public void setUp() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.create(fileSystem);
    }

    @AfterMethod(groups = { "integration" })
    public void tearDown() {
	putter.deleteMyFiles();
    }

    private SplunkServiceParameters testParameters;

    @Parameters({ "splunk.username", "splunk.password", "splunk.host",
	    "splunk.mgmtport" })
    @Test(groups = { "integration" })
    public void should_putDataInSplunk_when_runningAMapReduceJob_with_SplunkOutputFormat(
	    String splunkUsername, String splunkPassword, String splunkHost,
	    String splunkMGMTPort) throws IOException, InterruptedException,
	    ClassNotFoundException {
	testParameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);
	// Run hadoop
	runHadoopWordCount();

	// Verify in splunk
	verifySplunk();
    }

    /**
     * Hadoop MapReduce job -->
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    private void runHadoopWordCount() throws IOException, InterruptedException,
	    ClassNotFoundException {
	Job mapReduceJob = getConfiguredJob();
	configureJobInputAndOutputPaths(mapReduceJob);
	assertTrue(mapReduceJob.waitForCompletion(true));
    }

    private Job getConfiguredJob() throws IOException {
	Job job = new Job();
	job.setJobName(SOURCE);
	SplunkConfiguration.setConnInfo(job.getConfiguration(),
		testParameters.host, testParameters.mgmtPort,
		testParameters.username, testParameters.password);

	job.setJarByClass(WordCount.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);
	job.setMapperClass(WordCount.TokenizerMapper.class);
	job.setReducerClass(WordCount.IntSumReducer.class);
	job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(SplunkOutputFormat.class);

	return job;
    }

    private void configureJobInputAndOutputPaths(Job job) throws IOException {
	Path inputFile = getFileOnHadoopWithTestInput();
	Path outputFile = getJobOutputFile();

	FileInputFormat.setInputPaths(job, inputFile);
	FileOutputFormat.setOutputPath(job, outputFile);
    }

    private Path getFileOnHadoopWithTestInput() {
	File localFile = getLocalFileWithTestInput();
	putter.putFile(localFile);
	Path remoteFile = putter.getPathForFile(localFile);
	return remoteFile;
    }

    private Path getJobOutputFile() {
	Path remoteDir = putter.getPathOfMyFiles();
	Path outputFile = new Path(remoteDir, "output");
	return outputFile;
    }

    /**
     * Splunk verification -->
     */
    private void verifySplunk() {
	List<String> searchResults = getSearchResultsFromSplunk();
	assertTrue(!searchResults.isEmpty());
	verifyWordCountInSearchResults(searchResults);
    }

    private List<String> getSearchResultsFromSplunk() {
	Service service = testParameters.getLoggedInService();
	com.splunk.Job search = startSearch(service);
	SplunkTestUtils.waitWhileJobFinishes(search);
	InputStream results = search.getResults();
	return readResults(results);
    }

    private com.splunk.Job startSearch(Service service) {
	String search = "search index=main source=\"" + SOURCE
		+ "\" sourcetype=\"hadoop_event\" |"
		+ " rex \"(?i)^(?:[^ ]* ){6}(?P<FIELDNAME>.+)\" |"
		+ " table FIELDNAME | tail 5";
	com.splunk.Job job = service.getJobs().create(search);
	System.out.println("Splunk search: " + search);
	return job;
    }

    private List<String> readResults(InputStream results) {
	try {
	    return IOUtils.readLines(results);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private void verifyWordCountInSearchResults(List<String> searchResults) {
	StringBuffer mergedLines = new StringBuffer();
	for (String result : searchResults)
	    mergedLines.append(result);
	Set<String> expectedWordCount = getExpectedWordCount();
	for (String wordCount : expectedWordCount)
	    assertTrue(mergedLines.toString().contains(wordCount));
    }

    private Set<String> getExpectedWordCount() {
	Set<String> expectedWordCountResults = new HashSet<String>();
	expectedWordCountResults.add("Bye 1");
	expectedWordCountResults.add("Goodbye 1");
	expectedWordCountResults.add("Hadoop 2");
	expectedWordCountResults.add("Hello 2");
	expectedWordCountResults.add("World 2");
	return expectedWordCountResults;
    }

}
