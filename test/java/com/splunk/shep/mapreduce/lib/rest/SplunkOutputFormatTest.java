package com.splunk.shep.mapreduce.lib.rest;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Job;
import com.splunk.Service;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;

public class SplunkOutputFormatTest {

    private static final String FILENAME_FOR_FILE_WITH_TEST_INPUT = "file01";
    private static final String SOURCE = SplunkOutputFormatTest.class
	    .getSimpleName();

    private HadoopFileSystemPutter putter;

    private File getLocalFileWithTestInput() {
	String pathToFileWithTestInput = "test/java/com/splunk/shep/mapreduce/lib/rest"
		+ "/" + FILENAME_FOR_FILE_WITH_TEST_INPUT;
	return new File(pathToFileWithTestInput);
    }

    @BeforeMethod(groups = { "slow" })
    public void setUp() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.get(fileSystem);
    }

    @AfterMethod(groups = { "slow" })
    public void tearDown() {
	putter.deleteMyFiles();
    }

    private SplunkServiceParameters testParameters;

    @Parameters({ "splunk.username", "splunk.password", "splunk.host",
	    "splunk.mgmtport" })
    @Test(groups = { "slow" })
    public void should_putDataInSplunk_when_runningAMapReduceJob_with_SplunkOutputFormat(
	    String splunkUsername, String splunkPassword, String splunkHost,
	    String splunkMGMTPort) {
	testParameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);
	// Run hadoop
	runHadoopWordCount();

	// Verify in splunk
	verifySplunk();
    }

    /**
     * Hadoop MapReduce job -->
     */
    private void runHadoopWordCount() {
	JobConf mapReduceJob = getConfiguredJob();
	configureJobInputAndOutputPaths(mapReduceJob);
	runJob(mapReduceJob);
    }

    private JobConf getConfiguredJob() {
	JobConf conf = new JobConf();
	conf.setJobName(SOURCE);
	SplunkConfiguration.setConnInfo(conf, testParameters.host,
		testParameters.mgmtPort, testParameters.username,
		testParameters.password);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);
	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);
	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(SplunkOutputFormat.class);

	return conf;
    }

    private void configureJobInputAndOutputPaths(JobConf conf) {
	Path inputFile = getFileOnHadoopWithTestInput();
	Path outputFile = getJobOutputFile();

	FileInputFormat.setInputPaths(conf, inputFile);
	FileOutputFormat.setOutputPath(conf, outputFile);
    }

    private Path getFileOnHadoopWithTestInput() {
	File localFile = getLocalFileWithTestInput();
	putter.putFile(localFile);
	Path remoteFile = putter.getPathForFile(localFile);
	return remoteFile;
    }

    private Path getJobOutputFile() {
	Path remoteDir = putter.getPathWhereMyFilesAreStored();
	Path outputFile = new Path(remoteDir, "output");
	return outputFile;
    }

    private void runJob(JobConf conf) {
	try {
	    JobClient.runJob(conf);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
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
	Job search = startSearch(service);
	waitWhileSearchFinishes(search);
	InputStream results = search.getResults();
	return readResults(results);
    }

    private Job startSearch(Service service) {
	String search = "search index=main source=\"" + SOURCE
		+ "\" sourcetype=\"hadoop_event\" |"
		+ " rex \"(?i)^(?:[^ ]* ){6}(?P<FIELDNAME>.+)\" |"
		+ " table FIELDNAME | tail 5";
	Job job = service.getJobs().create(search);
	System.out.println("Splunk search: " + search);
	return job;
    }

    private void waitWhileSearchFinishes(Job job) {
	while (!job.isDone()) {
	    sleep(30);
	    job.refresh();
	}
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

    private void sleep(int millis) {
	try {
	    Thread.sleep(millis);
	} catch (InterruptedException e) {
	    throw new RuntimeException(e);
	}
    }

    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    String line = value.toString();
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		output.collect(word, one);
	    }
	}
    }

    public static class Reduce extends MapReduceBase implements
	    Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    int sum = 0;
	    while (values.hasNext()) {
		sum += values.next().get();
	    }
	    output.collect(key, new IntWritable(sum));

	}
    }
}
