package com.splunk.shep.mapreduce.lib.rest;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapred.TextOutputFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Job;
import com.splunk.Service;
import com.splunk.shep.mapreduce.lib.rest.tests.SplunkRecord;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;
import com.splunk.shep.testutil.SplunkTestUtils;

public class SplunkInputFormatTest {

    private static final String TEST_INPUT_FILENAME = "wordfile-timestamp";
    private static final String TEST_INPUT_FILE_PATH = MapReduceRestTestConstants.TEST_RESOURCES_PATH
	    + "/" + TEST_INPUT_FILENAME;
    private FileSystem fileSystem;
    private HadoopFileSystemPutter putter;
    private SplunkServiceParameters testParameters;

    @BeforeMethod(groups = { "slow" })
    public void setUp() throws IOException {
	fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.get(fileSystem);
    }

    @AfterMethod(groups = { "slow" })
    public void tearDown() {
	putter.deleteMyFiles();
    }

    @Test(groups = { "slow" })
    @Parameters({ "splunk.host", "splunk.mgmtport", "splunk.username",
	    "splunk.password" })
    public void should_runAMapReduceJob_by_usingSplunkAsAnInputToHadoop(
	    String splunkHost, String splunkMGMTPort, String splunkUsername,
	    String splunkPassword) throws InterruptedException, IOException {
	testParameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);
	Service loggedInService = testParameters.getLoggedInService();
	String splunkHome = loggedInService.getSettings().getSplunkHome();

	addDataToSplunk(splunkHome);

	// Wait until data has been processed in Splunk.
	Thread.sleep(1000);

	runMapReduceJob();

	verifyOutput();
    }

    public void addDataToSplunk(String splunkHome) {
	Service splunk = testParameters.getLoggedInService();
	if (!isTestFileAlreadyIndexed(splunk))
	    indexTestFile(splunkHome);
    }

    // There's currently no way to oneshot a file through the Splunk SDK/API
    // Currently using $SPLUNK_HOME instead.
    private void indexTestFile(String splunkHome) {
	File file = new File(TEST_INPUT_FILE_PATH);
	Process exec = oneshotFileToSplunk(splunkHome, file);
	int exitStatus = waitForOneshotToComplete(exec);
	if (exitStatus > 0)
	    printOneshotOutput(exec);
	assertEquals(exitStatus, 0);
    }

    private void printOneshotOutput(Process exec) {
	try {
	    System.err.println("Exec err: "
		    + IOUtils.readLines(exec.getErrorStream()));
	    System.err.println("Exec out: "
		    + IOUtils.readLines(exec.getInputStream()));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private Process oneshotFileToSplunk(String splunkHome, File file) {
	try {
	    return doOneshotFileToSplunk(splunkHome, file);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private Process doOneshotFileToSplunk(String splunkHome, File file)
	    throws IOException {
	String command = splunkHome + "/bin/splunk add oneshot "
		+ file.getAbsolutePath() + " -auth "
		+ testParameters.getUsername() + ":"
		+ testParameters.getPassword();
	return Runtime.getRuntime().exec(command);
    }

    private int waitForOneshotToComplete(Process exec) {
	try {
	    return exec.waitFor();
	} catch (InterruptedException e) {
	    throw new RuntimeException(e);
	}
    }

    private boolean isTestFileAlreadyIndexed(Service splunk) {
	Job search = splunk.getJobs().create(
		"search index=main source=*wordfile-timestamp");
	SplunkTestUtils.waitWhileJobFinishes(search);
	return search.getResultCount() > 0;
    }

    private void runMapReduceJob() throws IOException {
	JobConf job = new JobConf(); // cluster.createJobConf();
	configureJobConf(job);
	JobClient.runJob(job);
    }

    private Path getOutput() {
	return new Path(getPathWhereMyFilesAreStored(), "output");
    }

    private Path getInput() {
	return new Path(getPathWhereMyFilesAreStored(), "input");
    }

    private Path getPathWhereMyFilesAreStored() {
	return putter.getPathOfMyFiles();
    }

    private void configureJobConf(JobConf job) {
	job.setJobName(SplunkInputFormatTest.class.getSimpleName());
	SplunkConfiguration.setConnInfo(job, testParameters.host,
		testParameters.mgmtPort, testParameters.username,
		testParameters.password);
	String query = "source::*wordfile-timestamp";
	String indexer1 = "localhost";

	SplunkConfiguration.setSplunkQueryByIndexers(job, query,
		new String[] { indexer1 });
	job.set(SplunkConfiguration.SPLUNKEVENTREADER,
		SplunkRecord.class.getName());

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(Map.class);
	job.setCombinerClass(Reduce.class);
	job.setReducerClass(Reduce.class);

	job.setInputFormat(com.splunk.shep.mapreduce.lib.rest.SplunkInputFormat.class);
	job.setOutputFormat(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, getInput());
	FileOutputFormat.setOutputPath(job, getOutput());
    }

    private void verifyOutput() throws IOException {
	FSDataInputStream open = fileSystem.open(new Path(getOutput(),
		"part-00000"));
	Set<String> expected = new HashSet<String>();
	expected.add("17:04:15	1");
	expected.add("17:04:14	1");
	expected.add("17:04:13	1");
	expected.add("17:04:12	1");
	expected.add("17:04:11	1");
	expected.add("2011-09-19	5");
	expected.add("a	5");
	expected.add("is	5");
	expected.add("test	5");
	expected.add("this	5");

	List<String> readLines = IOUtils.readLines(open);
	Set<String> actual = new HashSet<String>(readLines);

	assertEquals(actual, expected);
    }

    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, SplunkRecord value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    String line = value.getMap().get("_raw");
	    if (line == null) {
		System.out.println("_raw is null");
		return;
	    }
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
