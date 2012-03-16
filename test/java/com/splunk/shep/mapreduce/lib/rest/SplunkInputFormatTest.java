package com.splunk.shep.mapreduce.lib.rest;

import static org.testng.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Service;
import com.splunk.shep.mapreduce.lib.rest.tests.SplunkRecord;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;
import com.splunk.shep.testutil.SplunkTestUtils;

public class SplunkInputFormatTest {

    private static final String TEST_INPUT_FILENAME = "wordfile-timestamp";
    private static final String TEST_INPUT_FILE_PATH = MapRedRestTestConstants.TEST_RESOURCES_PATH
	    + "/" + TEST_INPUT_FILENAME;
    private FileSystem fileSystem;
    private HadoopFileSystemPutter putter;
    private SplunkServiceParameters testParameters;

    @BeforeMethod(groups = { "slow" })
    public void setUp() throws IOException {
	fileSystem = FileSystemUtils.getLocalFileSystem();
	putter = HadoopFileSystemPutter.create(fileSystem);
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
	    String splunkPassword) throws InterruptedException, IOException,
	    ClassNotFoundException {
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
	com.splunk.Job search = splunk.getJobs().create(
		"search index=main source=*wordfile-timestamp");
	SplunkTestUtils.waitWhileJobFinishes(search);
	return search.getResultCount() > 0;
    }

    private void runMapReduceJob() throws IOException, InterruptedException,
	    ClassNotFoundException {
	Job job = new Job();
	configureJobConf(job);
	job.waitForCompletion(true);
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

    private void configureJobConf(Job job) throws IOException {
	Configuration conf = job.getConfiguration();
	job.setJobName(SplunkInputFormatTest.class.getSimpleName());
	SplunkConfiguration.setConnInfo(conf, testParameters.host,
		testParameters.mgmtPort, testParameters.username,
		testParameters.password);
	String query = "source::*wordfile-timestamp";
	String indexer1 = "localhost";

	SplunkConfiguration.setSplunkQueryByIndexers(conf, query,
		new String[] { indexer1 });
	conf.set(SplunkConfiguration.SPLUNKEVENTREADER,
		SplunkRecord.class.getName());

	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(IntWritable.class);

	job.setMapperClass(TestMapper.class);
	job.setReducerClass(TestReducer.class);

	job.setInputFormatClass(SplunkInputFormat.class);
	job.setOutputFormatClass(TextOutputFormat.class);

	FileInputFormat.setInputPaths(job, getInput());
	FileOutputFormat.setOutputPath(job, getOutput());
    }

    private void verifyOutput() throws IOException {
	FSDataInputStream open = fileSystem.open(new Path(getOutput(),
		"part-r-00000"));
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

    public static class TestMapper extends
	    Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	@Override
	public void map(LongWritable key, SplunkRecord value, Context context)
		throws IOException, InterruptedException {
	    String line = value.getMap().get("_raw");
	    if (line == null) {
		System.out.println("_raw is null");
		return;
	    }
	    StringTokenizer tokenizer = new StringTokenizer(line);
	    while (tokenizer.hasMoreTokens()) {
		word.set(tokenizer.nextToken());
		context.write(word, one);
	    }
	}
    }

    public static class TestReducer extends
	    Reducer<Text, IntWritable, Text, IntWritable> {
	@Override
	public void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException {
	    int sum = 0;
	    for (IntWritable v : values) {
		sum += v.get();
	    }
	    context.write(key, new IntWritable(sum));
	}
    }
}
