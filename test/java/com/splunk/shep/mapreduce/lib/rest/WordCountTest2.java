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

public class WordCountTest2 {

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
	    "splunk.password", "splunk.home" })
    public void should_runAMapReduceJob_by_usingSplunkAsAnInputToHadoop(
	    String splunkHost, String splunkMGMTPort, String splunkUsername,
	    String splunkPassword, String splunkHome)
	    throws InterruptedException, IOException {
	testParameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);

	addDataToSplunk(splunkHome);

	runMapReduceJob();

	verifyOutput();
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

	assertEquals(expected, actual);
    }

    public void addDataToSplunk(String splunkHome) throws InterruptedException,
	    IOException {
	Service splunk = testParameters.getLoggedInService();
	if (!isTestFileAlreadyIndexed(splunk))
	    indexTestFile(splunkHome);
    }

    private void indexTestFile(String splunkHome) throws IOException,
	    InterruptedException {
	// There's currently no way to oneshot a file through the Splunk SDK/API
	// yet. Using splunk.home instead.
	File file = new File("test/java/com/splunk/shep/mapreduce/lib/rest"
		+ "/wordfile-timestamp");
	Process exec = Runtime.getRuntime().exec(
		splunkHome + "/bin/splunk add oneshot "
			+ file.getAbsolutePath());
	int exitStatus = exec.waitFor();
	assertEquals(exitStatus, 0);
    }

    private boolean isTestFileAlreadyIndexed(Service splunk)
	    throws InterruptedException {
	Job search = splunk.getJobs().create(
		"search index=main source=*wordfile-timestamp");
	waitWhileSearchFinishes(search);
	return search.getResultCount() > 0;
    }

    private void waitWhileSearchFinishes(Job search)
	    throws InterruptedException {
	while (!search.isDone()) {
	    Thread.sleep(20);
	    search.refresh();
	}
    }

    private void runMapReduceJob() throws IOException, InterruptedException {
	JobConf job = new JobConf(); // cluster.createJobConf();
	configureJobConf(job);

	System.out.println("indexbyhost "
		+ job.getInt(SplunkConfiguration.INDEXBYHOST, 0));

	JobClient.runJob(job);

    }

    private Path getOutput() {
	return new Path(getPathWhereMyFilesAreStored(), "output");
    }

    private Path getInput() {
	return new Path(getPathWhereMyFilesAreStored(), "input");
    }

    private Path getPathWhereMyFilesAreStored() {
	Path pathWhereMyFilesAreStored = HadoopFileSystemPutter.get(
		FileSystemUtils.getLocalFileSystem())
		.getPathWhereMyFilesAreStored();
	return pathWhereMyFilesAreStored;
    }

    private void configureJobConf(JobConf job) {
	job.setJobName(WordCountTest2.class.getSimpleName());
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

    public static class Map extends MapReduceBase implements
	    Mapper<LongWritable, SplunkRecord, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, SplunkRecord value,
		OutputCollector<Text, IntWritable> output, Reporter reporter)
		throws IOException {
	    System.out.println("got a map");
	    String line = value.getMap().get("_raw");
	    if (line == null) {
		System.out.println("_raw is null");
		return;
	    }
	    System.out.println("line " + line);
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
