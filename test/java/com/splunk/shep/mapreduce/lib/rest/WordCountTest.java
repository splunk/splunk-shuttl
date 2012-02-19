package com.splunk.shep.mapreduce.lib.rest;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

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

import com.splunk.shep.mapreduce.lib.rest.tests.WordCount;
import com.splunk.shep.testutil.FileSystemUtils;
import com.splunk.shep.testutil.HadoopFileSystemPutter;

public class WordCountTest {

    private static final String FILENAME_FOR_FILE_WITH_TEST_INPUT = "file01";

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

    @Parameters({ "splunk.username", "splunk.password" })
    @Test(groups = { "slow" })
    public void fileCheck(String splunkUsername, String splunkPassword)
	    throws IOException {
	JobConf conf = new JobConf(WordCount.class);
	conf.setJobName("hadoopunittest1");
	SplunkConfiguration.setConnInfo(conf, "localhost", 8089,
		splunkUsername, splunkPassword);

	conf.setOutputKeyClass(Text.class);
	conf.setOutputValueClass(IntWritable.class);

	conf.setMapperClass(Map.class);
	conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(SplunkOutputFormat.class);

	File localFile = getLocalFileWithTestInput();
	putter.putFile(localFile);
	Path remoteFile = putter.getPathForFile(localFile);
	Path remoteDir = putter.getPathWhereMyFilesAreStored();
	Path outputFile = new Path(remoteDir, "output");

	FileInputFormat.setInputPaths(conf, remoteFile);
	FileOutputFormat.setOutputPath(conf, outputFile);

	JobClient.runJob(conf);
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
