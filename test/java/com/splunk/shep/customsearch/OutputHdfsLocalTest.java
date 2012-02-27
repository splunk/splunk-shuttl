package com.splunk.shep.customsearch;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.Socket;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.Args;
import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shep.testutil.UtilsFileSystem;
import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;

public class OutputHdfsLocalTest {

    HadoopFileSystemPutter putter;
    private FileSystem fileSystem;

    String simpleClassName = this.getClass().getSimpleName();
    String sourcetype = "shep-splunk-hadoop-test";
    String source = simpleClassName;
    String splunkMessage = simpleClassName;
    SplunkServiceParameters parameters;

    @Parameters({ "splunk.host", "splunk.mgmtport", "splunk.username",
	    "splunk.password" })
    @Test(groups = { "slow" })
    public void fileCheck(String splunkHost, String splunkMGMTPort,
	    String splunkUsername, String splunkPassword) throws IOException,
	    InterruptedException {
	parameters = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort);
	Service loggedInService = parameters.getLoggedInService();
	putDataInSplunk(loggedInService);
	Thread.sleep(1000);
	runCustomSearchCommand(splunkUsername, splunkPassword, loggedInService
		.getSettings().getSplunkHome());
    }

    private void runCustomSearchCommand(String username, String password,
	    String splunkhome) {
	try {
	    Path pathForFile = putter.getPathForFile(getOutputFile());
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = {
		    splunkhome + "/bin/splunk",
		    "search",
		    "index=main source=" + source + " sourcetype=" + sourcetype
			    + "| head 1 | outputhdfs file="
			    + pathForFile.toUri(), "-auth",
		    username + ":" + password };
	    System.out.println("executing command: "
		    + Arrays.toString(cmdarray));
	    Process proc = rt.exec(cmdarray);
	    int exitCode = proc.waitFor();
	    assertEquals(exitCode, 0);
	    FSDataInputStream is = fileSystem.open(pathForFile);
	    if (is == null) {
		Assert.fail("File not created by outputhdfs in HDFS");
	    }
	    List<String> readLines = IOUtils.readLines(is);
	    assertTrue(!readLines.isEmpty());
	} catch (Throwable t) {
	    t.printStackTrace();
	    Assert.fail(t.getMessage());
	}
    }

    private void putDataInSplunk(Service service) throws IOException {

	Index index = service.getIndexes().get("main");
	Args attachArgs = new Args();
	attachArgs.put("source", source);
	attachArgs.put("sourcetype", sourcetype);
	Socket stream = index.attach(attachArgs);
	OutputStream ostream = stream.getOutputStream();
	Writer writerOut = new OutputStreamWriter(ostream, "UTF8");
	writerOut.append(splunkMessage);
	writerOut.flush();
	writerOut.close();
    }

    @BeforeMethod(groups = { "slow" })
    public void setUp() throws IOException {
	fileSystem = UtilsFileSystem.getLocalFileSystem();
	putter = HadoopFileSystemPutter.create(fileSystem);
	Path pathForFile = putter.getPathForFile(getOutputFile());
	fileSystem.mkdirs(pathForFile.getParent());
    }

    private File getOutputFile() {
	return new File("outputFile");
    }

    @AfterMethod(groups = { "slow" })
    public void tearDown() {
	putter.deleteMyFiles();
    }
}
