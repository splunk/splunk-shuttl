package com.splunk.shep.customsearch;

import static org.testng.AssertJUnit.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.HadoopFileSystemPutter;
import com.splunk.shep.testutil.SplunkServiceParameters;
import com.splunk.shep.testutil.UtilsFileSystem;

public class InputHdfsLocalTest {

    private static final String FILENAME = "testfile";
    String line1 = "this is line1";
    String line2 = "this is line2";
    HadoopFileSystemPutter putter;
    private FileSystem fileSystem;

    @Parameters({ "splunk.host", "splunk.mgmtport", "splunk.username",
	    "splunk.password" })
    @Test(groups = { "integration" })
    public void fileCheck(String splunkHost, String splunkMGMTPort,
	    String splunkUsername, String splunkPassword) {
	String splunkHome = getSplunkHome(splunkHost, splunkMGMTPort,
		splunkUsername, splunkPassword);
	System.out.println("Running InputHdfs Test");
	Path pathToTestFile = putter.getPathForFileName(FILENAME);
	try {
	    System.out.println("pathToTestFile uri: " + pathToTestFile.toUri());
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = { splunkHome + "/bin/splunk", "search",
		    "| inputhdfs file=" + pathToTestFile.toUri(), "-auth",
		    splunkUsername + ":" + splunkPassword };
	    Process proc = rt.exec(cmdarray);
	    proc.waitFor();
	    InputStream stdin = proc.getInputStream();
	    InputStreamReader isr = new InputStreamReader(stdin);
	    BufferedReader br = new BufferedReader(isr);
	    String readLine1 = br.readLine();
	    assertTrue(readLine1.endsWith(line1));
	    String readLine2 = br.readLine();
	    assertTrue(readLine2.endsWith(line2));
	} catch (Throwable t) {
	    t.printStackTrace();
	    Assert.fail(t.getMessage());
	}
    }

    private String getSplunkHome(String splunkHost, String splunkMGMTPort,
	    String splunkUsername, String splunkPassword) {
	String splunkHome = new SplunkServiceParameters(splunkUsername,
		splunkPassword, splunkHost, splunkMGMTPort)
		.getLoggedInService().getSettings().getSplunkHome();
	return splunkHome;
    }

    @BeforeMethod(groups = { "integration" })
    public void beforeMethod() throws IOException {
	fileSystem = UtilsFileSystem.getLocalFileSystem();
	putter = HadoopFileSystemPutter.create(fileSystem);
	putFileWithTestInputOnHadoop();
    }

    private void putFileWithTestInputOnHadoop() throws IOException {
	File file = getTempFileThatDeletesOnExit();
	writeContentsToFile(file);
	putter.putFile(file);
    }

    private File getTempFileThatDeletesOnExit() throws IOException {
	File file = new File(FILENAME);
	file.deleteOnExit();
	return file;
    }

    private void writeContentsToFile(File file) throws IOException {
	FileWriter writer = new FileWriter(file);
	writer.append(line1);
	writer.append("\n");
	writer.append(line2);
	writer.append("\n");
	writer.flush();
	writer.close();
    }

    @AfterMethod(groups = { "integration" })
    public void afterTest() {
	putter.deleteMyFiles();
    }

}
