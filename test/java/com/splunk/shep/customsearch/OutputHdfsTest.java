package com.splunk.shep.customsearch;

import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class OutputHdfsTest extends SplunkHdfsTest {

    String testuri = null;

    @Parameters({ "splunk.username", "splunk.password", "splunk.home" })
    @Test(groups = { "functional" })
    public void fileCheck(String username, String password, String splunkhome) {
	System.out.println("Running OutputHdfs Test");
	System.out.println("splunkhome " + splunkhome);
	try {
	    Runtime rt = Runtime.getRuntime();
	    String cmdarray[] = { splunkhome + "/bin/splunk", "search",
		    "* | head 1 | outputhdfs file=" + testuri, "-auth",
		    username + ":" + password };
	    Process proc = rt.exec(cmdarray);
	    proc.waitFor();
	    FSDataInputStream is = getFileinHDFS(testuri);
	    if (is == null) {
		Assert.fail("File not created by outputhdfs in HDFS");
	    }

	} catch (Throwable t) {
	    t.printStackTrace();
	    Assert.fail(t.getMessage());
	}
    }

    @Parameters({ "hadoop.host", "hadoop.port" })
    @BeforeMethod(groups = { "functional" })
    public void beforeTest(String hadoophost, String hadoopport) {
	this.testuri = "hdfs://" + hadoophost + ":" + hadoopport
		+ "/outputhdfstest/testfile";
	try {
	    mkdirsinHDFS(this.testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

    @AfterMethod(groups = { "functional" })
    public void afterTest() {
	try {
	    deleteFileinHDFS(this.testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

}
