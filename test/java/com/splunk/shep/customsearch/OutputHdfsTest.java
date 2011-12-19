package com.splunk.shep.customsearch;

import org.apache.hadoop.fs.FSDataInputStream;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

public class OutputHdfsTest extends SplunkHdfsTest {

    String testuri = null;

    @Parameters({ "username", "password", "splunk.home" })
    @Test(groups = { "slow" })
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

    @Parameters({ "outputhdfstesturi" })
    @BeforeTest(groups = { "slow" })
    public void beforeTest(String uri) {
	System.out.println("uri: " + uri);
	this.testuri = uri;
	try {
	    mkdirsinHDFS(this.testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

    @AfterTest(groups = { "slow" })
    public void afterTest() {
	try {
	    deleteFileinHDFS(this.testuri);
	} catch (Exception e) {
	    Assert.fail(e.getMessage());
	}
    }

}
