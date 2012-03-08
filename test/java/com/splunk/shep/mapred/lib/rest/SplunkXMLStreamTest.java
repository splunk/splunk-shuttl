package com.splunk.shep.mapred.lib.rest;

import static org.testng.Assert.*;

import java.io.File;
import java.io.FileInputStream;
import java.net.URL;
import java.util.HashMap;

import org.testng.annotations.Test;

public class SplunkXMLStreamTest {

    @Test(groups = { "fast" })
    public void should_addSplunkResultTagsToBeginingAndEnd_when_readFully()
	    throws Exception {
	File testXML = getTestXMLFile();
	SplunkXMLStream splunkXMLStream = new SplunkXMLStream(
		new FileInputStream(testXML));

	boolean gotResults = false;
	HashMap<String, String> result;
	while ((result = splunkXMLStream.nextResult()) != null) {
	    assertEquals(result.get("_sourcetype"), "wordfile-timestamp");
	    gotResults = true;
	}
	assertTrue(gotResults);
    }

    private File getTestXMLFile() {
	URL resource = getClass().getResource("SplunkSearchResults.xml");
	if (resource == null)
	    throw new RuntimeException("could not find SplunkSearchResults.xml");
	return new File(resource.getPath());
    }

}
