package com.splunk.shep.mapred.lib.rest;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
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
	String splunkSearchResultsXMLPath = "test/java/com/splunk/shep/mapred/lib/rest/SplunkSearchResults.xml";
	String osSafePathToXML = splunkSearchResultsXMLPath.replaceAll("/", ""
		+ File.separatorChar);
	return new File(osSafePathToXML);
    }

}
