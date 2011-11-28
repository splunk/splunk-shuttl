package com.splunk.mapreduce.lib.rest;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;

import org.testng.annotations.Test;
import static org.testng.Assert.*;

public class SplunkXMLStreamTest {

	@Test
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
		String splunkSearchResultsXMLPath = "src/junit/com/splunk/mapreduce/lib/rest/SplunkSearchResults.xml";
		String osSafePathToXML = splunkSearchResultsXMLPath.replaceAll("/", ""
				+ File.separatorChar);
		return new File(osSafePathToXML);
	}

}
