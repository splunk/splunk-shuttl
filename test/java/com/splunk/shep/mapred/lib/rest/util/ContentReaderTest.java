package com.splunk.shep.mapred.lib.rest.util;

import static org.testng.Assert.assertEquals;

import java.io.StringReader;

import org.testng.annotations.Test;

import com.splunk.shep.mapred.lib.rest.util.ContentReader;

public class ContentReaderTest {

    @Test(groups = { "fast" })
    public void getReader_should_containStringReaderContent_when_fullyRead() {
	String expectedContent = "SomeString";
	StringReader reader = new StringReader(expectedContent);
	String actualContent = new ContentReader(reader).getContent();
	assertEquals(expectedContent, actualContent);
    }

}
