package com.splunk.shep.mapreduce.lib.rest.util;

import static org.testng.Assert.*;

import java.io.StringReader;

import org.testng.annotations.Test;

public class ContentReaderTest {

    @Test(groups = { "fast-unit" })
    public void getReader_should_containStringReaderContent_when_fullyRead() {
	String expectedContent = "SomeString";
	StringReader reader = new StringReader(expectedContent);
	String actualContent = new ContentReader(reader).getContent();
	assertEquals(expectedContent, actualContent);
    }

}
