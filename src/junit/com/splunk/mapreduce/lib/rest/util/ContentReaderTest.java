package com.splunk.mapreduce.lib.rest.util;

import static junit.framework.Assert.assertEquals;

import java.io.StringReader;

import org.junit.Test;


public class ContentReaderTest {

	@Test
	public void getReader_should_containStringReaderContent_when_fullyRead() {
		String expectedContent = "SomeString";
		StringReader reader = new StringReader(expectedContent);
		String actualContent = new ContentReader(reader).getContent();
		assertEquals(expectedContent, actualContent);
	}

}
