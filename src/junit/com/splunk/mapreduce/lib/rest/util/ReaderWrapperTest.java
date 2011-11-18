package com.splunk.mapreduce.lib.rest.util;

import static org.junit.Assert.*;

import java.io.StringReader;

import org.junit.Before;
import org.junit.Test;

import com.splunk.mapreduce.lib.rest.util.ReaderWrapper;

public class ReaderWrapperTest {

	private static final String STRING_READER_CONTENT = "string";
	private static final String PREFIX = "prefix";
	private static final String SUFFIX = "suffix";
	private ReaderWrapper readerWrapper;

	@Before
	public void setUp() {
		readerWrapper = new ReaderWrapper(PREFIX, SUFFIX);
	}

	private StringReader getReader(String content) {
		return new StringReader(content);
	}

	private String getWrappedReaderContent() {
		return new ContentReader(readerWrapper).getContent();
	}

	@Test
	public void should_containPrefixReaderContentAndSuffix_when_fullyRead() {
		String expectedContent = PREFIX + STRING_READER_CONTENT + SUFFIX;
		readerWrapper.wrapReader(getReader(STRING_READER_CONTENT));
		String actualContent = getWrappedReaderContent();
		assertEquals(expectedContent, actualContent);
	}

	@Test
	public void should_removeXMLVersionTagFromTheReaderToBeWrapped_when_thereIsAnXMLVersionTag() {
		String xmlVersionTag = "<?xml version='1.0' encoding='UTF-8'?>";
		String readerContent = xmlVersionTag + STRING_READER_CONTENT;
		String expectedContent = PREFIX + STRING_READER_CONTENT + SUFFIX;

		readerWrapper.wrapReader(getReader(readerContent));
		String actualContent = getWrappedReaderContent();
		assertEquals(expectedContent, actualContent);
	}

}
