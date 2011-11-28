package com.splunk.mapreduce.lib.rest.util;

import static org.testng.Assert.assertEquals;

import java.io.StringReader;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ReaderWrapperTest {

	private static final String STRING_READER_CONTENT = "string";
	private static final String PREFIX = "prefix";
	private static final String SUFFIX = "suffix";
	private ReaderWrapper readerWrapper;

	@BeforeMethod
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

	private void assertXMLVersionTagIsRemoved(String xmlVersionTag) {
		String readerContent = xmlVersionTag + STRING_READER_CONTENT;
		String expectedContent = PREFIX + STRING_READER_CONTENT + SUFFIX;

		readerWrapper.wrapReader(getReader(readerContent));
		String actualContent = getWrappedReaderContent();
		assertEquals(expectedContent, actualContent);
	}

	@Test
	public void should_removeXMLVersionTagFromTheReaderToBeWrapped_when_thereIsAnXMLVersionTagWithSingleQuotes() {
		String xmlVersionTag = "<?xml version='1.0' encoding='UTF-8'?>";
		assertXMLVersionTagIsRemoved(xmlVersionTag);
	}

	@Test
	public void should_removeXMLVersionTagFromTheReaderToBeWrapped_when_thereIsAnXMLVersionTagWithDoubleQuotes() {
		String xmlVersionTagWithQuotes = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>";
		assertXMLVersionTagIsRemoved(xmlVersionTagWithQuotes);
	}

	@Test
	public void should_removeXMLVersionTagFromTheReaderToBeWrapped_when_thereIsAnXMLVersionTagWithStartingWhitespaces() {
		String xmlVersionTagWithWhitespaces = "         <?xml version=\"1.0\" encoding=\"UTF-8\"?>";
		assertXMLVersionTagIsRemoved(xmlVersionTagWithWhitespaces);
	}

}
