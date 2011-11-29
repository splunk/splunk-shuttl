package com.splunk.shep.mapreduce.lib.rest.util;

import java.io.IOException;
import java.io.Reader;

import org.apache.commons.io.IOUtils;

/**
 * Reads the content of a reader
 * 
 * @author periksson
 * 
 */
public class ContentReader {

	private final Reader reader;

	public ContentReader(Reader reader) {
		this.reader = reader;
	}

	public String getContent() {
		try {
			return IOUtils.toString(reader);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
}
