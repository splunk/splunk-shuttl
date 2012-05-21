package com.splunk.shuttl.archiver.model;

import java.io.IOException;

/**
 * Thrown when an expected directory is a file.
 */
public class FileNotDirectoryException extends IOException {

    public FileNotDirectoryException(String string) {
	super(string);
    }

    /**
     * Default generated serial version uid.
     */
    private static final long serialVersionUID = 1L;

}
