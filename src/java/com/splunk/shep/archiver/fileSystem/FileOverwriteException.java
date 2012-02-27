package com.splunk.shep.archiver.fileSystem;

import java.io.IOException;

public class FileOverwriteException extends IOException {

    private static final long serialVersionUID = 1L;

    public FileOverwriteException() {
	super();
    }

    public FileOverwriteException(String message) {
	super(message);
    }
}
