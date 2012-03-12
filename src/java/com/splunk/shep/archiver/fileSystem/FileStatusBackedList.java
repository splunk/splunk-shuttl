package com.splunk.shep.archiver.fileSystem;

import java.net.URI;
import java.util.AbstractList;

import org.apache.hadoop.fs.FileStatus;

/**
 * Wrapes around {@link FileStatus} and provides a list of URI objects.
 * 
 */
public class FileStatusBackedList extends AbstractList<URI> {

    private FileStatus[] fileStatus;
    private URI[] uriCache;

    /**
     * Creates a list backed by the specified FileStatus array.
     */
    public FileStatusBackedList(FileStatus... fileStatus) {
	super();
	this.fileStatus = fileStatus;
	uriCache = new URI[fileStatus.length];
    }

    @Override
    public URI get(int index) {
	if (uriCache[index] == null) {
	    uriCache[index] = fileStatus[index].getPath().toUri();
	}
	return uriCache[index];
    }

    @Override
    public int size() {
	return fileStatus.length;
    }

}
