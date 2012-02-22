package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;

/**
 * With this interface code can put, retrieve and list files in any system that
 * is used for to archiving.
 * 
 */
public interface ArchiveFileSystem {

    /**
     * Puts the specified file on local file system to the archiving file
     * system.
     * 
     * @param fileOnLocalFileSystem
     *            An existing file on the local file system.
     * @param fileOnArchiveFileSystem
     *            A non exiting file on the archive file system.
     * 
     * @throws FileNotFoundException
     *             If specified file on the local file system doesn't exist.
     * @throws FileAlreadyExistsException
     *             If the specified file on archive file system already exists.
     */
    void putFile(File fileOnLocalFileSystem,
	    File fileOnArchiveFileSystem) throws FileNotFoundException,
	    FileAlreadyExistsException;
}
