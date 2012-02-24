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
     *            Path pointing for an non exiting file on the archive file
     *            system.
     * 
     * @throws FileNotFoundException
     *             If specified file on the local file system doesn't exist.
     * @throws FileOverwriteException
     *             If there is already a file on the specified path.
     */
    void putFile(File fileOnLocalFileSystem,
	    FileSystemPath fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException;
    
    /**
     * Retries the file from specified path on archiving file system and stores
     * it to the specified file on local file system.
     * 
     * @param fileOnLocalFileSystem
     *            A non exiting file on the local file system.
     * @param fileOnArchiveFileSystem
     *            A path to an existing file on the archiving file system.
     * @throws FileNotFoundException
     *             If there isn't a file on the archiving file system with the
     *             specified path.
     * @throws FileOverwriteException
     *             If there is already a file on the local file system.
     */
    void getFile(File fileOnLocalFileSystem,
	    FileSystemPath fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException;
    
}
