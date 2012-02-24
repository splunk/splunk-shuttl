package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;

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

    /**
     * Lists the contents of the specified path.
     * 
     * @param pathToBeListed
     *            The returned list fill contains the contents of this path.
     * @return One of three possibilities: 1. The contents of specified path. 2.
     *         A list with only the path it self if its a file. 3. An empty list
     *         if the specified path doesn't exist OR it's an empty directory.
     */
    List<FileSystemPath> listPath(FileSystemPath pathToBeListed);

}
