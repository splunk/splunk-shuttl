package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splunk.shep.archiver.util.UtilsPath;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

    private final Path atomicPutTmpPath;
    private final FileSystem hadoopFileSystem;

    private static Logger logger = Logger
	    .getLogger(HadoopFileSystemArchive.class);

    public HadoopFileSystemArchive(FileSystem hadoopFileSystem, Path path) {
	atomicPutTmpPath = path;
	this.hadoopFileSystem = hadoopFileSystem;
    }

    @Override
    public void putFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	throwExceptionIfFileDoNotExist(fileOnLocalFileSystem);
	Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
	throwExceptionIfRemotePathAlreadyExist(hadoopPath);
	Path localPath = createPathFromFile(fileOnLocalFileSystem);
	hadoopFileSystem.copyFromLocalFile(localPath, hadoopPath);
    }

    @Override
    public void putFileAtomically(File fileOnLocalFileSystem,
	    URI fileOnArchiveFileSystem) throws FileNotFoundException,
	    FileOverwriteException, IOException {
	Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
	throwExceptionIfRemotePathAlreadyExist(hadoopPath);
	Path tmpLocation = putFileToTmpDirectoryOverwirtingOldFilesAppendingPath(
		fileOnLocalFileSystem,
		fileOnArchiveFileSystem);
	move(tmpLocation, hadoopPath);
    }

    /**
     * Do NOT call nor override this method outside this class.It's meant to be
     * private but is package private for testing purposes. If you want to
     * expose this method make it public or protected!
     */
    /* package private */void deletePathRecursivly(Path fileOnArchiveFileSystem)
	    throws IOException {
	hadoopFileSystem.delete(fileOnArchiveFileSystem, true);
    }

    /**
     * Do NOT call nor override this method outside this class.It's meant to be
     * private but is package private for testing purposes. If you want to
     * expose this method make it public or protected!
     */
    /* package private */void move(Path src, Path dst) throws IOException {
	hadoopFileSystem.mkdirs(dst.getParent());
	hadoopFileSystem.rename(src, dst);

    }

    /**
     * Do NOT call nor override this method outside this class.It's meant to be
     * private but is package private for testing purposes. If you want to
     * expose this method make it public or protected!
     * 
     * The specified file will be copied from local file system in to the tmp
     * directory on hadoop. The tmp directory will be the base and the full path
     * of the file on hadoop will contains the specified URI.
     */
    /* package private */Path putFileToTmpDirectoryOverwirtingOldFilesAppendingPath(
	    File fileOnLocalFileSystem, URI appendPathToTmpDirectory)
	    throws FileNotFoundException, IOException {

	Path hadoopPath = UtilsPath.createPathByAppending(atomicPutTmpPath,
		createPathFromURI(appendPathToTmpDirectory));
	deletePathRecursivly(hadoopPath);
	try {
	    putFile(fileOnLocalFileSystem, hadoopPath.toUri());

	} catch (FileOverwriteException e) {
	    throw new IOException(
		    "The old tmp path was not deleted this shouldn't happen!",
		    e);
	}
	return hadoopPath;
    }

    @Override
    public void getFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	throwExceptionIfFileAlreadyExist(fileOnLocalFileSystem);
	Path localPath = createPathFromFile(fileOnLocalFileSystem);
	Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
	// FileNotFoundException is already thrown by copyToLocalFile.
	hadoopFileSystem.copyToLocalFile(hadoopPath, localPath);
    }

    @Override
    public List<URI> listPath(URI pathToBeListed) throws IOException {
	Path hadoopPath = createPathFromURI(pathToBeListed);
	FileStatus[] fileStatusOfPath = hadoopFileSystem.listStatus(hadoopPath);
	if (fileStatusOfPath != null) {
	    return new FileStatusBackedList(fileStatusOfPath);
	} else {
	    return Collections.emptyList();
	}
    }

    private Path createPathFromURI(URI uri) {
	return new Path(uri);
    }

    private Path createPathFromFile(File file) {
	return createPathFromURI(file.toURI());
    }

    private void throwExceptionIfFileDoNotExist(File file)
	    throws FileNotFoundException {
	if (!file.exists()) {
	    throw new FileNotFoundException(file.toString() + " doesn't exist.");
	}
    }

    private void throwExceptionIfFileAlreadyExist(File file)
	    throws FileOverwriteException {
	if (file.exists()) {
	    throw new FileOverwriteException(file.toString()
		    + " already exist.");
	}
    }

    private void throwExceptionIfRemotePathAlreadyExist(Path path)
	    throws IOException {
	if (hadoopFileSystem.exists(path)) {
	    throw new FileOverwriteException(path.toString()
		    + " already exist.");
	}
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.archiver.fileSystem.ArchiveFileSystem#getSize(java.net
     * .URI)
     */
    @Override
    public Long getSize(URI uri) throws IOException {
	FileStatus fileStatus = hadoopFileSystem
		.getFileStatus(createPathFromURI(uri));
	long blockSize = fileStatus.getBlockSize();
	logger.info("file status: " + fileStatus + ", block size: " + blockSize
		+ ", path: " + fileStatus.getPath() + ", modTime: "
		+ fileStatus.getModificationTime());
	return blockSize;
    }

}
