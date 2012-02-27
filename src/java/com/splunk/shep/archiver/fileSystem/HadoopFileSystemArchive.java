package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

    private FileSystem hadoopFileSystem;

    public HadoopFileSystemArchive(FileSystem hadoopFileSystem) {
	this.hadoopFileSystem = hadoopFileSystem;
    }

    @Override
    public void putFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	throwExceptionIfFileDoNotExist(fileOnLocalFileSystem);
	Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
	throwExceptionIfRemotePathAllreadyExist(hadoopPath);
	Path localPath = createPathFromFile(fileOnLocalFileSystem);
	hadoopFileSystem.copyFromLocalFile(localPath, hadoopPath);
    }

    @Override
    public void getFile(File fileOnLocalFileSystem, URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	throwExceptionIfFileAllreadyExist(fileOnLocalFileSystem);
	Path localPath = createPathFromFile(fileOnLocalFileSystem);
	Path hadoopPath = createPathFromURI(fileOnArchiveFileSystem);
	// FileNotFoundException is already thrown by copyToLocalFile.
	hadoopFileSystem.copyToLocalFile(hadoopPath, localPath);
    }

    @Override
    public List<URI> listPath(URI pathToBeListed) throws IOException {
	// TODO Auto-generated method stub
	return null;
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

    private void throwExceptionIfFileAllreadyExist(File file)
	    throws FileOverwriteException {
	if (file.exists()) {
	    throw new FileOverwriteException(file.toString()
		    + " allready exist.");
	}
    }

    private void throwExceptionIfRemotePathAllreadyExist(Path path)
	    throws IOException {
	if (hadoopFileSystem.exists(path)) {
	    throw new FileOverwriteException(path.toString()
		    + " allready exist.");
	}
    }

}
