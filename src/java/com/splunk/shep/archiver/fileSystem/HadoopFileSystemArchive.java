package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

    private FileSystem hadoopFileSystem;

    public HadoopFileSystemArchive(FileSystem hadoopFileSystem) {
	this.hadoopFileSystem = hadoopFileSystem;
    }

    @Override
    public void putFile(File fileOnLocalFileSystem,
 URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {

    }

    @Override
    public void getFile(File fileOnLocalFileSystem,
 URI fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public List<URI> listPath(URI pathToBeListed)
	    throws IOException {
	// TODO Auto-generated method stub
	return null;
    }
}
