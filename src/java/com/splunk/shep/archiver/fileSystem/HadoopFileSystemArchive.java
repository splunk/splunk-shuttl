package com.splunk.shep.archiver.fileSystem;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HadoopFileSystemArchive implements ArchiveFileSystem {

    private FileSystem hadoopFileSystem;

    public HadoopFileSystemArchive(FileSystem hadoopFileSystem) {
	this.hadoopFileSystem = hadoopFileSystem;
    }

    @Override
    public void putFile(File fileOnLocalFileSystem,
	    FileSystemPath fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	// TODO Auto-generated method stub
    }

    @Override
    public void getFile(File fileOnLocalFileSystem,
	    FileSystemPath fileOnArchiveFileSystem)
	    throws FileNotFoundException, FileOverwriteException, IOException {
	// TODO Auto-generated method stub

    }

    @Override
    public List<FileSystemPath> listPath(FileSystemPath pathToBeListed)
	    throws IOException {
	// TODO Auto-generated method stub
	return null;
    }

    /**
     * Converts the specified {@link Path} object defined in Hadoop to a
     * {@link FileSystemPath} defined in out project.
     * 
     * @param hadoopPath
     *            The {@link Path} object as defined by Hadoop project.
     * @return a {@link FileSystemPath} object representing the speicified .
     *         hadoopPath
     */
    public static FileSystemPath convertHadoopPathToFilesystemPath(Path hadoopPath) {
	return new FileSystemPath(createFileSystemString(hadoopPath));
    }
    
    private static String createFileSystemString(Path hadoopPath) {
	if(hadoopPath.getParent() == null) {
	    return "";
	} else {
	    return createFileSystemString(hadoopPath.getParent())
		    + FileSystemPath.SEPERATOR + hadoopPath.getName();
	}
    }

}
