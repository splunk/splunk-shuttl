package com.splunk.shep.testutil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * HadoopFileSystemPutter puts a file on a hadoop {@link FileSystem} in a safe
 * place. <br>
 * It uses the {@link SafePathCreator} to get a unique, readable and writable
 * directory on Hadoop. <br>
 * Use convenience method {@link HadoopFileSystemPutter#get(FileSystem)} to
 * instanciate this class with whatever filesystem you'd like to use.
 * 
 * @author periksson
 * 
 */
public class HadoopFileSystemPutter {

    public static class LocalFileNotFound extends RuntimeException {
	private static final long serialVersionUID = 1L;
    }

    private final FileSystem fileSystem;
    private final SafePathCreator safePathCreator;

    public HadoopFileSystemPutter(FileSystem fileSystem,
	    SafePathCreator safePathCreator) {
	this.fileSystem = fileSystem;
	this.safePathCreator = safePathCreator;
    }

    public void putFile(File source) {
	if (!source.exists())
	    throw new LocalFileNotFound();
	else
	    doPutFile(source);
    }

    private void doPutFile(File src) {
	try {
	    fileSystem.copyFromLocalFile(new Path(src.getPath()),
		    getSafePathOnFileSystemForFile(src));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private Path getSafePathOnFileSystemForFile(File src) {
	Path safeDirectory = safePathCreator.getSafeDirectory(fileSystem);
	return new Path(safeDirectory, src.getName());
    }

    public boolean isFileCopiedToFileSystem(File file) {
	try {
	    return fileSystem.exists(getSafePathOnFileSystemForFile(file));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    public Path getPathWhereFileIsStored(File file) {
	return getSafePathOnFileSystemForFile(file);
    }

    public static HadoopFileSystemPutter get(FileSystem fileSystem) {
	return new HadoopFileSystemPutter(fileSystem, SafePathCreator.get());
    }
}
