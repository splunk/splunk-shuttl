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
    private final MethodCallerHelper methodCallerHelper;

    public HadoopFileSystemPutter(FileSystem fileSystem,
	    SafePathCreator safePathCreator,
	    MethodCallerHelper methodCallerHelper) {
	this.fileSystem = fileSystem;
	this.safePathCreator = safePathCreator;
	this.methodCallerHelper = methodCallerHelper;
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
	Path safeDirectory = getSafePathForClassPuttingFile();
	return new Path(safeDirectory, src.getName());
    }

    private Path getSafePathForClassPuttingFile() {
	Class<?> callerToThisMethod = methodCallerHelper.getCallerToMyMethod();
	Path safeDirectory = safePathCreator.getSafeDirectory(fileSystem,
		callerToThisMethod);
	return safeDirectory;
    }

    public boolean isFileCopiedToFileSystem(File file) {
	try {
	    return fileSystem.exists(getSafePathOnFileSystemForFile(file));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    public Path getPathWhereMyFilesAreStored() {
	return getSafePathForClassPuttingFile();
    }

    public void deleteMyFiles() {
	try {
	    doDeleteMyFiles();
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private void doDeleteMyFiles() throws IOException {
	Path filesDir = getPathWhereMyFilesAreStored();
	fileSystem.delete(filesDir, true);
    }

    public Path getPathForFile(File file) {
	return getSafePathOnFileSystemForFile(file);
    }

    public static HadoopFileSystemPutter get(FileSystem fileSystem) {
	return new HadoopFileSystemPutter(fileSystem, SafePathCreator.get(),
		MethodCallerHelper.get());
    }

    public Path getPathForFileName(String fileName) {
	return getPathForFile(new File(fileName));
    }
}
