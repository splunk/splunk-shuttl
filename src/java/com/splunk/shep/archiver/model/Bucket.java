package com.splunk.shep.archiver.model;

import static com.splunk.shep.archiver.ArchiverLogger.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

import org.apache.commons.io.FileUtils;

import com.splunk.shep.archiver.archive.BucketFormat;

/**
 * Model representing a Splunk bucket
 */
public class Bucket {

    private final BucketFormat format;
    private final File directory;
    private final String indexName;
    private final BucketName bucketName;
    private final URI uri;

    /**
     * Bucket with an index and format<br/>
     * 
     * @param indexName
     *            The name of the index that this bucket belongs to.
     * @param directory
     *            that is the bucket
     * @throws FileNotFoundException
     *             if the file doesn't exist
     * @throws FileNotDirectoryException
     *             if the file is not a directory
     */
    public Bucket(String indexName, File directory)
	    throws FileNotFoundException, FileNotDirectoryException {
	this(directory.toURI(), directory, indexName, directory.getName(),
		BucketFormat.getFormatFromDirectory(directory));
    }

    /**
     * Invokes {@link #Bucket(String, File)}, by creating a file from specified
     * bucketPath
     */
    public Bucket(String indexName, String bucketPath)
	    throws FileNotFoundException, FileNotDirectoryException {
	this(indexName, new File(bucketPath));
    }

    /**
     * Bucket created with an URI to support remote buckets.
     * 
     * @param uri
     *            to bucket
     * @param index
     *            that the bucket belongs to.
     * @param bucketName
     *            that identifies the bucket
     * @param format
     *            of this bucket
     * @throws FileNotFoundException
     * @throws FileNotDirectoryException
     */
    public Bucket(URI uri, String index, String bucketName, BucketFormat format)
	    throws FileNotFoundException, FileNotDirectoryException {
	this(uri, getFileFromUri(uri), index, bucketName, format);
    }

    private Bucket(URI uri, File directory, String index, String bucketName,
	    BucketFormat format) throws FileNotFoundException,
	    FileNotDirectoryException {
	this.uri = uri;
	this.directory = directory;
	this.indexName = index;
	this.bucketName = new BucketName(bucketName);
	this.format = format;
	verifyExistingDirectory(directory);
    }

    private static File getFileFromUri(URI uri) {
	if (uri.getScheme().equals("file")) {
	    return new File(uri);
	}
	return null;
    }

    private void verifyExistingDirectory(File directory)
	    throws FileNotFoundException, FileNotDirectoryException {
	if (isRemote())
	    return;
	if (!directory.exists()) {
	    throw new FileNotFoundException("Could not find directory: "
		    + directory);
	} else if (!directory.isDirectory()) {
	    throw new FileNotDirectoryException("Directory " + directory
		    + " is not a directory");
	}
    }

    /**
     * @return The directory that this bucket has its data in.
     */
    public File getDirectory() {
	if (directory == null) {
	    did("Got directory from bucket",
		    "Bucket was remote and can't instantiate a File.", "",
		    "bucket", this);
	    throw new RemoteBucketException();
	}
	return directory;
    }

    /**
     * @return The name of this bucket.
     */
    public String getName() {
	return bucketName.getName();
    }

    /**
     * @return The name of the index that this bucket belong to.
     */
    public String getIndex() {
	return indexName;
    }

    /**
     * @return {@link BucketFormat} of this bucket.
     */
    public BucketFormat getFormat() {
	return format;
    }

    /**
     * @return {@link URI} of this bucket.
     */
    public URI getURI() {
	return uri;
    }

    public Bucket moveBucketToDir(File directoryToMoveTo)
	    throws FileNotFoundException, FileNotDirectoryException {
	File directory = getDirectory();
	verifyExistingDirectory(directoryToMoveTo);
	File newName = new File(directoryToMoveTo.getAbsolutePath(),
		directory.getName());

	if (!directory.renameTo(newName)) {
	    throw new RuntimeException("Can't move bucket on this file system");
	}

	return new Bucket(getIndex(), newName);
    }

    /**
     * Deletes the bucket from the file system.
     * 
     * @throws IOException
     *             if it's not possible to delete the directory
     */
    public void deleteBucket() throws IOException {
	FileUtils.deleteDirectory(getDirectory());
    }

    @Override
    public boolean equals(Object obj) {
	if (this == obj)
	    return true;
	if (obj == null)
	    return false;
	if (getClass() != obj.getClass())
	    return false;
	Bucket other = (Bucket) obj;
	if (directory == null) {
	    if (other.directory != null)
		return false;
	} else if (!directory.getAbsolutePath().equals(
		other.directory.getAbsolutePath()))
	    return false;
	if (indexName == null) {
	    if (other.indexName != null)
		return false;
	} else if (!indexName.equals(other.indexName))
	    return false;
	return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
	return "Bucket [format=" + format + ", directory=" + directory
		+ ", indexName=" + indexName + ", bucketName=" + bucketName
		+ ", uri=" + uri + "]";
    }

    /**
     * @return true if the bucket is not on the local file system.
     */
    public boolean isRemote() {
	return !uri.getScheme().equals("file");
    }

    /**
     * @return {@link Date} with earliest time of indexed data in the bucket.
     */
    public Date getEarliest() {
	return new Date(bucketName.getEarliest());
    }

    /**
     * @return {@link Date} with latest time of indexed data in the bucket.
     */
    public Date getLatest() {
	return new Date(bucketName.getLatest());
    }

}
