package com.splunk.shep.archiver.archive;

import java.lang.ref.SoftReference;
import java.net.URI;

import org.apache.hadoop.fs.Path;

// CONFIG This whole class should use MBeans
public class ArchiveConfiguration {
    
    /**
     * Soft link so the memory can be used if needed. (Soft links are
     * GarbageCollected only if there is really need for the memory)
     */
    private static SoftReference<ArchiveConfiguration> sharedInstanceRef;

    private static final String ARCHIVING_ROOT = "archiving_root";
    private static final String CLUSTER_NAME = "cluster_name";
    private static final String SERVER_NAME = "server_name";
    private static final String TMP_DIRECTORY_OF_ARCHIVER = "/archiver-tmp";
    private static final URI archiverHadoopURI = URI
	    .create("hdfs://localhost:9000");

    protected ArchiveConfiguration() {
	super();
	/*
	 * If the configuration of ArchiveConfiguration is time consuming maybe
	 * the shared instance should be hardlinked.
	 */
    }

    public static ArchiveConfiguration getSharedInstance() {
	ArchiveConfiguration sharedInstance = null;
	if (sharedInstanceRef != null) {
	    sharedInstance = sharedInstanceRef.get();
	}
	if (sharedInstance == null) {
	    sharedInstance = new ArchiveConfiguration();
	    sharedInstanceRef = new SoftReference<ArchiveConfiguration>(
		    sharedInstance);
	}
	return sharedInstance;
    }

    public BucketFormat getArchiveFormat() {
	return BucketFormat.SPLUNK_BUCKET;
    }

    public String getArchivingRoot() {
	return ARCHIVING_ROOT;
    }

    public String getClusterName() {
	return CLUSTER_NAME;
    }

    public String getServerName() {
	return SERVER_NAME;
    }

    /**
     * @return The Path on hadoop filesystem that is used as a temp directory
     */
    public Path getTmpDirectory() {
	return new Path(new Path(getArchiverHadoopURI()),
		TMP_DIRECTORY_OF_ARCHIVER);
    }
    
    /**
     * @return URI pointing to the hadoop filesystem instance that is used for
     *         archiving.
     */
    public URI getArchiverHadoopURI() {
	return archiverHadoopURI;
    }

}
