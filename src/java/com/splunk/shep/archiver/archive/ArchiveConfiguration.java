package com.splunk.shep.archiver.archive;

import java.lang.ref.SoftReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.splunk.shep.server.mbeans.ShepArchiverMBean;
import com.splunk.shep.server.mbeans.util.MBeanUtils;

public class ArchiveConfiguration {

    private final ShepArchiverMBean mBean;
    static private final Logger logger = Logger
	    .getLogger(ArchiveConfiguration.class);

    public ArchiveConfiguration(ShepArchiverMBean mBean) {
	this.mBean = mBean;
    }

    /**
     * Soft link so the memory can be used if needed. (Soft links are
     * GarbageCollected only if there is really need for the memory)
     */
    private static SoftReference<ArchiveConfiguration> sharedInstanceRef;

    private static ShepArchiverMBean getProxy() {
	try {
	    return MBeanUtils.getMBeanInstance(ShepArchiverMBean.OBJECT_NAME,
		    ShepArchiverMBean.class);
	} catch (Exception e) {
	    logger.error("Error when retrieving Archiver MBean proxy."
		    + e.toString());
	    throw new RuntimeException(e);
	}
    }

    public static ArchiveConfiguration getSharedInstance() {
	ArchiveConfiguration sharedInstance = null;
	if (sharedInstanceRef != null) {
	    sharedInstance = sharedInstanceRef.get();
	}
	if (sharedInstance == null) {
	    sharedInstance = new ArchiveConfiguration(getProxy());
	    sharedInstanceRef = new SoftReference<ArchiveConfiguration>(
		    sharedInstance);
	}
	return sharedInstance;
    }

    public BucketFormat getArchiveFormat() {
	return BucketFormat.valueOf(mBean.getArchiveFormat());
    }

    public URI getArchivingRoot() {
	return URI.create(mBean.getArchiverRootURI());
    }

    public String getClusterName() {
	return mBean.getClusterName();
    }

    public String getServerName() {
	return mBean.getServerName();
    }

    /**
     * List of bucket formats, where lower index means it has higher priority. <br/>
     * {@link ArchiveConfiguration#getBucketFormatPriority()}.get(0) has the
     * highest priority, while .get(length-1) has the least priority.
     */
    public List<BucketFormat> getBucketFormatPriority() {
	List<BucketFormat> tempList = new ArrayList<BucketFormat>();
	for (String format : mBean.getBucketFormatPriority()) {
	    tempList.add(BucketFormat.valueOf(format));
	}
	return tempList;
    }

    /**
     * @return The Path on hadoop filesystem that is used as a temp directory
     */
    public Path getTmpDirectory() {
	return new Path(mBean.getTmpDirectory());
    }

}
