// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.lang.ref.SoftReference;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import javax.management.InstanceNotFoundException;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;

public class ArchiveConfiguration {

	private static final String ARCHIVE_DATA_DIRECTORY_NAME = "archive_data";
	private static final String TEMPORARY_DATA_DIRECTORY_NAME = "temporary_data";

	private final List<BucketFormat> bucketFormats;
	private final URI archivingRoot;
	private final String clusterName;
	private final String serverName;
	private final List<BucketFormat> bucketFormatPriority;
	private final URI tmpDirectory;

	private ArchiveConfiguration(List<BucketFormat> bucketFormats,
			URI archivingRoot, String clusterName, String serverName,
			List<BucketFormat> bucketFormatPriority, URI tmpDirectory) {
		this.bucketFormats = bucketFormats;
		this.archivingRoot = archivingRoot;
		this.clusterName = clusterName;
		this.serverName = serverName;
		this.bucketFormatPriority = bucketFormatPriority;
		this.tmpDirectory = tmpDirectory;
	}

	/**
	 * Soft link so the memory can be used if needed. (Soft links are
	 * GarbageCollected only if there is really need for the memory)
	 */
	private static SoftReference<ArchiveConfiguration> sharedInstanceRef;

	public static ArchiveConfiguration getSharedInstance() {
		ArchiveConfiguration sharedInstance = null;
		if (sharedInstanceRef != null)
			sharedInstance = sharedInstanceRef.get();

		if (sharedInstance == null) {
			sharedInstance = createConfigurationFromMBean();
			sharedInstanceRef = new SoftReference<ArchiveConfiguration>(
					sharedInstance);
		}
		return sharedInstance;
	}

	private static ArchiveConfiguration createConfigurationFromMBean() {
		try {
			return createConfigurationWithMBean(ShuttlArchiver.getMBeanProxy());
		} catch (InstanceNotFoundException e) {
			logInstanceNotFoundException(e);
			throw new RuntimeException(e);
		}
	}

	private static void logInstanceNotFoundException(InstanceNotFoundException e) {
		Logger.getLogger(ArchiveConfiguration.class).error(
				did("Tried getting a ShuttlArchiverMBean", e,
						"An instance to be registered to the MBean."));
	}

	/**
	 * @return {@link ArchiveConfiguration} with properties from a
	 *         {@link ShuttlArchiverMBean}
	 */
	public static ArchiveConfiguration createConfigurationWithMBean(
			ShuttlArchiverMBean mBean) {
		List<BucketFormat> bucketFormats = bucketFormatsFromMBean(mBean);
		URI archivingRootURI = archivingRootFromMBean(mBean);

		String clusterName = mBean.getClusterName();
		String serverName = mBean.getServerName();
		List<BucketFormat> bucketFormatPriority = createFormatPriorityList(mBean);
		return createSafeConfiguration(archivingRootURI, bucketFormats,
				clusterName,
				serverName, bucketFormatPriority);
	}

	public static ArchiveConfiguration createSafeConfiguration(
			URI archivingRootURI,
			List<BucketFormat> bucketFormats, String clusterName, String serverName,
			List<BucketFormat> bucketFormatPriority) {
		URI archivingData = getChildToArchivingRoot(archivingRootURI,
				ARCHIVE_DATA_DIRECTORY_NAME);
		URI tmpDirectory = getChildToArchivingRoot(archivingRootURI,
				TEMPORARY_DATA_DIRECTORY_NAME);
		return new ArchiveConfiguration(bucketFormats, archivingData, clusterName,
				serverName, bucketFormatPriority, tmpDirectory);
	}

	private static URI archivingRootFromMBean(ShuttlArchiverMBean mBean) {
		String archivingRoot = mBean.getArchiverRootURI();
		return archivingRoot != null ? URI.create(archivingRoot) : null;
	}

	private static List<BucketFormat> bucketFormatsFromMBean(
			ShuttlArchiverMBean mBean) {
		return getFormatsFromNames(mBean.getArchiveFormats());
	}

	private static URI getChildToArchivingRoot(URI archivingRoot,
			String childNameToArchivingRoot) {
		if (archivingRoot != null) {
			String rootName = FilenameUtils.getName(archivingRoot.getPath());
			return archivingRoot.resolve(rootName + "/" + childNameToArchivingRoot);
		} else {
			return null;
		}
	}

	private static List<BucketFormat> createFormatPriorityList(
			ShuttlArchiverMBean mBean) {
		List<String> formatNames = mBean.getBucketFormatPriority();
		return getFormatsFromNames(formatNames);
	}

	private static List<BucketFormat> getFormatsFromNames(List<String> formatNames) {
		List<BucketFormat> bucketFormats = new ArrayList<BucketFormat>();
		if (formatNames != null)
			for (String format : formatNames)
				bucketFormats.add(BucketFormat.valueOf(format));
		return bucketFormats;
	}

	public List<BucketFormat> getArchiveFormats() {
		return bucketFormats;
	}

	public URI getArchivingRoot() {
		return archivingRoot;
	}

	public String getClusterName() {
		return clusterName;
	}

	public String getServerName() {
		return serverName;
	}

	/**
	 * List of bucket formats, where lower index means it has higher priority. <br/>
	 * {@link ArchiveConfiguration#getBucketFormatPriority()}.get(0) has the
	 * highest priority, while .get(length-1) has the least priority.
	 */
	public List<BucketFormat> getBucketFormatPriority() {
		return bucketFormatPriority;
	}

	/**
	 * @return The Path on hadoop filesystem that is used as a temp directory
	 */
	public URI getTmpDirectory() {
		return tmpDirectory;
	}

}
