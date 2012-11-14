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
import java.util.ArrayList;
import java.util.List;

import javax.management.InstanceNotFoundException;

import org.apache.log4j.Logger;

import com.splunk.shuttl.server.mbeans.ShuttlArchiver;
import com.splunk.shuttl.server.mbeans.ShuttlArchiverMBean;

public class ArchiveConfiguration {

	private static final String ARCHIVE_DATA_DIRECTORY_NAME = "archive_data";
	private static final String TEMPORARY_DATA_DIRECTORY_NAME = "temporary_data";

	private final String localArchiverDir;
	private final List<BucketFormat> bucketFormats;
	private final String clusterName;
	private final String serverName;
	private final List<BucketFormat> bucketFormatPriority;
	private final String tempPath;
	private final String archivePath;
	private final String backendName;

	ArchiveConfiguration(String localArchiverDir,
			List<BucketFormat> bucketFormats, String clusterName, String serverName,
			List<BucketFormat> bucketFormatPriority, String tempPath,
			String archivePath, String backendName) {
		this.localArchiverDir = localArchiverDir;
		this.bucketFormats = bucketFormats;
		this.clusterName = clusterName;
		this.serverName = serverName;
		this.bucketFormatPriority = bucketFormatPriority;
		this.tempPath = tempPath;
		this.archivePath = archivePath;
		this.backendName = backendName;
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

	public static ArchiveConfiguration createConfigurationFromMBean() {
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

		String backendName = mBean.getBackendName();
		String archivePath = mBean.getArchivePath();
		String clusterName = mBean.getClusterName();
		String serverName = mBean.getServerName();
		List<BucketFormat> bucketFormatPriority = createFormatPriorityList(mBean);
		return createSafeConfiguration(mBean.getLocalArchiverDir(), archivePath,
				bucketFormats, clusterName, serverName, bucketFormatPriority,
				backendName);
	}

	public static ArchiveConfiguration createSafeConfiguration(
			String localArchiverDir, String archivePath,
			List<BucketFormat> bucketFormats, String clusterName, String serverName,
			List<BucketFormat> bucketFormatPriority, String backendName) {
		String archiveDataPath = getChildToArchivingRoot(archivePath,
				ARCHIVE_DATA_DIRECTORY_NAME);
		String archiveTempPath = getChildToArchivingRoot(archivePath,
				TEMPORARY_DATA_DIRECTORY_NAME);
		return new ArchiveConfiguration(localArchiverDir, bucketFormats,
				clusterName, serverName, bucketFormatPriority, archiveTempPath,
				archiveDataPath, backendName);
	}

	private static List<BucketFormat> bucketFormatsFromMBean(
			ShuttlArchiverMBean mBean) {
		return getFormatsFromNames(mBean.getArchiveFormats());
	}

	private static String getChildToArchivingRoot(String archivePath,
			String childNameToArchivPath) {
		return archivePath + "/" + childNameToArchivPath;
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

	public String getClusterName() {
		return clusterName;
	}

	public String getServerName() {
		return serverName;
	}

	public ArchiveConfiguration newConfigWithServerName(String serverName) {
		return new ArchiveConfiguration(localArchiverDir, bucketFormats,
				clusterName, serverName, bucketFormatPriority, tempPath, archivePath,
				backendName);
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
	public String getArchiveTempPath() {
		return tempPath;
	}

	/**
	 * @return path to where files are stored on the local file system.
	 */
	public String getLocalArchiverDir() {
		return localArchiverDir;
	}

	/**
	 * @return path where the files are stored on the archiving file system.
	 */
	public String getArchiveDataPath() {
		return archivePath;
	}

	/**
	 * @return backend name to archive data to.
	 */
	public String getBackendName() {
		return backendName;
	}

}
