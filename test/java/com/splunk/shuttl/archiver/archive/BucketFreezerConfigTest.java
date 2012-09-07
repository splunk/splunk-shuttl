// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.server.mbeans.ShuttlMBeanException;

@Test(groups = { "fast-unit" })
public class BucketFreezerConfigTest {

	private Runtime runtime;
	private BucketFreezerProvider bucketFreezerProvider;
	private BucketFreezer bucketFreezer;
	private RegistersArchiverMBean registersArchiverMBeans;
	private String existingPath;

	@BeforeMethod
	public void setUp() {
		runtime = mock(Runtime.class);
		bucketFreezerProvider = mock(BucketFreezerProvider.class);
		bucketFreezer = mock(BucketFreezer.class);
		registersArchiverMBeans = mock(RegistersArchiverMBean.class);
		existingPath = createDirectory().getAbsolutePath();
	}

	public void main_correctArguments_registerAndUnregisterTheShuttlArchiverMBeanBetweenCreatingBucketFreezerAndFreezing()
			throws Exception {
		when(bucketFreezerProvider.getConfiguredBucketFreezer()).thenReturn(
				bucketFreezer);
		runMainWithCorrectArguments();

		InOrder inOrder = inOrder(registersArchiverMBeans, bucketFreezerProvider,
				bucketFreezer);
		inOrder.verify(registersArchiverMBeans).register();
		inOrder.verify(bucketFreezerProvider).getConfiguredBucketFreezer();
		inOrder.verify(bucketFreezer).freezeBucket(anyString(), anyString());
		inOrder.verify(registersArchiverMBeans).unregister();
		inOrder.verifyNoMoreInteractions();
	}

	private void runMainWithCorrectArguments() {
		BucketFreezer.runMainWithDependencies(runtime, bucketFreezerProvider,
				registersArchiverMBeans, "index", existingPath);
	}

	public void main_bucketFreezerThrowsException_stillUnregistersTheMBean()
			throws Exception {
		doThrow(RuntimeException.class).when(bucketFreezerProvider)
				.getConfiguredBucketFreezer();

		try {
			runMainWithCorrectArguments();
			fail("Exception is expected");
		} catch (RuntimeException e) {
		}
		verify(registersArchiverMBeans).unregister();
	}

	public void main_registerMBeansThrowsShuttlMBeanException_exitWithCouldNotConfigureBucketFreezer()
			throws Exception {
		doThrow(ShuttlMBeanException.class).when(registersArchiverMBeans)
				.register();

		runMainWithCorrectArguments();

		verify(runtime).exit(BucketFreezer.EXIT_COULD_NOT_CONFIGURE_BUCKET_FREEZER);
	}
}
