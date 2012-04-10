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
package com.splunk.shep.archiver.thaw;

import static org.testng.AssertJUnit.*;

import java.lang.reflect.Method;

import org.testng.annotations.Test;

import com.splunk.shep.server.mbeans.ShepArchiver;
import com.splunk.shep.server.mbeans.ShepArchiverMBean;
import com.splunk.shep.server.mbeans.util.MBeanUtils;

public class BucketThawerFactoryTest {

    @Test(groups = { "fast-unit" })
    public void createDefaultThawer_default_noParameters()
	    throws SecurityException, NoSuchMethodException {
	Method method = BucketThawerFactory.class
		.getMethod("createDefaultThawer");
	assertEquals(0, method.getParameterTypes().length);
    }

    @Test(groups = { "integration" })
    public void createDefaultThawer_default_notNull() throws Exception {
	MBeanUtils.registerMBean(ShepArchiverMBean.OBJECT_NAME,
		ShepArchiver.class);
	assertNotNull(BucketThawerFactory.createDefaultThawer());
    }
}
