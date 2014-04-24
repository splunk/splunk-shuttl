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
package com.splunk.shuttl.server.model;

import static org.testng.AssertJUnit.*;

import javax.xml.namespace.QName;

import org.testng.annotations.Test;

@Test
public class ArchiveFormatTest {

	public void create_withName_canGetName() {
		ArchiveFormat format = ArchiveFormat.create("name");
		assertEquals("name", format.getName());
		assertTrue(format.getAttributes().isEmpty());
	}

	public void create_withAttributes_canGetAttrs() {
		ArchiveFormat format = ArchiveFormat.create("_", "key", "value");
		assertEquals(format.getAttributes().get(new QName("key")), "value");
	}

	@Test(expectedExceptions = { RuntimeException.class })
	public void create_withTooFew_throws() {
		ArchiveFormat.create("_", "key-only");
	}
}
