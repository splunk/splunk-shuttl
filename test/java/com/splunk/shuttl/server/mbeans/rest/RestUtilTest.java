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
package com.splunk.shuttl.server.mbeans.rest;

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.testng.annotations.Test;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

@Test(groups = { "fast-unit" })
public class RestUtilTest {

	public void writeKeyValueAsJson_noKeyValues_emptyJson() {
		assertJsonEquals(RestUtil.writeKeyValueAsJson(), new JSONObject());
	}

	@Test(expectedExceptions = { RuntimeException.class })
	public void writeKeyValueAsJson_keyNoValue_throws() {
		RestUtil.writeKeyValueAsJson("key");
	}

	public void writeKeyValueAsJson_keyAndValue_jsonWithKeyAndValue()
			throws JSONException {
		JSONObject expected = new JSONObject();
		expected.put("key", "value");
		JSONObject actual = RestUtil.writeKeyValueAsJson("key", "value");
		assertJsonEquals(expected, actual);
	}

	public void writeKeyValueAsJson_emptyList_brackets() {
		JSONObject actual = RestUtil.writeKeyValueAsJson("key",
				new ArrayList<String>());
		assertEquals(actual.toString(), "{\"key\":[]}");
	}

	public void writeKeyValueAsJson_list_isJSONArray() throws JSONException {
		JSONObject actual = RestUtil.writeKeyValueAsJson("key",
				new ArrayList<String>());
		assertTrue(actual.get("key") instanceof JSONArray);
	}

	public void writeKeyValueAsJson_map_isJSONObject() throws JSONException {
		JSONObject actual = RestUtil.writeKeyValueAsJson("key",
				new HashMap<String, String>());
		assertTrue(actual.get("key") instanceof JSONObject);
	}

	public void writeKeyValueAsJson_listWithThings_thingsInBrackets() {
		JSONObject actual = RestUtil.writeKeyValueAsJson("key", asList("v1", "v2"));
		assertEquals(actual.toString(), "{\"key\":[\"v1\",\"v2\"]}");
	}

	public void writeKeyValueAsJson_emptySet_brackets() {
		JSONObject actual = RestUtil.writeKeyValueAsJson("key",
				new HashSet<String>());
		assertEquals(actual.toString(), "{\"key\":[]}");
	}

	private void assertJsonEquals(JSONObject o1, JSONObject o2) {
		assertEquals(o1.toString(), o2.toString());
	}

}
