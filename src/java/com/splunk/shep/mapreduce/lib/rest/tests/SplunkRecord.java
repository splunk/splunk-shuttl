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

package com.splunk.shep.mapreduce.lib.rest.tests;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;

import com.splunk.shep.mapreduce.lib.rest.SplunkWritable;

public class SplunkRecord implements Writable, SplunkWritable {
    Map<String, String> map = null;

    @Override
    public void setMap(Map<String, String> m) {
	this.map = m;
    }

    public Map<String, String> getMap() {
	return this.map;
    }

    @Override
    public void readFields(DataInput arg0) throws IOException {
	// TODO Auto-generated method stub
    }

    @Override
    public void write(DataOutput arg0) throws IOException {
	// TODO Auto-generated method stub
    }
}
