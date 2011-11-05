# Copyright 2011 Splunk, Inc.                                                                       
#                                                                                                        
# Licensed under the Apache License, Version 2.0 (the "License");                                      
# you may not use this file except in compliance with the License.                                     
# You may obtain a copy of the License at                                                              
#                                                                                                        
#   http://www.apache.org/licenses/LICENSE-2.0                                                       
#                                                                                                        
# Unless required by applicable law or agreed to in writing, software                                  
# distributed under the License is distributed on an "AS IS" BASIS,                                    
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.                             
# See the License for the specific language governing permissions and                                  
# limitations under the License.

# This is an example script for archiving cold buckets to HDFS. 
# It must be modified to suit your individual needs, and 
# we highly recommend testing this on a non-production instance 
# before deploying it.
echo "Calling exporttool with $1" at `date` >> $SPLUNK_HOME/var/log/splunk/coldtofrozen.log
$SPLUNK_HOME/bin/exporttool $1 $1.csv -csv
# assuming dir /splunk/output exists on hadoop, replace with any other dir
$HADOOP_HOME/bin/hadoop dfs -put $1.csv /splunk/output/`basename $1`.csv > $1.hdfs.out 2>&1

