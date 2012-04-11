#!/bin/bash

rsync -vr ~/splunk/splunk-shep/package/* ~/splunk/splunk-shep/build-cache/splunk/etc/apps/shep/
~/splunk/splunk-shep/build-cache/splunk/bin/splunk restart 
