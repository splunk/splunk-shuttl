#!/bin/sh

splunkPath=$1
filePath=$2
index=$3
loremIpsum=$filePath/loremIpsum.txt

echo "\$0 = $0"
echo "\$splunkPath = $splunkPath"
echo "\$filePath = $filePath"
echo "\$loremIpsum = $loremIpsum"
echo "\$index = $index"
echo ""

ret_code=0
if [[ ! -f $loremIpsum ]]; then
	
	echo "Get some random input data"
	curl http://loripsum.net/api/1000/verylong > $loremIpsum
	ret_code=$?
	
	if [[ $ret_code != 0 ]]; then
		echo "Error when getting input data!"
		exit $ret_code
	else
		echo "Got some random input data!"
	fi
fi

i=0
while [ $i -lt 20 ]; do 
	$splunkPath/bin/splunk add oneshot $loremIpsum -index $index
	sleep 1
	let i++
done
echo "Added $i oneshots to splunk!"