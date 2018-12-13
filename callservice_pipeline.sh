#!/bin/bash

# JSON object to pass to Lambda Function

echo "Invoking Lambda function using AWS CLI"  >> SequentialTestOutput.txt
start_time="$(date -u +%s.%N)"
echo "start time : $start_time" >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
echo "Invoking Transform Service"  >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
json={"\"bucketname\"":"\"test.bucket.562.firsttest\"","\"filename\"":"\"10000\u0020Sales\u0020Records.csv\"","\"seqNum\"":$2,"\"threadId\"":$3}
time output=`aws lambda invoke --invocation-type RequestResponse --function-name ClientFlow_Transform --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "AWS CLI RESULT:"  >> SequentialTestOutput.txt
echo $output  >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
echo "Invoking Load Service" >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
transformedfile=newsales_$2.csv
json1={"\"bucketname\"":\"test.bucket.562.firsttest\"","\"filename\"":\"$transformedfile\"","\"seqNum\"":$2,"\"threadId\"":$3}
time output=`aws lambda invoke --invocation-type RequestResponse --function-name ClientFlow_Load --region us-east-2 --payload $json1 /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "AWS CLI RESULT:"  >> SequentialTestOutput.txt
echo $output  >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
echo "Invoking Query Service" >> SequentialTestOutput.txt
echo "#############################" >>SequentialTestOutput.txt
databasefile=salesdatabase_$2.db
json2={"\"filename\"":"\"$databasefile\"",\"bucketname\"":\"test.bucket.562.firsttest\"","\"seqNum\"":$2,"\"threadId\"":$3}
time output=`aws lambda invoke --invocation-type RequestResponse --function-name ClientFlow_Query --region us-east-2 --payload $json2 /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "AWS CLI RESULT:"  >> SequentialTestOutput.txt
echo $output  >> SequentialTestOutput.txt
end_time="$(date -u +%s.%N)"
elapsed="$(bc <<<"$end_time-$start_time")"
echo "Total of $elapsed seconds elapsed for process through AWS CLI"  >> SequentialTestOutput.txt
printf '%s\n'$2 $elapsed | paste -sd ',' >> Results_Seq_Transform.csv
echo ""


