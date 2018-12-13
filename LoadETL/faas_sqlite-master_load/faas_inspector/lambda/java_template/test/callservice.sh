#!/bin/bash

# JSON object to pass to Lambda Function

json={"\"filename\"":"\"500000\u0020Sales\u0020Records.csv\",\"bucketname\"":\"test.bucket.562.firsttest\",\"param2\"":2,\"param3\"":3}

echo "Invoking Lambda function using API Gateway"
start_time_api="$(date -u +%s.%N)"
time output=`curl -s -H "Content-Type: application/json" -X POST -d  $json https://ffs385ccr0.execute-api.us-east-2.amazonaws.com/loadCSV_dev`
echo ""
echo "CURL RESULT:"
echo $output
end_time_api="$(date -u +%s.%N)"
elapsed_api="$(bc <<<"$end_time_api-$start_time_api")"
echo "Total of $elapsed_api seconds elapsed for AWS API Gateway process"
echo ""
echo ""

echo "Invoking Lambda function using AWS CLI"
start_time="$(date -u +%s.%N)"
time output=`aws lambda invoke --invocation-type RequestResponse --function-name loadCSV --region us-east-2 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`
echo ""
echo "AWS CLI RESULT:"
echo $output
end_time="$(date -u +%s.%N)"
elapsed="$(bc <<<"$end_time-$start_time")"
echo "Total of $elapsed seconds elapsed for AWS CLI process"
echo ""
