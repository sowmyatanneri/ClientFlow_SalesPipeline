# SalesPipeline

This project provides services written in java for TLQ pipeline.Transform service picks csv file from s3 , transform the file and upload transformed csv file to s3.Load service Loads the transformed CSV into sqlite database and uploads database file to S3.Query service Downloads database file (if not present in /tmp), and constructs and executes queries against it based on input parameters.

We have scripts for the sequential and parallel test on the client flow architecture.

SequentialTest.sh runs the TLQ pipeline for 25 times, we can change the number of sequential runs in the script. An output CSV file is generated as output for the SequentialTest.sh

partest_PIpeline.sh runs the TLQ pipeline for 25 times, we can modify the number of parallel runs in the script.
