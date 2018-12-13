#!/bin/bash

# parallel run Transform Service
./partest_Transform.sh 2 2 1 1

# parallel run Load service
./partest_Load.sh 2 2 1 1

#parallel run Query service
./partest_Query.sh 2 2 1 1

