#!/bin/bash


printf '%s\n'run elapsedTime_api elapsedTime_cli | paste -sd ',' >> Results_Seq_Transform.csv
for ((i=1;i<=25;i++)); 
do
  echo "Pass $i" >> SequentialTestOutput.txt
  echo "---------------------" >> SequentialTestOutput.txt
   echo "loading data of file $i"  >> SequentialTestOutput.txt
    filename=`echo "\"10000\u0020Sales\u0020Records.csv\"" | sed 's/.\(.*\)/\1/' | sed 's/\(.*\)./\1/'`
    ./callservice_pipeline.sh $filename $i 0 
done               
# End of outer loop.

exit 0

