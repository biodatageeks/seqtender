#!/bin/sh
#echo "Running shell script"
while read LINE; do
   echo ${LINE}
   echo ${LINE} >> out.vcf
done