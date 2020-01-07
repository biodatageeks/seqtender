#!/usr/bin/env bash

# extract version
version=`grep 'version :=' build.sbt | sed  's|version := \"||g' | sed 's|\"||g'`
echo Version is $version


cd performance/bdg_perf && papermill seqtender-perf-report.ipynb seqtender-perf-report_out.ipynb -k sequila && jupyter nbconvert seqtender-perf-report_out.ipynb


docker build --no-cache -t zsi-bio/bdg-seqtender-snap-perf .
if [ $(docker ps | grep bdg-seqtender-snap-perf | wc -l) -gt 0 ]; then docker stop bdg-seqtender-snap-perf && docker rm bdg-seqtender-snap-perf; fi
docker run -p 84:80 -d --name bdg-seqtender-snap-perf zsi-bio/bdg-seqtender-snap-perf
