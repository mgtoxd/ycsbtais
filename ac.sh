#!/bin/bash

for i in {1..10000};do

  ./bin/ycsb run tais -s -P workloads/workloadtais -p "tais.target=172.111.0.$1:5000" > outputRunttttwwtttt$i.txt

done