#!/bin/env bash

export LD_LIBRARY_PATH=/u01/app/oracle/product/11.2.0/dbhome_1//lib
export ORACLE_HOME=/u01/app/oracle/product/11.2.0/dbhome_1/

python "/home/oracle/scripts/oracle_metrics.py" "-u" "system" "-p" "bdtbdt" "-s" PBDT
