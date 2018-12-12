#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

java -cp $DIR/distributedBank.jar -DlogFile=distributedBank_$(date +%H:%M:%S).log es.upm.dit.cnvr.distributedBank.BankCore $DIR 2>/dev/null