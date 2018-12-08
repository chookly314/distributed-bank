#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

java -cp $DIR/distributedBank.jar es.upm.dit.cnvr.distributedBank.BankCore $DIR