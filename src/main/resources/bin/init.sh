#!/bin/bash

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

( "init-hadoop.sh"  )