#!/usr/bin/env bash
WORKDIR="/home/alidezhihui/incubator-resilientdb"

kill "$(tail -n1 $WORKDIR/server1.pid)"