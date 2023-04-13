#!/bin/bash

basename=bttool

usage() {
    usagestr="$basename [-h] [-b] [-s] [-r]
    where:
        -h help
        -b build docker image with docker file and tag it as backtrader
        -s start docker container
        -r run docker container bash
    "
    echo "$usagestr"
    exit -1
}

options='hbcsr'

BUILD=false
START=false
RUN=false

while getopts $options option; do
    case "$option" in
        h) usage;;
        b) BUILD="true";;
        s) START="true";;
        r) RUN="true";;
        ?) usage;;
        :) usage;;
    esac
done

shift $(($OPTIND - 1))

if [ $BUILD = "true" ]
then
    echo "try to build docker image with dockerfile"
    docker build -f dockerfile -t backtrader . 2>&1 | tee build.log
fi

if [ $START = "true" ]
then
    echo "try to start docker container backtrader"
    docker volume create btlog
    docker run -d -it --name backtrader -v btlog:/backtrader/tests/logs backtrader
fi

if [ $RUN = "true" ]
then
    echo "try to enter docker bash"
    docker exec -it backtrader /bin/bash
fi
