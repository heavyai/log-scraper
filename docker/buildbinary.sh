#!/bin/bash
set -e
ROOTDIR=${PWD%/*}
docker build -q -t omniscilogbuild .
docker run -i -v $ROOTDIR/:/log-scraper-internal omniscilogbuild
