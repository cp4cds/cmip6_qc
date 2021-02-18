#!/bin/bash
f="$@"
cat $f| 
    while read id; do 
       scancel $id
       echo $id
    done
