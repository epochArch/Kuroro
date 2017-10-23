#!/bin/sh
day=`date -d -8days +%Y-%m-%d`
rm -rf /var/log/webapps//*$day*

yesterday=`date -d -1days +%Y-%m-%d`
cd /var/log/webapps/
for file in `ls | grep $yesterday"$"`;do
    tar czf "$file".tar.gz $file
    rm -rf $file
done