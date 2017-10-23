#!/bin/sh
#need python env
python closeMonitor.py

###kill pid
PID=$(ps -ef | grep kuroro.broker.MainBootStrap | grep -v grep | awk '{print $2}' | head -1)
if [ $PID > 0  ];then
  kill -9  $PID
  echo "Kill Kuroro Pid..."
fi