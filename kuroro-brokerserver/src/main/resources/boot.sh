#
# Copyright 2017 EpochArch.com
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


#!/bin/bash
# chkconfig: 2345 45 65
# description: kuroro-mq daemon

if [ $USER != deploy ]; then
    echo "User deploy is needed, quit!"
   exit 0
fi

. /etc/init.d/functions


if [ x"$JAVA_HOME" == x ]; then
    export JAVA_HOME=/usr/j2sdk
    export CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar
    export PATH=$JAVA_HOME/bin:$PATH
fi
BASE_BIN_DIR='/var/www/webapps/kuroro-brokerserver'
BASE=`basename $0`

send_email() {
    IP=`/sbin/ip a | egrep -v 'inet6|127.0.0.1|\/32' | awk -F'[ /]+' '/inet/{print $3}' | head -n1`
    TIME=`date +"%F %T"`
}

check_pid() {
    PID=$(ps -ef | grep kuroro.broker.MainBootStrap | grep -v grep | awk '{print $2}' | head -1)
    [ x"$PID" == x ] && return 0 || return 1  #1:exist; 0:not exist
}

start() {
    check_pid
    if [ x$PID != x ]; then
        echo "${BASE} (pid $PID) is running..."
    else
        echo -n "Starting $BASE:"
        cd $BASE_BIN_DIR && sh start.sh > /dev/null 2>&1
        retval=$?
        sleep 0.5
        check_pid
        if [ x$PID != x -a $retval -eq 0 ]; then
            echo_success
            echo
        else
            echo_failure
            echo
        fi
    fi
}

stop() {
    check_pid
    if [ x$PID == x ]; then
        echo "${BASE} is stopped."
    else
        echo -n "Stopping $BASE:"
        cd $BASE_BIN_DIR && sh shutdown.sh > /dev/null 2>&1
        retval=$?
        sleep 0.5
        check_pid
        if [ x$PID == x -a $retval -eq 0 ]; then
            echo_success
            echo
        else
            echo_failure
            echo
        fi
    fi
}

status() {
    check_pid
    if [ x$PID != x ]; then
        echo "${BASE} (pid $PID) is running..."
    else
        echo "${BASE} is stopped."
    fi
}

check() {
    check_pid
    if [ x$PID != x ]; then
        echo "${BASE} (pid $PID) is running..."
    else
        echo -e "${BASE} is stopped, trying to start:"
        start
        send_email > /dev/null 2>&1
    fi
}

#main
case $1 in
    start)
        start
        ;;
    stop)
        stop
        ;;
    ''|restart)
        stop
        sleep 0.5
        start
        ;;
    status)
        status
        ;;
    check)
        check
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|check}"
        exit 1
esac

