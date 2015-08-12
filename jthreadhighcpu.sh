#!/bin/sh

LOG_FILE="/home/work/local/mop/jcpu.log";
JSTACK_FILE="/home/work/local/mop/jstack.log";

PID="$1";
shift;
i=0;
j="$1";
if [ -z "${j}" ]; then
    j=5;
fi

ps -mp ${PID} -o THREAD,tid,time | sort -rn > ${LOG_FILE};
jstack ${PID} > ${JSTACK_FILE};

for LINE in `cat ${LOG_FILE}|gawk -F '-' '{print $5}'|gawk -F ' ' '{print $1}'`
do
    i=$(($i+1));
    if (($i>$j)); then
        break;
    fi;
    XPID=`printf "%x\n" ${LINE}`;
    echo ${XPID};
    grep -A 10 "0x${XPID}" ${JSTACK_FILE};
done;
