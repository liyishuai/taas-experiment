#!/bin/bash

if [ $# -ne 3 ]; then
  echo "请提供两个参数"
  exit 1
fi

if [ "$2" == "kill" ]; then
    IFS=";"
    i=1
    for arg in $1; do
    #echo "type${i}: $arg"
        last_var=$(echo $arg | awk -F":" '{print $NF}')
        echo $last_var
        rel=$(lsof -i:$last_var)
        if [ -z "$rel" ]; then
            echo "the "$arg" is dead"
            continue
        else
            echo $arg
            if [ "$3" == "tso" ];then
                lsof -i:$(curl -s http://$arg/pd/api/v1/members|grep \\\"leader\\\": -A 5|grep http|awk -F '[:|/]' '{len=length($5);print substr($5,0,len-1)}'|head -n1)|grep "LISTEN"|awk -F ' ' '{print $2}'|xargs  kill -9
                break
            else
                LPID=$(lsof -i:$(curl -s http://$arg/pd/api/v1/members|grep \\\"leader\\\": -A 5|grep http|awk -F '[:|/]' '{len=length($5);print substr($5,0,len-1)}'|head -n1)|grep "LISTEN"|awk -F ' ' '{print $2}')
                ps aux|grep pd-server|grep -v grep|grep -v ${LPID}|awk -F ' ' '{print $2}'|awk 'BEGIN{srand();}{line[NR]=$0;}END{print line[randint(NR)]}function randint(n){return int(n*rand()+1);}'|xargs kill -9 
            fi
        fi
        #lsof -i:$(curl -s http://11.158.168.215:5010/pd/api/v1/members|grep \\\"leader\\\": -A 5|grep http|awk -F '[:|/]' '{len=length($5);print substr($5,0,len-1)}'|head -n1)|grep "LISTEN"|awk -F ' ' '{print $2}'|xargs  kill -9
    done
elif [ "$2" == "create" ];then 
    IFS=";"
    i=1
    index=1
    for arg in $1; do
        last_var=$(echo $arg | awk -F":" '{print $NF}')
        echo $arg
        rel=$(lsof -i:$last_var)
        if [ -z "$rel" ]; then
            echo "the "$arg" is dead"
            cd  playground/p${index} && bash run.sh restart
            break
            #lsof -i:$(curl -s http://$arg/pd/api/v1/members|grep \\\"leader\\\": -A 5|grep http|awk -F '[:|/]' '{len=length($5);print substr($5,0,len-1)}'|head -n1)|grep "LISTEN"|awk -F ' ' '{print $2}'|xargs  kill -9
        fi
        index=$((index+1))
        #lsof -i:$(curl -s http://11.158.168.215:5010/pd/api/v1/members|grep \\\"leader\\\": -A 5|grep http|awk -F '[:|/]' '{len=length($5);print substr($5,0,len-1)}'|head -n1)|grep "LISTEN"|awk -F ' ' '{print $2}'|xargs  kill -9
    done
fi      

