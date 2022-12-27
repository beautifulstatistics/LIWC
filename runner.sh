#!/bin/bash

export PYTHONUNBUFFERED="1"

(while true; 
    do sensors | 
       grep -oP '(?<=Core [0-9]:        \+)[0-9]{2}' | 
       awk -v date="$(date)" '$1>82{c++}; END{if (c+0 > 1) print "Cores Over Temp: " c+0};';
       sleep 900;
    done) &

sensors_pid=$!
echo "Temp monitor PID: "$sensors_pid

ls | grep -P "^[8]_" | while read -r line ;
do
    echo -n $line" "

    rm "./logs/"$line".log" 2> /dev/null

    if [ "${line: -3}" == ".py" ]; then
        if time python $line >> "./logs/"$line".log"
        then
            echo
            echo "Finished"
        else
            echo
            echo "Error. Aborting."
            break
        fi
    elif [ "${line: -2}" == ".R" ]; then
        if time Rscript $line >> "./logs/"$line".log"
        then
            echo
            echo "Finished"
        else
            echo
            echo "Error. Aborting."
            break
        fi
    fi
done

echo "Killing temp monitor."
kill $sensors_pid
