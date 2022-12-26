
(while true; 
    do cat /proc/440277/fd/1 >> ./logs/3_clean.py.log1;
        sleep 300;
    done) &

fd_pid=$!
echo $fd_pid
# kill $fd_pid
# mine is: 499586