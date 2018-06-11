#!/bin/sh

# końcówka '-pl <nazwa-modułu>' pozwala z katalogu głównego projektu zawołać zadany moduł
run_spark='BROKERS="localhost:9092" TOPICS="my-source" mvn install exec:java -pl spark-processor'
run_source='mvn spring-boot:run -pl source'
run_sink='mvn spring-boot:run -pl sink'
run_flink='mvn spring-boot:run -pl flink-processor'
run_kafkastreams='mvn spring-boot:run -pl kafkastreamsprocessor'
run_websocket='mvn spring-boot:run -pl monitor'

print_help(){
    echo "$0 (start|stop) (source|sink|spark|flink|kafkastreams|websocket)"
}

if [ "$#" -lt 2 ]; then
    print_help
    exit 1
fi

run_command(){
    if [ -f /tmp/"$1" ]; then
        if ps -p $(cat /tmp/"$1") > /dev/null 
        then
            echo "Program już jest uruchomiony. Odpalam kolejną instancę"
        fi # else process was terminated some other way and we didn't notice
    fi
    sh -c "$1" &
    echo $! >> /tmp/"$1" # save PID
}

kill_command(){
    if [ ! -f /tmp/"$1" ]; then
        echo "Proces już nie żyje"
        exit 7
    fi
    while read pid; do pkill -P $pid; done </tmp/"$1" # that file contains PIDs
    rm /tmp/"$1"
}

get_command(){
    if [ "$1" = 'source' ]; then
        cmd="$run_source"
    elif [ "$1" = 'sink' ]; then
        cmd="$run_sink"
    elif [ "$1" = 'spark' ]; then
        cmd="$run_spark"
    elif [ "$1" = 'flink' ]; then
        cmd="$run_flink"
    elif [ "$1" = 'kafkastreams' ]; then
        cmd="$run_kafkastreams"
    elif [ "$1" = 'websocket' ]; then
        cmd="$run_websocket"
    else
        print_help
        exit 3
    fi
}

get_command "$2"

if [ "$1" = 'start' ]; then
    run_command "$cmd"
elif [ "$1" = 'stop' ]; then
    kill_command "$cmd"
else
    print_help
    exit 4
fi

