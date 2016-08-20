TICK=$1
kapacitor -url http://localhost:9092 delete tasks $TICK
kapacitor -url http://localhost:9092 define -name $TICK -type stream -dbrp kapacitor.default -tick /etc/kapacitor/ticks/$TICK.tick
kapacitor -url http://localhost:9092 enable tasks $TICK
