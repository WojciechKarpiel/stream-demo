żeby odpalić ten program trzeba najpierw uruchomić zookepera i kafkę
https://kafka.apache.org/11/documentation/streams/quickstart (pierwsze dwa kroki z tąd)

Ewentualnie można postawić kafkę + zookeper za pomocą docker-compose:
```bash
git clone git@github.com:wurstmeister/kafka-docker.git

cd kafka-docker

docker-compose --file docker-compose-single-broker.yml  up -d
```

potem trzeba odpalić źródło, zlew i wybrany procesor. Można się posłużyć skryptem `manager.sh`

```sh
./manager.sh start source
./manager.sh start sink
./manager.sh start spark
#...
./manager.sh stop spark
./manager.sh start flink
#...
./mangager.sh stop flink
./mangager.sh stop sink
./mangager.sh stop source
```

Graf jest wzięty z
https://github.com/perrygeo/pi_sensor_realtime
żeby odpalić potrzeba websocketd http://websocketd.com/
Po odpaleniu źródła, zlewu i procesora można zrobić
```
$ ./manager.sh start monitor
...
$ ./manager.sh stop monitor
```
Dashboard jest pod `localhost:8083/monitor`

