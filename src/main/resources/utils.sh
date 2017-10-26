#!/bin/bash

#==================config================================
#kafka config
partition_num=5
topic='Logs'
kafka_home='/soc/kafka/'
broker_list='10.2.4.12:9092,10.2.4.13:9092,10.2.4.14:9092'
#zookeeper config
zookeeper='10.2.4.12:2181,10.2.4.13:2181,10.2.4.14:2181'
zookeeper_home='/soc/zookeeper/'
#storm config
storm_home='/soc/storm/'
topology_name='soc'
#redis config
redis_home='/soc/redis/'
#es config
esnod='10.2.4.15:9200'
#==================config================================

RED_COLOR='\E[1;31m'
GREEN_COLOR='\E[1;32m'
RES='\E[0m'

prints()
{
  echo -e  "${RED_COLOR}= = = = = = = = = = result start = = = = = = = = = = ${RES}"
  echo ""
  echo ""
}

printe()
{
  echo ""
  echo ""
  echo -e  "${RED_COLOR}= = = = = = = = = = result end = = = = = = = = = = = ${RES}"
}

loginprint()
{
echo -e "${RED_COLOR}////////////////////////////////////////////////////////////////${RES}"
echo -e "${RED_COLOR}//                          _ooOoo_                           //${RES}"
echo -e "${RED_COLOR}//                         o8888888o                          //${RES}"
echo -e "${RED_COLOR}//                         888888888                          //${RES}"
echo -e "${RED_COLOR}//                         (| ^_^ |)                          //${RES}"
echo -e "${RED_COLOR}//                         O\  =  /O                          //${RES}"
echo -e "${RED_COLOR}////////////////////////////////////////////////////////////////${RES}"

}


welcome()
{
  loginprint
  main 0
}

main()
{
 if [[ $1 == 0 ]]; then
    start
 fi
}

redis()
{
echo "Please Select Redis Options:"
}


elastic()
{
echo -e "${GREEN_COLOR}Please Select ElasticSearch Options:
0.CurrentJavaProcess
1.Operation.StartElasticServer
2.Show.EsClusterInfo
3.Show.EsPendingTasks
4.Show.InputTimeData
r.Return
q.Quit${RES}"
read -p "Enter selection is ===>" num
if [[ $num =~ ^[0-4]$ || $num='r' || $num='q' ]]; then

  if [[ $num == 0 ]]; then
    prints
    jps
    printe
    elastic
  fi

   if [[ $num == 1 ]]; then
    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
	      prints;
	      plugins_start 8;
	      sleep 10s;
	      printe;
	      break;;
        No ) elastic;;
    esac
    done
    elastic
  fi

  if [[ $num == '2' ]]; then
    prints
    curl -XGET 'http://'$esnod'/_cluster/health?pretty'
    printe
    elastic
  fi


  if [[ $num == '3' ]]; then
    prints
    curl -XGET 'http://'$esnod'/_cluster/pending_tasks?pretty=true'
    printe
    elastic
  fi

  if [[ $num == '4' ]]; then
    read -p "Enter search date formate is [0000-00-00 or 000000]===>" dt
    seconds=`date -d "$dt" +%s`000
    echo "After Dataformate is" $seconds
    read -p "Enter es search index===>" ix
    prints
    curl -XGET 'http://'$esnod'/'$ix'/t_event/_search' -d ' { "query" : { "range" : { "time" : { "gt" : "'$seconds'" } } } } '
    printe
    elastic
  fi


  if [[ $num == 'r' ]]; then
    main 0
  fi

  if [[ $num == 'q' ]]; then
    exit
  fi

fi
}

storm()
{
echo -e "${GREEN_COLOR}Please Select Zookeeper Options:
0.CurrentJavaProcess
1.Operation.StartNimbusAndUi
2.Operation.StartSupervisor
3.Operation.DeActiveTopologySpout
4.Operation.ActiveTopologySpout
5.Operation.KillTopology
6.Show.CurrentStormVersion
7.Show.CurrentStormList
r.Return
q.Quit${RES}"
read -p "Enter selection is ===>" num
if [[ $num =~ ^[0-7]$ || $num='r' || $num='q' ]]; then

 if [[ $num == 0 ]]; then
    prints
    jps
    printe
    storm
  fi

 if [[ $num == 1 ]]; then
    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
	      prints;
	      plugins_start 16;
	      sleep 10s;
	      printe;
	      break;;
        No ) storm;;
    esac
    done
    storm
 fi

 if [[ $num == 2 ]]; then
    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
             prints;
             plugins_start 32;
             sleep 15s;
             printe;
             break;;
        No ) storm;;
    esac
    done
    storm
 fi

 if [[ $num == 3 ]]; then

    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
            prints;
            $storm_home/bin/storm deactivate $topology_name;
            sleep 3s;
            printe;
	    break;;
	No ) storm;;
    esac
    done
    storm
 fi

 if [[ $num == 4 ]]; then

    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
	    prints;
            $storm_home/bin/storm activate $topology_name;
            sleep 3s;
            printe;
	    break;;
	No ) storm;;
    esac
    done
    storm
 fi

 if [[ $num == 5 ]]; then

    echo -e  "${RED_COLOR}Do you want to do this Operation? ${RES}"
    select yn in "Yes" "No"; do
    case $yn in
        Yes )
	    prints;
            $storm_home/bin/storm kill $topology_name -3;
            sleep 3s;
            printe;
	    break;;
        No ) storm;;
    esac
    done
    storm
 fi


 if [[ $num == 6 ]]; then
   prints
   $storm_home/bin/storm version
   sleep 3s
   printe
   storm
 fi

 if [[ $num == 7 ]]; then
   prints
   $storm_home/bin/storm list
   sleep 3s
   printe
   storm
 fi

 if [[ $num == 'r' ]]; then
    main 0
 fi

 if [[ $num == 'q' ]]; then
    exit
 fi

fi
}

kafka()
{
echo -e "${GREEN_COLOR}Please Select Kafka Options:
0.CurrentJavaProcess
1.Show.AllTopics
2.Show.CurrentTopicPartitionAndReplicas
3.Show.CurrentInputPartitionOffSet
4.Show.CurrentOutputPartitionOffSet
5.Show.CurrentTopicProducer.Monitor
6.Show.CurrentTopicConsumer.Monitor
7.Operation.StartZookeeperClient
r.Return
q.Quit${RES}"
read -p "Enter selection is ===>" num
if [[ $num =~ ^[0-6]$ || $num='r' || $num='q' ]]; then

 if [[ $num == 'r' ]]; then
    main 0
 fi

 if [[ $num == 'q' ]]; then
    exit
 fi

  if [[ $num == 0 ]]; then
    prints
    jps
    printe
    kafka
  fi

 if [[ $num == 1 ]]; then
   prints
   $kafka_home/bin/kafka-topics.sh --list --zookeeper $zookeeper
   printe
   kafka
 fi

 if [[ $num == 2 ]]; then
   prints
   $kafka_home/bin/kafka-topics.sh --describe --zookeeper $zookeeper --topic $topic
   printe
   kafka
 fi

 if [[ $num == 3 ]]; then
   prints
   $kafka_home/bin/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list $broker_list --topic $topic --time -1
   printe
   kafka
 fi

 if [[ $num == 6 ]]; then
   prints
   $kafka_home/bin/kafka-console-consumer.sh --zookeeper $zookeeper --topic $topic
   printe
   kafka
 fi

 if [[ $num == 7 ]]; then
   prints
   $zookeeper_home/bin/zkCli.sh -server localhost:2181
   printe
   kafka
 fi

 if [[ $num == 5 ]]; then
   prints
   /kafka/bin/kafka-console-producer.sh --broker-list $broker_list --topic $topic
   printe
   kafka
 fi

  if [[ $num == 4 ]]; then
   prints
   echo ""
   $zookeeper_home/bin/zkCli.sh -server localhost:2181 get /storm/data/Logs/logFramework/partition_0
   printe
   kafka
 fi

fi
}

flume()
{
echo "Please Select Flume Options:"
}

zookeeper()
{
echo -e "${GREEN_COLOR}Please Select Zookeeper Options:
0.CurrentJavaProcess
1.Operation.ConnectZookeeperClient
2.Operation.StartZookeeperServer
r.Return
q.Quit{RES}"
read -p "Enter selection is ===>" num
if [[ $num =~ ^[0-2]$ || $num='r' || $num='q' ]]; then


  if [[ $num == 0 ]]; then
    prints
    jps
    printe
    zookeeper
  fi

 if [[ $num == 1 ]]; then
   prints
   $zookeeper_home/bin/zkCli.sh -server localhost:2181
   printe
   zookeeper
 fi

  if [[ $num == 2 ]]; then
   prints
   plugins_start 1
   sleep 3s
   printe
   zookeeper
 fi

 if [[ $num == 'r' ]]; then
    main 0
 fi

 if [[ $num == 'q' ]]; then
    exit
 fi

fi
}

start()
{
echo -e "${GREEN_COLOR}Please Select Plugins:
0.CurrentJavaProcess
1.Redis
2.Storm
3.Kafka
4.Zookerper
5.ElasticSearch
6.Flume
q.Quit${RES}"
read -p "Enter selection is ===>" num
if [[ $num =~ ^[0-6]$ ]]; then

  if [[ $num == 'q' ]]; then
    exit
  fi

  if [[ $num == 0 ]]; then
    prints;
    jps;
    printe;
    main 0
  fi

  if [[ $num == 1 ]]; then
    echo ""
    exit;
  fi

  if [[ $num == 2 ]]; then
    storm
  fi

  if [[ $num == 3 ]]; then
    kafka
  fi

  if [[ $num == 4 ]]; then
    zookeeper
  fi

  if [[ $num == 5 ]]; then
    elastic
  fi

  if [[ $num == 6 ]]; then
    flume
  fi
fi

if [[ $num == 'q' ]]; then
    exit
fi
echo -e  "${RED_COLOR}Error input Invalid entry Please Enter right selection to be continued ! ${RES}"
start

}

plugins_start()
{
operation=$1
if [ $(($operation & 1)) -ne 0 ] ; then
	# zookeeper start
	echo 'zookeeper is starting...'
	$zookeeper_home/bin/zkServer.sh start &
	if [ $? -eq 0 ];then
		echo 'zookeeper start finish'
	fi
fi

if [ $(($operation & 2)) -ne 0 ] ; then
	# kafka start
	echo 'kafka is starting...'
	$kafka_home/bin/kafka-server-start.sh $kafka_home/config/server.properties &
	if [ $? -eq 0 ];then
		echo 'kafka start finish'
	fi
fi

if [ $(($operation & 4)) -ne 0 ] ; then
	# redis start
	$redis_home/redis-server $redis_home/redis.conf &
	if [ $? -eq 0 ];then
		echo 'redis start finish'
	fi
fi

if [ $(($operation & 8)) -ne 0 ] ; then
	# elasticsearch start run as elsearch
	su elsearch -c "/soc/elasticsearch/bin/elasticsearch &"
	if [ $? -eq 0 ];then
		echo 'elasticsearch start finish'
	fi
fi

if [ $(($operation & 16)) -ne 0 ] ; then
	echo 'storm is starting...'
	# nimbus start
	$storm_home/bin/storm nimbus &
	if [ $? -eq 0 ];then
		# ui start
		$storm_home/bin/storm ui &
	fi
fi

if [ $(($operation & 32)) -ne 0 ] ; then
	# supervisor start
	$storm_home/bin/storm supervisor &

	# logviewer start
	$storm_home/bin/storm logviewer &
fi
}


welcome