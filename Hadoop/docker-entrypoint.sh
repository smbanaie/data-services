#!/usr/bin/env bash

sudo service ssh start

if [ ! -d $DFS_DIR/name ]; then
        echo "********************************"
        echo "******* First Time Entry *******"
        echo "********************************"        
        $HADOOP_HOME/bin/hdfs namenode -format
fi

$HADOOP_HOME/sbin/start-dfs.sh
echo "********************************"
echo "******* Between starting main services *******"
echo "********************************"     
$HADOOP_HOME/sbin/start-yarn.sh

echo "*******************************************"
echo "******* EntryPoint Sleeping ***************"
echo "*******************************************"

sleep infinity
