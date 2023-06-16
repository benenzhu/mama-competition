






















#!/bin/bash
##!/bin/bash
#
while true; do 
  if [ -f /root/a ]; then
    rm ~/a
    
    clear
    echo "start"
    ETCDCTL_API=3 etcdctl del "" --prefix --endpoints=etcd:2379


    CHECK=ECKSUM make -j 

    touch ~/b
    NODE_ID=1 NODE_NUM=2 catchsegv ./greeter_server


    echo "waiting for new command....................."



  fi
  sleep 0.5
done



