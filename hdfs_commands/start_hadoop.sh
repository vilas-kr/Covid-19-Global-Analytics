#!/bin/bash

echo "Starting Hadoop Services...."

# ---- Start Hadoop Service -----
# 1. Start Hadoop Service 
start-dfs.sh > /dev/null
if [ $? -eq 0 ]; then 
    echo "Hadoop File System started."

    # 2. Start YARN service
    start-yarn.sh > /dev/null
    if [ $? -eq 0 ]; then
        echo "Hadoop YARN service started."
    else
        echo "Hadoop YARN failed"
        exit 1
    fi

else
    echo "Hadoop File System failed"
    exit 1
fi

# 2. Verify 
echo "Verifying services using jps..."
jps

echo "For Web UI open following in browser"
echo "Web UI:"
echo "NameNode -> http://localhost:9870"
echo "YARN     -> http://localhost:8088"