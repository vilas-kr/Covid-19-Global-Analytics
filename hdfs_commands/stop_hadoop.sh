#!/bin/bash

# --- Stop Hadoop Services ---
echo "Stopping Hadoop services..."

# 1. STOP HADOOP YARN SERVICES
stop-yarn.sh > /dev/null

if [ $? -eq 0 ]; then
    echo "Hadoop YARN service stoped."
else
    echo "Hadoop YARN sercice failed to stop."
fi

# 2. STOP HADOOP FILE SYSTEM
stop-dfs.sh > /dev/null

if [ $? -eq 0 ]; then
    echo "Hadoop File service stoped."
else
    echo "Hadoop File service failed to stop"
fi

# 3. VERIFY
echo "Verifying stopped services..."
jps

echo "All Hadoop services stopped..."