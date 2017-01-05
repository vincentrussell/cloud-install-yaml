#!/bin/bash

{{cloud-install-dir}}/hadoop/sbin/hadoop-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop --script hdfs start namenode
{{cloud-install-dir}}/hadoop/sbin/hadoop-daemons.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop --script hdfs start datanode
{{cloud-install-dir}}/hadoop/sbin/start-dfs.sh
{{cloud-install-dir}}/hadoop/sbin/yarn-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop start resourcemanager
{{cloud-install-dir}}/hadoop/sbin/yarn-daemons.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop start nodemanager
{{cloud-install-dir}}/hadoop/sbin/yarn-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop start proxyserver
{{cloud-install-dir}}/hadoop/sbin/start-yarn.sh
{{cloud-install-dir}}/hadoop/sbin/mr-jobhistory-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop start historyserver