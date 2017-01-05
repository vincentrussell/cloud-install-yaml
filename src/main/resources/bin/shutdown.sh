#!/bin/bash

{{cloud-install-dir}}/hadoop/sbin/hadoop-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop --script hdfs stop namenode
{{cloud-install-dir}}/hadoop/sbin/hadoop-daemons.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop --script hdfs stop datanode
{{cloud-install-dir}}/hadoop/sbin/stop-dfs.sh
{{cloud-install-dir}}/hadoop/sbin/yarn-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop stop resourcemanager
{{cloud-install-dir}}/hadoop/sbin/yarn-daemons.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop stop nodemanager
{{cloud-install-dir}}/hadoop/sbin/stop-yarn.sh
{{cloud-install-dir}}/hadoop/sbin/yarn-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop stop proxyserver
{{cloud-install-dir}}/hadoop/sbin/mr-jobhistory-daemon.sh --config {{cloud-install-dir}}/hadoop/etc/hadoop stop historyserver