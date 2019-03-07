#!/usr/bin/env bash

if [ -z "$TF_VAR_cluster_size" ]; then
    echo "Cluster size is not specified."
    echo "Please set cluster size via TF_VAR_cluster_size enviroment variable."
    exit 1
fi

echo "Creating infrastructure"
cd terraform
if ! terraform apply; then
    exit 1
fi

echo "Creating inventory"
cd ../ansible
if ! python tf2inventory.py; then
    echo "An error occurred while assembling inventory"
    exit 1
fi

echo "Waiting for 30 seconds for nodes to go online"
sleep 30

echo "Provisioning with Ansible"
export ANSIBLE_HOST_KEY_CHECKING=False
ansible-playbook -i resources/inventory.yml flink_cluster.yml
