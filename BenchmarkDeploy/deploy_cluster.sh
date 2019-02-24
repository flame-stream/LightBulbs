#!/usr/bin/env bash

echo "Creating infrastructure"
if ! terraform apply; then
    exit 1
fi

echo "Creating inventory"
python tf2inventory.py
echo "Waiting for 60 seconds for nodes to go online"
sleep 60

echo "Provisioning with Ansible"
export ANSIBLE_HOST_KEY_CHECKING=False
ansible-playbook -i resources/inventory.yml flink_cluster.yml