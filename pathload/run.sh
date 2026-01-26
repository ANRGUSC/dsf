#!/usr/bin/env bash
set -e
sudo docker build -t pathload-d:latest .
sudo docker save -o pathload-d.tar pathload-d:latest
sudo chmod a+r /home/anrg/k3s-test/pathload/pathload-d.tar
ansible-playbook -i inventory.ini send-daemonsets.yml
kubectl delete daemonset pathload-receiver --ignore-not-found
kubectl delete daemonset pathload-sender --ignore-not-found
kubectl apply -f pathload-sender-daemonset.yml
kubectl apply -f pathload-receiver-daemonset.yml