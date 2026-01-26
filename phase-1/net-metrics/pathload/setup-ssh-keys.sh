#!/bin/bash
# Script to add SSH public key to nodes 3 and 4
# This needs to be run on each node, OR you can manually add the key

PUBLIC_KEY="ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIFZFaYc2Kge4id61Vrlqu8wPwKe5276kgKtEVdx0WZ8W mohammadali.khodabandelou@gmail.com"

echo "Adding SSH public key to authorized_keys..."
mkdir -p ~/.ssh
chmod 700 ~/.ssh

if ! grep -q "$PUBLIC_KEY" ~/.ssh/authorized_keys 2>/dev/null; then
    echo "$PUBLIC_KEY" >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    echo "SSH key added successfully!"
else
    echo "SSH key already exists in authorized_keys"
fi
