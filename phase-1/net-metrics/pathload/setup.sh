#!/bin/bash
set -e

# Create directories
mkdir -p sender receiver

# Clone pathload to receiver folder
cd receiver
if [ ! -d ".git" ]; then
    git clone https://github.com/jean2/pathload.git .
fi
cd ..

# Clone pathload to sender folder  
cd sender
if [ ! -d ".git" ]; then
    git clone https://github.com/jean2/pathload.git .
fi
cd ..

echo "Setup complete. Pathload code cloned to sender/ and receiver/ folders."
