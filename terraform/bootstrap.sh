#!/bin/bash
set -e

if type sudo 2>/dev/null; then \
     echo "The sudo command already exists... Skipping."; \
    else \
     echo -e "#!/bin/sh\n\${@}" > /usr/sbin/sudo; \
     chmod +x /usr/sbin/sudo; \
    fi

sudo add-apt-repository universe
sudo apt update
sudo apt install -y python3-pip docker.io
pip3 install awscli
sudo curl -L "https://github.com/docker/compose/releases/download/1.23.1/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

set +e
sudo groupadd docker
sudo usermod -aG docker ${USER}
set -e

source ~/.profile

aws configure set region eu-west-1
sudo $(aws ecr get-login --no-include-email)

sudo mkdir -p /var/app
sudo chmod 777 /var/app

