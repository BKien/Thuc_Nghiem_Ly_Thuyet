#!/bin/bash
set -e

# Sao chép keyFile từ bind mount sang thư mục nội bộ container để đặt quyền 0400 chuẩn xác
# (Tương thích cả trên Ubuntu 24.04 LTS và Docker Desktop trên Windows/WSL2)
if [ -f /tmp/host-keyfile/mongo-keyfile ]; then
    mkdir -p /etc/mongo-keyfile
    tr -d '\r' < /tmp/host-keyfile/mongo-keyfile > /etc/mongo-keyfile/mongo-keyfile
    chown mongodb:mongodb /etc/mongo-keyfile/mongo-keyfile
    chmod 400 /etc/mongo-keyfile/mongo-keyfile
fi

exec /usr/local/bin/docker-entrypoint.sh "$@"
