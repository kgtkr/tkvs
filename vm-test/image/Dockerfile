FROM ubuntu:22.04

RUN apt update && \
    apt install -y cloud-image-utils

COPY cloud.txt .
RUN cloud-localds --disk-format qcow2 cloud.img cloud.txt
