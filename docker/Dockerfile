FROM python:3.8.0-buster

RUN useradd -ms /bin/bash chronicle-user
RUN apt-get update && apt-get upgrade -y && apt-get dist-upgrade -y
RUN apt-get install -y python3-pip
USER chronicle-user
WORKDIR /home/chronicle-user/
RUN mkdir -p chronicle-project
WORKDIR /home/chronicle-user/chronicle-project
COPY chronicle chronicle
COPY setup.py .
COPY install install
RUN ls -al
RUN pip install --user -e .[redis]

ENTRYPOINT ["chronicle"]

