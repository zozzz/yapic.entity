FROM python:3.11-slim-bullseye

RUN apt update \
	&& apt -y install python3-dev python3-dbg pkg-config build-essential gdb

ADD requirements.txt /requirements.txt
RUN python3 -m pip install -r /requirements.txt

ADD requirements-dev.txt /requirements-dev.txt
RUN python3 -m pip install -r /requirements-dev.txt

ENV IN_DOCKER 1
ENV DEBUG 1

ADD . /app
RUN chmod a+x /app/run-memray
WORKDIR /app
