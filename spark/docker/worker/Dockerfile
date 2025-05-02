FROM bde2020/spark-worker:3.0.1-hadoop3.2

USER root

RUN apk add --no-cache \
      py3-pip \
      python3-dev \
      build-base \
      openblas-dev \
      gfortran \
  && pip3 install numpy \
  && apk del build-base python3-dev


