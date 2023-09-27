FROM ubuntu:latest
LABEL authors="flaviassantos"

ENTRYPOINT ["top", "-b"]