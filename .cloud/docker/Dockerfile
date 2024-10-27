# syntax=docker/dockerfile:1

ARG KIND=plugins
ARG USERNAME=codeclarity
ARG PLUGINNAME=plugin

# DEV IMAGE
FROM golang:1.23.0-alpine AS plugin-dev
ARG KIND
ARG PLUGINNAME
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
ADD .cloud/docker/config/.netrc /root/.netrc
RUN apk add git
RUN go install github.com/air-verse/air@latest
CMD air -c .air.toml

# DEBUG IMAGE
FROM golang:1.23.0-alpine AS plugin-debug
ARG KIND
ARG PLUGINNAME
ENV KIND ${KIND}
ENV PLUGINNAME ${PLUGINNAME}
RUN apk add git
RUN go install github.com/go-delve/delve/cmd/dlv@latest
ADD .cloud/docker/config/.netrc /root/.netrc
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
CMD /go/bin/dlv debug . --headless --listen=:40000 --accept-multiclient
# CMD tail -f /dev/null


# BUILD IMAGE
FROM golang:1.23.0-alpine AS plugin-build
ARG KIND
ARG PLUGINNAME
COPY . /codeclarity/${KIND}/${PLUGINNAME}
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
ADD .cloud/docker/config/.netrc /root/.netrc
RUN rm go.work
RUN apk update && apk upgrade --no-cache
RUN apk add git
RUN [ "go", "build", "-o", "plugin" ]

# PRODUCTION IMAGE
FROM alpine:3.20.2 AS plugin
ARG KIND
ARG USERNAME
ARG PLUGINNAME
RUN adduser -D ${USERNAME}
USER ${USERNAME}
WORKDIR /home/${USERNAME}/${PLUGINNAME}
COPY --from=plugin-build --chown=codeclarity:codeclarity /codeclarity/${KIND}/${PLUGINNAME}/plugin .
COPY --from=plugin-build --chown=codeclarity:codeclarity /codeclarity/${KIND}/${PLUGINNAME}/config.json .

RUN mkdir /home/${USERNAME}/private

ENTRYPOINT [ "./plugin" ]
