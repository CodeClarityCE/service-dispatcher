# syntax=docker/dockerfile:1

ARG KIND=plugins
ARG USERNAME=codeclarity
ARG PLUGINNAME=plugin

# DEV IMAGE
FROM golang:1.26.0-alpine AS plugin-dev
ARG KIND
ARG PLUGINNAME
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
ADD .cloud/docker/config/* /root
RUN apk add git
RUN go install github.com/air-verse/air@v1.62.0
CMD ["air", "-c", ".air.toml"]

# DEBUG IMAGE
FROM golang:1.26.0-alpine AS plugin-debug
ARG KIND
ARG PLUGINNAME
ENV KIND=${KIND}
ENV PLUGINNAME=${PLUGINNAME}
RUN apk add git
RUN go install github.com/go-delve/delve/cmd/dlv@latest
ADD .cloud/docker/config/* /root
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
CMD ["/go/bin/dlv", "debug", ".", "--headless", "--listen=:40000", "--accept-multiclient"]
# CMD tail -f /dev/null


# BUILD IMAGE
FROM golang:1.26.0-alpine AS plugin-build
ARG KIND
ARG PLUGINNAME
ARG DIRNAME=
COPY . /codeclarity_src
RUN DIR="${DIRNAME:-${PLUGINNAME}}" && \
    if [ -d "/codeclarity_src/${KIND}/$DIR" ]; then \
      mv /codeclarity_src /codeclarity; \
      if [ "$DIR" != "${PLUGINNAME}" ]; then \
        ln -s "$DIR" "/codeclarity/${KIND}/${PLUGINNAME}"; \
      fi; \
    else \
      mkdir -p /codeclarity/${KIND} && \
      mv /codeclarity_src /codeclarity/${KIND}/${PLUGINNAME} && \
      rm -f /codeclarity/${KIND}/${PLUGINNAME}/go.work; \
    fi
WORKDIR /codeclarity/${KIND}/${PLUGINNAME}
RUN apk update && apk upgrade --no-cache
RUN apk add git
RUN [ "go", "build", "-o", "plugin" ]

# PRODUCTION IMAGE
FROM alpine:3.23.3 AS plugin
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
