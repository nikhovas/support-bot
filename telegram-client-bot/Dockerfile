FROM golang:latest AS build
WORKDIR /app
COPY go.mod ./
COPY go.sum ./
RUN go mod download
COPY . .
RUN go build -o /telegram-client-bot
CMD [ "/telegram-client-bot" ]