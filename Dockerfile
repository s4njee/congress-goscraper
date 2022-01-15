FROM alpine:latest
RUN apk add go py3-pip alpine-sdk libxml2-dev libxslt-dev py3-lxml
RUN mkdir /app
WORKDIR /app

COPY requirements.txt ./
RUN pip3 install -r requirements.txt
COPY go.mod ./
COPY go.sum ./
RUN go mod download

COPY *.go ./

RUN go build -o /scraper
CMD ["/scraper"]
