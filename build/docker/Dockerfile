FROM golang:1.15.3-alpine3.12 AS builder
WORKDIR /
COPY . .

RUN apk add --update git
RUN CGO_ENABLED=0 go build -installsuffix cgo -o /gravity-controller ./cmd/gravity-controller/gravity-controller.go

FROM alpine:3.12
COPY --from=builder /gravity-controller /gravity-controller
COPY ./configs .

USER 1001
EXPOSE 40000
CMD ["/gravity-controller"]
