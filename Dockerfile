FROM golang:1 AS builder

COPY . /work
WORKDIR /work
RUN useradd pressurecooker
RUN cd /work ; go build -o kubernetes-pressurecooker cmd/main.go

FROM scratch

LABEL MAINTAINER="Rene Treffer <treffer+github@measite.de>"
COPY --from=builder /work/kubernetes-pressurecooker /usr/bin/kubernetes-pressurecooker
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/

USER pressurecooker

ENTRYPOINT ["/usr/bin/kubernetes-pressurecooker", "-logtostderr"]
