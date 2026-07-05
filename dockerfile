FROM golang:1.25

WORKDIR /app

# copy entire project (including local modules)
COPY . .

# build directly (Go will resolve deps here)
RUN go build ./api 2>&1 || true

CMD ["./app"]