# Keep the complete Go toolchain in the evaluation image.
FROM golang:1.22

WORKDIR /app

# This project uses only the Go standard library and therefore has no go.sum.
COPY go.mod ./
COPY . .

# Pre-populate the build cache and prove the source compiles in the image.
RUN go build ./...

CMD ["bash"]
