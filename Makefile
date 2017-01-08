default: build

deps:
	go get -v github.com/Masterminds/glide
	glide install

build: deps 
	glide install
	CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o /app/nats-pipe ./nats-pipe.go

test: deps
	@glide novendor|xargs go test -v

build-docker: build
	docker build -t pipesandfilters/nats-pipe -f Dockerfile .
