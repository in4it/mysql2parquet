BINARY1 = mysql2parquet
GOARCH = amd64

all: build-linux build-darwin

build-linux:
	CGO_ENABLED=0 GOOS=linux GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY1}-linux-${GOARCH} cmd/${BINARY1}/main.go 

build-darwin:
	CGO_ENABLED=0 GOOS=darwin GOARCH=${GOARCH} go build -a -installsuffix cgo ${LDFLAGS} -o ${BINARY1}-darwin-${GOARCH} cmd/${BINARY1}/main.go 

clean:
	rm -f ${BINARY}-linux-${GOARCH}

