build:
	go build cmd/gobroke/gobroke.go

test:
	go test ./... -timeout 30s -race -v
