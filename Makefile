build:
	go build cmd/gobroke/gobroke.go

test:
	go test ./... -count=1 -timeout 20s -race -v
