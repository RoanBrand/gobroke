build:
	go build -race cmd/gobroke/gobroke.go

test:
	go test ./... -count=1 -timeout 15s -race -v

bench:
	go test -count=4 -run=^a -bench=. ./internal/tests
