build:
	go build cmd/gobroke/gobroke.go

test:
	go test ./... -count=1 -timeout 20s -race -v

bench:
	go test -count=3 -run=^a -bench=. ./internal/tests
