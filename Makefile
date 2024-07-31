.PHONY: generate
generate:
	protoc --go_out=. --go-grpc_out=. api/gatekeeper.proto