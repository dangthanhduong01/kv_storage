package server

import (
	"kvstorage/config"
	"kvstorage/gen-go/kv"
	kvhandler "kvstorage/handler"
	"log"
	"net"

	"github.com/apache/thrift/lib/go/thrift"
)

func main() {
	config, err := config.LoadConfig()
	if err != nil || config.DNS == "" {
		log.Fatalf("Error loading config: %v", err)
	}
	dns := config.DNS

	handler := kvhandler.NewHandler(dns)
	processor := kv.NewStorageServiceProcessor(handler)
	transport, err := thrift.NewTServerSocket(net.JoinHostPort(config.Host, config.Port))
	if err != nil {
		log.Fatalf("Error creating server socket: %v", err)
	}
	transportFactory := thrift.NewTBufferedTransportFactory(8192)
	protocolFactory := thrift.NewTBinaryProtocolFactoryDefault()

	server := thrift.NewTSimpleServer4(processor, transport, transportFactory, protocolFactory)

	log.Printf("Starting the server on %s:%s...\n", config.Host, config.Port)
	if err := server.Serve(); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}
