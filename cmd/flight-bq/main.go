package main

import (
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/TFMV/flight-bq/internal/observability"
	"github.com/TFMV/flight-bq/internal/server"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":32010")
	if err != nil {
		log.Fatal(err)
	}

	grpcServer := grpc.NewServer()

	var drv drivermgr.Driver
	logger := observability.StdLogger{}
	config := server.DefaultConfig()
	config.BigQuery.ProjectID = "tfmv-371720"
	config.BigQuery.AuthType = server.AuthTypeDefault

	svc, err := server.NewFlightSQLServerWithConfig(
		&drv,
		nil,
		config,
		logger,
		observability.NopMetrics{},
	)
	if err != nil {
		log.Fatal(err)
	}

	svc.Start()

	flightServer := flightsql.NewFlightServer(svc)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	// Graceful shutdown on SIGINT/SIGTERM.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("Received signal %v, shutting down...", sig)
		grpcServer.GracefulStop()
		if err := svc.Shutdown(); err != nil {
			log.Printf("Shutdown error: %v", err)
		}
	}()

	log.Println("FlightSQL server listening on :32010")
	log.Fatal(grpcServer.Serve(lis))
}
