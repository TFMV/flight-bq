package main

import (
	"log"
	"net"

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
	svc, err := server.NewFlightSQLServerWithDriver(&drv, map[string]string{
		"driver":                       "bigquery",
		"adbc.bigquery.sql.project_id": "tfmv-371720",
	})
	if err != nil {
		log.Fatal(err)
	}

	flightServer := flightsql.NewFlightServer(svc)
	flight.RegisterFlightServiceServer(grpcServer, flightServer)

	log.Println("FlightSQL server listening on :32010")
	log.Fatal(grpcServer.Serve(lis))
}
