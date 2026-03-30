package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()

	db, err := flightsql.NewClient("localhost:32010", nil, nil, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	queries := []string{
		"SELECT word, corpus FROM `bigquery-public-data`.samples.shakespeare WHERE word_count = 1 LIMIT 10",
	}

	for i, query := range queries {
		log.Printf("=== Query %d: %s ===\n", i+1, query)
		start := time.Now()

		info, err := db.Execute(ctx, query)
		if err != nil {
			log.Printf("Execute failed: %v", err)
			continue
		}

		log.Printf("Got FlightInfo, fetching data... time: %v\n", time.Since(start))

		printResult(ctx, db, info)

		log.Printf("=== Query %d complete ===\n\n", i+1)
	}
}

func printResult(ctx context.Context, db *flightsql.Client, info *flight.FlightInfo) {
	if len(info.Endpoint) == 0 {
		log.Println("No endpoints returned")
		return
	}

	ticket := info.Endpoint[0].Ticket
	rdr, err := db.DoGet(ctx, ticket)
	if err != nil {
		log.Printf("DoGet failed: %v", err)
		return
	}
	defer rdr.Release()

	schema := rdr.Schema()
	fmt.Printf("Schema: %s\n\n", schema.String())

	rowCount := int64(0)
	for rdr.Next() {
		chunk := rdr.Chunk()
		data := chunk.Data
		rows := int(data.NumRows())
		rowCount += int64(rows)

		for i := 0; i < rows; i++ {
			fmt.Printf("Row %d: ", rowCount-int64(rows)+int64(i)+1)
			for j := 0; j < int(data.NumCols()); j++ {
				col := data.Column(j)
				if j > 0 {
					fmt.Printf(", ")
				}
				fmt.Printf("%s=%s", schema.Field(j).Name, getValue(col, i))
			}
			fmt.Println()
		}
		data.Release()
	}
	if err := rdr.Err(); err != nil {
		log.Printf("Reader error: %v", err)
	}
	fmt.Printf("\nTotal rows: %d\n", rowCount)
}

func getValue(col arrow.Array, row int) string {
	if col.IsNull(row) {
		return "NULL"
	}

	switch col := col.(type) {
	case *array.Int8:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Int16:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Int32:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Int64:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Uint8:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Uint16:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Uint32:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Uint64:
		return fmt.Sprintf("%d", col.Value(row))
	case *array.Float32:
		return fmt.Sprintf("%f", col.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%f", col.Value(row))
	case *array.String:
		return col.Value(row)
	case *array.LargeString:
		return col.Value(row)
	case *array.Timestamp:
		return col.Value(row).ToTime(arrow.Nanosecond).String()
	default:
		return fmt.Sprintf("[%T]", col)
	}
}
