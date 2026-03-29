package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type QueryRequest struct {
	Query string `json:"query"`
}

type SchemaMessage struct {
	Type   string   `json:"type"`
	Fields []string `json:"fields"`
}

func main() {
	port := flag.String("port", "8080", "WebSocket server port")
	query := flag.String("query", "", "SQL query to execute")
	flag.Parse()

	args := flag.Args()
	if len(args) > 0 {
		// First positional arg could be port or query
		if args[0] == "-port" || args[0] == "--port" {
			// Already handled by flag
		} else if _, err := fmt.Sscanf(args[0], "%d", new(int)); err == nil && len(args[0]) > 0 {
			// Looks like a port number
			*port = args[0]
			if len(args) > 1 {
				*query = strings.Join(args[1:], " ")
			}
		} else {
			*query = strings.Join(args, " ")
		}
	}

	if *query == "" {
		*query = "SELECT 1 AS id, 'porter' AS name"
	}

	ctx := context.Background()
	u := url.URL{Scheme: "ws", Host: "localhost:" + *port, Path: "/"}
	log.Printf("connecting to %s", u.String())

	conn, _, err := websocket.Dial(ctx, u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	start := time.Now()
	req := QueryRequest{Query: *query}
	if err := wsjson.Write(ctx, conn, req); err != nil {
		log.Fatal("write:", err)
	}

	log.Printf("Executing: %s", *query)

	var schema *arrow.Schema
	var rowCount int

	for {
		msgType, msg, err := conn.Read(ctx)
		if err != nil {
			if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
				break
			}
			log.Printf("read error: %v", err)
			break
		}

		if msgType == websocket.MessageText {
			var schemaMsg SchemaMessage
			if err := json.Unmarshal(msg, &schemaMsg); err != nil {
				log.Printf("unmarshal error: %v", err)
				continue
			}
			if schemaMsg.Type == "schema" {
				fmt.Printf("Schema: %s\n", strings.Join(schemaMsg.Fields, ", "))
			}
		} else if msgType == websocket.MessageBinary {
			rows, schemaOut, err := decodeIPC(msg, schema)
			if err != nil {
				log.Printf("decode error: %v", err)
				continue
			}
			if schema == nil && schemaOut != nil {
				schema = schemaOut
			}
			rowCount += rows
			if rows > 0 {
				printBatch(msg, schema)
			}
		}
	}

	elapsed := time.Since(start)
	if rowCount > 0 {
		fmt.Printf("\n%d rows returned in %v\n", rowCount, elapsed.Round(time.Millisecond))
	} else {
		fmt.Printf("Query completed in %v\n", elapsed.Round(time.Millisecond))
	}
}

func decodeIPC(data []byte, schema *arrow.Schema) (int, *arrow.Schema, error) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return 0, nil, err
	}
	defer reader.Release()

	outSchema := reader.Schema()

	if !reader.Next() {
		return 0, outSchema, nil
	}

	rec := reader.Record()
	if rec == nil {
		return 0, outSchema, nil
	}
	defer rec.Release()

	return int(rec.NumRows()), outSchema, nil
}

func printBatch(data []byte, schema *arrow.Schema) {
	reader, err := ipc.NewReader(bytes.NewReader(data))
	if err != nil {
		return
	}
	defer reader.Release()

	for reader.Next() {
		rec := reader.Record()
		if rec == nil {
			continue
		}
		defer rec.Release()

		printRecord(rec)
	}
}

func printRecord(rec arrow.Record) {
	ncols := int(rec.NumCols())
	nrows := int(rec.NumRows())

	values := make([][]string, ncols)
	for i := 0; i < ncols; i++ {
		values[i] = make([]string, nrows)
		col := rec.Column(i)
		for row := 0; row < nrows; row++ {
			values[i][row] = formatValue(col, row)
		}
	}

	for row := 0; row < nrows; row++ {
		rowVals := make([]string, ncols)
		for col := 0; col < ncols; col++ {
			rowVals[col] = values[col][row]
		}
		fmt.Println(strings.Join(rowVals, "\t"))
	}
}

func formatValue(col arrow.Array, row int) string {
	if col.IsNull(row) {
		return "NULL"
	}

	switch arr := col.(type) {
	case *array.Boolean:
		return fmt.Sprintf("%t", arr.Value(row))
	case *array.Int8:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int16:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int32:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Int64:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint8:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint16:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint32:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Uint64:
		return fmt.Sprintf("%d", arr.Value(row))
	case *array.Float32:
		return fmt.Sprintf("%g", arr.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%g", arr.Value(row))
	case *array.String:
		return arr.Value(row)
	case *array.Binary:
		return string(arr.Value(row))
	default:
		return fmt.Sprintf("%v", col)
	}
}
