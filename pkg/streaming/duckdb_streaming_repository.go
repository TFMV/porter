package streaming

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"

	"github.com/TFMV/porter/pkg/infrastructure/pool"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"
	"github.com/rs/zerolog"
)

// duckDBStreamingRepository implements the StreamingRepository interface for DuckDB.
type duckDBStreamingRepository struct {
	pool      pool.ConnectionPool
	allocator memory.Allocator
	logger    zerolog.Logger
}

// NewDuckDBStreamingRepository creates a new DuckDBStreamingRepository.
func NewDuckDBStreamingRepository(p pool.ConnectionPool, alloc memory.Allocator, logger zerolog.Logger) StreamingRepository {
	return &duckDBStreamingRepository{
		pool:      p,
		allocator: alloc,
		logger:    logger,
	}
}

// FlightDataReader is an interface that flight.NewRecordReader expects.
// flight.FlightService_DoPutServer implements this.
type FlightDataReader interface {
	Schema() *arrow.Schema
	Recv() (*flight.FlightData, error)
}

// IngestStream ingests Arrow RecordBatches into DuckDB using the Appender API.
func (r *duckDBStreamingRepository) IngestStream(ctx context.Context, transactionID string, targetTable string, schema *arrow.Schema, reader flight.MessageReader) (int64, error) {
	r.logger.Debug().Str("target_table", targetTable).Str("transaction_id", transactionID).Msg("IngestStream called")

	sqlDB, err := r.pool.Get(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to acquire DB from pool: %w", err)
	}

	sqlConn, err := sqlDB.Conn(ctx)
	if err != nil {
		return 0, fmt.Errorf("failed to get *sql.Conn from *sql.DB: %w", err)
	}
	defer sqlConn.Close()

	safeTableIdentifier := r.quoteIdentifier(targetTable)

	createTableStmt := r.generateCreateTableStmt(safeTableIdentifier, schema)
	if _, err = sqlConn.ExecContext(ctx, createTableStmt); err != nil {
		return 0, fmt.Errorf("failed to create table %s: %w", safeTableIdentifier, err)
	}

	var totalRows int64
	var appender *duckdb.Appender

	rawConnErr := sqlConn.Raw(func(driverConnRaw interface{}) error {
		driverConn, ok := driverConnRaw.(driver.Conn)
		if !ok {
			return fmt.Errorf("expected driverConn to be driver.Conn, got %T", driverConnRaw)
		}
		appender, err = duckdb.NewAppenderFromConn(driverConn, "", safeTableIdentifier)
		if err != nil {
			return fmt.Errorf("failed to create DuckDB appender for table %s: %w", safeTableIdentifier, err)
		}
		return nil
	})

	if rawConnErr != nil {
		return 0, rawConnErr
	}
	if appender == nil {
		return 0, fmt.Errorf("appender is nil after Raw connection setup")
	}

	appenderCloseErr := fmt.Errorf("appender not closed yet")
	defer func() {
		if appender != nil {
			appenderCloseErr = appender.Close()
			if appenderCloseErr != nil {
				r.logger.Error().Err(appenderCloseErr).Str("table", safeTableIdentifier).Msg("Failed to close DuckDB appender")
			}
		}
	}()

	flightDataReaderAdapter, ok := reader.(FlightDataReader)
	if !ok {
		return 0, fmt.Errorf("IngestStream reader is not a FlightDataReader; got %T", reader)
	}

	recordReader, err := flight.NewRecordReader(flightDataReaderAdapter, ipc.WithAllocator(r.allocator), ipc.WithSchema(schema))
	if err != nil {
		return 0, fmt.Errorf("failed to create flight record reader: %w", err)
	}
	defer recordReader.Release()

	for recordReader.Next() {
		rec := recordReader.Record()
		numRows := int(rec.NumRows())
		numCols := int(rec.NumCols())

		for i := 0; i < numRows; i++ {
			rowArgs := make([]driver.Value, numCols)
			for j := 0; j < numCols; j++ {
				col := rec.Column(j)
				if col.IsNull(i) {
					rowArgs[j] = nil
					continue
				}
				switch typedCol := col.(type) {
				case *array.Boolean:
					rowArgs[j] = typedCol.Value(i)
				case *array.Int8:
					rowArgs[j] = typedCol.Value(i)
				case *array.Int16:
					rowArgs[j] = typedCol.Value(i)
				case *array.Int32:
					rowArgs[j] = typedCol.Value(i)
				case *array.Int64:
					rowArgs[j] = typedCol.Value(i)
				case *array.Uint8:
					rowArgs[j] = typedCol.Value(i)
				case *array.Uint16:
					rowArgs[j] = typedCol.Value(i)
				case *array.Uint32:
					rowArgs[j] = typedCol.Value(i)
				case *array.Uint64:
					rowArgs[j] = typedCol.Value(i)
				case *array.Float32:
					rowArgs[j] = typedCol.Value(i)
				case *array.Float64:
					rowArgs[j] = typedCol.Value(i)
				case *array.String:
					rowArgs[j] = typedCol.Value(i)
				case *array.LargeString:
					rowArgs[j] = typedCol.Value(i)
				case *array.Binary:
					rowArgs[j] = typedCol.Value(i)
				case *array.LargeBinary:
					rowArgs[j] = typedCol.Value(i)
				case *array.FixedSizeBinary:
					rowArgs[j] = typedCol.Value(i) // Potentially convert to []byte if driver expects that
				case *array.Date32:
					rowArgs[j] = typedCol.Value(i).ToTime()
				case *array.Date64:
					rowArgs[j] = typedCol.Value(i).ToTime()
				case *array.Timestamp:
					rowArgs[j] = typedCol.Value(i).ToTime(typedCol.DataType().(*arrow.TimestampType).Unit)
				case *array.Time32:
					rowArgs[j] = typedCol.Value(i).ToTime(typedCol.DataType().(*arrow.Time32Type).Unit)
				case *array.Time64:
					rowArgs[j] = typedCol.Value(i).ToTime(typedCol.DataType().(*arrow.Time64Type).Unit)
				case *array.Decimal128:
					// driver.Value might not directly support Decimal128.
					// DuckDB itself handles decimals, but the Go driver might expect string or float.
					// This might require specific handling or string conversion.
					// For now, using ValueStr for simplicity, but this needs verification.
					// Consider converting to string: typedCol.Value(i).String()
					// Or if your driver/DB supports it, find the appropriate Go type.
					rowArgs[j] = typedCol.ValueStr(i) // Placeholder - review for actual DB driver compatibility
				case *array.Decimal256:
					// Similar to Decimal128
					rowArgs[j] = typedCol.ValueStr(i) // Placeholder - review
				// TODO: Add cases for List, Struct, Map, Duration, Interval if they are expected
				// These will require more complex handling (e.g., converting to JSON string or nested slices/maps if supported)
				default:
					// Fallback to ValueStr, but this might not be ideal for all types.
					// Or return an error for unsupported types.
					r.logger.Warn().Str("arrow_type", col.DataType().String()).Msg("Unsupported Arrow type for appender, attempting ValueStr")
					rowArgs[j] = col.ValueStr(i) // This is a fallback, may not be correct for all unhandled types
					// Alternatively, return an error:
					// rec.Release()
					// return totalRows, fmt.Errorf("unsupported arrow column type for appender: %s", col.DataType().Name())
				}
			}
			if err = appender.AppendRow(rowArgs...); err != nil {
				rec.Release() // Release before returning error
				return totalRows, fmt.Errorf("appender failed to append row %d of batch: %w. Appender close status: %v", i, err, appenderCloseErr)
			}
			totalRows++ // Correctly increment after each successful row append
		}
		// totalRows += rec.NumRows() // This was the original logic, assuming AppendArrowRecord.
		// If AppendRow is used, totalRows should be incremented inside the inner loop after a successful AppendRow,
		// or simply use rec.NumRows() after the inner loop if all rows in the batch are processed.
		// For now, keeping it similar to original structure, but this counts the whole batch even if a row fails.
		// Correct would be: totalRows += int64(numRows) if the inner loop completes. Or increment inside.

		rec.Release() // Release the current record after processing all its rows
	}

	if recordReader.Err() != nil {
		return totalRows, fmt.Errorf("error reading records from flight stream: %w. Appender close status: %v", recordReader.Err(), appenderCloseErr)
	}

	if errFlush := appender.Flush(); errFlush != nil {
		return totalRows, fmt.Errorf("failed to flush DuckDB appender for table %s: %w. Appender close status: %v", safeTableIdentifier, errFlush, appenderCloseErr)
	}

	if appenderCloseErr != nil && appenderCloseErr.Error() != "appender not closed yet" {
		return totalRows, appenderCloseErr
	}

	r.logger.Info().Str("target_table", safeTableIdentifier).Int64("rows_affected", totalRows).Msg("Successfully ingested stream using Appender")
	return totalRows, nil
}

func (r *duckDBStreamingRepository) quoteIdentifier(name string) string {
	return fmt.Sprintf("\"%s\"", strings.ReplaceAll(name, "\"", "\"\""))
}

func (r *duckDBStreamingRepository) generateCreateTableStmt(tableName string, schema *arrow.Schema) string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (", tableName))
	for i, field := range schema.Fields() {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(r.quoteIdentifier(field.Name))
		sb.WriteString(" ")
		sb.WriteString(r.arrowToDuckDBType(field.Type))
		if !field.Nullable {
			sb.WriteString(" NOT NULL")
		}
	}
	sb.WriteString(")")
	return sb.String()
}

func (r *duckDBStreamingRepository) arrowToDuckDBType(dt arrow.DataType) string {
	switch dt := dt.(type) {
	case *arrow.NullType:
		return "NULL"
	case *arrow.BooleanType:
		return "BOOLEAN"
	case *arrow.Int8Type:
		return "TINYINT"
	case *arrow.Int16Type:
		return "SMALLINT"
	case *arrow.Int32Type:
		return "INTEGER"
	case *arrow.Int64Type:
		return "BIGINT"
	case *arrow.Uint8Type:
		return "UTINYINT"
	case *arrow.Uint16Type:
		return "USMALLINT"
	case *arrow.Uint32Type:
		return "UINTEGER"
	case *arrow.Uint64Type:
		return "UBIGINT"
	case *arrow.Float16Type:
		return "FLOAT"
	case *arrow.Float32Type:
		return "FLOAT"
	case *arrow.Float64Type:
		return "DOUBLE"
	case *arrow.StringType:
		return "VARCHAR"
	case *arrow.LargeStringType:
		return "VARCHAR"
	case *arrow.BinaryType:
		return "BLOB"
	case *arrow.LargeBinaryType:
		return "BLOB"
	case *arrow.FixedSizeBinaryType:
		return "BLOB"
	case *arrow.Date32Type, *arrow.Date64Type:
		return "DATE"
	case *arrow.TimestampType:
		switch dt.Unit {
		case arrow.Nanosecond:
			return "TIMESTAMP_NS"
		case arrow.Microsecond:
			return "TIMESTAMP"
		case arrow.Millisecond:
			return "TIMESTAMP_MS"
		case arrow.Second:
			return "TIMESTAMP_S"
		}
		if dt.TimeZone != "" {
			return "TIMESTAMPTZ"
		}
		return "TIMESTAMP"
	case *arrow.Time32Type, *arrow.Time64Type:
		return "TIME"
	case *arrow.Decimal128Type:
		return fmt.Sprintf("DECIMAL(%d, %d)", dt.Precision, dt.Scale)
	case *arrow.Decimal256Type:
		precision := dt.Precision
		if precision > 38 {
			r.logger.Warn().Int32("arrow_precision", precision).Msg("Arrow Decimal256 precision > 38, clamping to 38 for DuckDB")
			precision = 38
		}
		return fmt.Sprintf("DECIMAL(%d, %d)", precision, dt.Scale)
	case *arrow.ListType:
		return r.arrowToDuckDBType(dt.Elem()) + "[]"
	case *arrow.LargeListType:
		return r.arrowToDuckDBType(dt.Elem()) + "[]"
	case *arrow.FixedSizeListType:
		return r.arrowToDuckDBType(dt.Elem()) + "[]"
	case *arrow.MapType:
		keyType := r.arrowToDuckDBType(dt.KeyType())
		valueType := r.arrowToDuckDBType(dt.ItemType())
		return fmt.Sprintf("MAP(%s, %s)", keyType, valueType)
	case *arrow.StructType:
		var fields []string
		for _, field := range dt.Fields() {
			fields = append(fields, fmt.Sprintf("%s %s", r.quoteIdentifier(field.Name), r.arrowToDuckDBType(field.Type)))
		}
		return "STRUCT(" + strings.Join(fields, ", ") + ")"
	case *arrow.DurationType:
		return "INTERVAL"
	case *arrow.MonthIntervalType, *arrow.DayTimeIntervalType, *arrow.MonthDayNanoIntervalType:
		return "INTERVAL"
	default:
		r.logger.Warn().Str("arrow_type", dt.String()).Msg("Unsupported Arrow type for DDL generation, defaulting to BLOB")
		return "BLOB"
	}
}
