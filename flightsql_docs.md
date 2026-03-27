package flightsql // import "github.com/apache/arrow-go/v18/arrow/flight/flightsql"


CONSTANTS

const (
	CatalogNameKey     = "ARROW:FLIGHT:SQL:CATALOG_NAME"
	SchemaNameKey      = "ARROW:FLIGHT:SQL:SCHEMA_NAME"
	TableNameKey       = "ARROW:FLIGHT:SQL:TABLE_NAME"
	TypeNameKey        = "ARROW:FLIGHT:SQL:TYPE_NAME"
	PrecisionKey       = "ARROW:FLIGHT:SQL:PRECISION"
	ScaleKey           = "ARROW:FLIGHT:SQL:SCALE"
	IsAutoIncrementKey = "ARROW:FLIGHT:SQL:IS_AUTO_INCREMENT"
	IsCaseSensitiveKey = "ARROW:FLIGHT:SQL:IS_CASE_SENSITIVE"
	IsReadOnlyKey      = "ARROW:FLIGHT:SQL:IS_READ_ONLY"
	IsSearchableKey    = "ARROW:FLIGHT:SQL:IS_SEARCHABLE"
	RemarksKey         = "ARROW:FLIGHT:SQL:REMARKS"
)
    Metadata Key Constants

const (
	CreatePreparedStatementActionType     = "CreatePreparedStatement"
	ClosePreparedStatementActionType      = "ClosePreparedStatement"
	CreatePreparedSubstraitPlanActionType = "CreatePreparedSubstraitPlan"
	CancelQueryActionType                 = "CancelQuery"
	BeginSavepointActionType              = "BeginSavepoint"
	BeginTransactionActionType            = "BeginTransaction"
	EndTransactionActionType              = "EndTransaction"
	EndSavepointActionType                = "EndSavepoint"
)
    Constants for Action types

const (

	// Retrieves a UTF-8 string with the name of the Flight SQL Server.
	SqlInfoFlightSqlServerName = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_NAME)
	// Retrieves a UTF-8 string with the native version of the Flight SQL Server.
	SqlInfoFlightSqlServerVersion = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_VERSION)
	// Retrieves a UTF-8 string with the Arrow format version of the Flight SQL Server.
	SqlInfoFlightSqlServerArrowVersion = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_ARROW_VERSION)

	// Retrieves a boolean value indicating whether the Flight SQL Server is read only.
	//
	// Returns:
	// - false: if read-write
	// - true: if read only
	SqlInfoFlightSqlServerReadOnly = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_READ_ONLY)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports executing
	// SQL queries.
	//
	// Note that the absence of this info (as opposed to a false value) does not necessarily
	// mean that SQL is not supported, as this property was not originally defined.
	SqlInfoFlightSqlServerSql = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_SQL)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports executing
	// Substrait plans.
	SqlInfoFlightSqlServerSubstrait = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_SUBSTRAIT)

	// Retrieves a string value indicating the minimum supported Substrait version, or null
	// if Substrait is not supported.
	SqlInfoFlightSqlServerSubstraitMinVersion = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_SUBSTRAIT_MIN_VERSION)

	// Retrieves a string value indicating the maximum supported Substrait version, or null
	// if Substrait is not supported.
	SqlInfoFlightSqlServerSubstraitMaxVersion = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_SUBSTRAIT_MAX_VERSION)

	// Retrieves an int32 indicating whether the Flight SQL Server supports the
	// BeginTransaction/EndTransaction/BeginSavepoint/EndSavepoint actions.
	//
	// Even if this is not supported, the database may still support explicit "BEGIN
	// TRANSACTION"/"COMMIT" SQL statements (see SQL_TRANSACTIONS_SUPPORTED); this property
	// is only about whether the server implements the Flight SQL API endpoints.
	//
	// The possible values are listed in `SqlSupportedTransaction`.
	SqlInfoFlightSqlServerTransaction = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_TRANSACTION)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports explicit
	// query cancellation (the CancelQuery action).
	SqlInfoFlightSqlServerCancel = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_CANCEL)

	// Retrieves an int32 indicating the timeout (in milliseconds) for prepared statement handles.
	//
	// If 0, there is no timeout.  Servers should reset the timeout when the handle is used in a command.
	SqlInfoFlightSqlServerStatementTimeout = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_STATEMENT_TIMEOUT)

	// Retrieves an int32 indicating the timeout (in milliseconds) for transactions, since transactions are not tied to a connection.
	//
	// If 0, there is no timeout.  Servers should reset the timeout when the handle is used in a command.
	SqlInfoFlightSqlServerTransactionTimeout = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_TRANSACTION_TIMEOUT)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports executing
	// bulk ingestion.
	SqlInfoFlightSqlServerBulkIngestion = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_BULK_INGESTION)
	// Retrieves a boolean value indicating whether transactions are supported for bulk ingestion. If not, invoking
	// the method commit in the context of a bulk ingestion is a noop, and the isolation level is
	// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel.TRANSACTION_NONE`.
	//
	// Returns:
	// - false: if bulk ingestion transactions are unsupported;
	// - true: if bulk ingestion transactions are supported.
	SqlInfoFlightSqlServerIngestTransactionsSupported = SqlInfo(pb.SqlInfo_FLIGHT_SQL_SERVER_INGEST_TRANSACTIONS_SUPPORTED)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of catalogs.
	//
	// Returns:
	// - false: if it doesn't support CREATE and DROP of catalogs.
	// - true: if it supports CREATE and DROP of catalogs.
	SqlInfoDDLCatalog = SqlInfo(pb.SqlInfo_SQL_DDL_CATALOG)

	// Retrieves a boolean value indicating whether the Flight SQL Server supports CREATE and DROP of schemas.
	//
	// Returns:
	// - false: if it doesn't support CREATE and DROP of schemas.
	// - true: if it supports CREATE and DROP of schemas.
	SqlInfoDDLSchema = SqlInfo(pb.SqlInfo_SQL_DDL_SCHEMA)

	// Indicates whether the Flight SQL Server supports CREATE and DROP of tables.
	//
	// Returns:
	// - false: if it doesn't support CREATE and DROP of tables.
	// - true: if it supports CREATE and DROP of tables.
	SqlInfoDDLTable = SqlInfo(pb.SqlInfo_SQL_DDL_TABLE)

	// Retrieves a int32 ordinal representing the case sensitivity of catalog, table, schema and table names.
	//
	// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
	SqlInfoIdentifierCase = SqlInfo(pb.SqlInfo_SQL_IDENTIFIER_CASE)
	// Retrieves a UTF-8 string with the supported character(s) used to surround a delimited identifier.
	SqlInfoIdentifierQuoteChar = SqlInfo(pb.SqlInfo_SQL_IDENTIFIER_QUOTE_CHAR)

	// Retrieves a int32 describing the case sensitivity of quoted identifiers.
	//
	// The possible values are listed in `arrow.flight.protocol.sql.SqlSupportedCaseSensitivity`.
	SqlInfoQuotedIdentifierCase = SqlInfo(pb.SqlInfo_SQL_QUOTED_IDENTIFIER_CASE)

	// Retrieves a boolean value indicating whether all tables are selectable.
	//
	// Returns:
	// - false: if not all tables are selectable or if none are;
	// - true: if all tables are selectable.
	SqlInfoAllTablesAreASelectable = SqlInfo(pb.SqlInfo_SQL_ALL_TABLES_ARE_SELECTABLE)

	// Retrieves the null ordering.
	//
	// Returns a int32 ordinal for the null ordering being used, as described in
	// `arrow.flight.protocol.sql.SqlNullOrdering`.
	SqlInfoNullOrdering = SqlInfo(pb.SqlInfo_SQL_NULL_ORDERING)
	// Retrieves a UTF-8 string list with values of the supported keywords.
	SqlInfoKeywords = SqlInfo(pb.SqlInfo_SQL_KEYWORDS)
	// Retrieves a UTF-8 string list with values of the supported numeric functions.
	SqlInfoNumericFunctions = SqlInfo(pb.SqlInfo_SQL_NUMERIC_FUNCTIONS)
	// Retrieves a UTF-8 string list with values of the supported string functions.
	SqlInfoStringFunctions = SqlInfo(pb.SqlInfo_SQL_STRING_FUNCTIONS)
	// Retrieves a UTF-8 string list with values of the supported system functions.
	SqlInfoSystemFunctions = SqlInfo(pb.SqlInfo_SQL_SYSTEM_FUNCTIONS)
	// Retrieves a UTF-8 string list with values of the supported datetime functions.
	SqlInfoDateTimeFunctions = SqlInfo(pb.SqlInfo_SQL_DATETIME_FUNCTIONS)

	// Retrieves the UTF-8 string that can be used to escape wildcard characters.
	// This is the string that can be used to escape '_' or '%' in the catalog search parameters that are a pattern
	// (and therefore use one of the wildcard characters).
	// The '_' character represents any single character; the '%' character represents any sequence of zero or more
	// characters.
	SqlInfoSearchStringEscape = SqlInfo(pb.SqlInfo_SQL_SEARCH_STRING_ESCAPE)

	// Retrieves a UTF-8 string with all the "extra" characters that can be used in unquoted identifier names
	// (those beyond a-z, A-Z, 0-9 and _).
	SqlInfoExtraNameChars = SqlInfo(pb.SqlInfo_SQL_EXTRA_NAME_CHARACTERS)

	// Retrieves a boolean value indicating whether column aliasing is supported.
	// If so, the SQL AS clause can be used to provide names for computed columns or to provide alias names for columns
	// as required.
	//
	// Returns:
	// - false: if column aliasing is unsupported;
	// - true: if column aliasing is supported.
	SqlInfoSupportsColumnAliasing = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_COLUMN_ALIASING)

	// Retrieves a boolean value indicating whether concatenations between null and non-null values being
	// null are supported.
	//
	// - Returns:
	// - false: if concatenations between null and non-null values being null are unsupported;
	// - true: if concatenations between null and non-null values being null are supported.
	SqlInfoNullPlusNullIsNull = SqlInfo(pb.SqlInfo_SQL_NULL_PLUS_NULL_IS_NULL)

	// Retrieves a map where the key is the type to convert from and the value is a list with the types to convert to,
	// indicating the supported conversions. Each key and each item on the list value is a value to a predefined type on
	// SqlSupportsConvert enum.
	// The returned map will be:  map<int32, list<int32>>
	SqlInfoSupportsConvert = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_CONVERT)

	// Retrieves a boolean value indicating whether, when table correlation names are supported,
	// they are restricted to being different from the names of the tables.
	//
	// Returns:
	// - false: if table correlation names are unsupported;
	// - true: if table correlation names are supported.
	SqlInfoSupportsTableCorrelationNames = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_TABLE_CORRELATION_NAMES)

	// Retrieves a boolean value indicating whether, when table correlation names are supported,
	// they are restricted to being different from the names of the tables.
	//
	// Returns:
	// - false: if different table correlation names are unsupported;
	// - true: if different table correlation names are supported
	SqlInfoSupportsDifferentTableCorrelationNames = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_DIFFERENT_TABLE_CORRELATION_NAMES)

	// Retrieves a boolean value indicating whether expressions in ORDER BY lists are supported.
	//
	// Returns:
	// - false: if expressions in ORDER BY are unsupported;
	// - true: if expressions in ORDER BY are supported;
	SqlInfoSupportsExpressionsInOrderBy = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_EXPRESSIONS_IN_ORDER_BY)

	// Retrieves a boolean value indicating whether using a column that is not in the SELECT statement in a GROUP BY
	// clause is supported.
	//
	// Returns:
	// - false: if using a column that is not in the SELECT statement in a GROUP BY clause is unsupported;
	// - true: if using a column that is not in the SELECT statement in a GROUP BY clause is supported.
	SqlInfoSupportsOrderByUnrelated = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_ORDER_BY_UNRELATED)

	// Retrieves the supported GROUP BY commands;
	//
	// Returns an int32 bitmask value representing the supported commands.
	// The returned bitmask should be parsed in order to retrieve the supported commands.
	//
	// For instance:
	// - return 0 (\b0)   => [] (GROUP BY is unsupported);
	// - return 1 (\b1)   => [SQL_GROUP_BY_UNRELATED];
	// - return 2 (\b10)  => [SQL_GROUP_BY_BEYOND_SELECT];
	// - return 3 (\b11)  => [SQL_GROUP_BY_UNRELATED, SQL_GROUP_BY_BEYOND_SELECT].
	// Valid GROUP BY types are described under `arrow.flight.protocol.sql.SqlSupportedGroupBy`.
	SqlInfoSupportedGroupBy = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_GROUP_BY)

	// Retrieves a boolean value indicating whether specifying a LIKE escape clause is supported.
	//
	// Returns:
	// - false: if specifying a LIKE escape clause is unsupported;
	// - true: if specifying a LIKE escape clause is supported.
	SqlInfoSupportsLikeEscapeClause = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_LIKE_ESCAPE_CLAUSE)

	// Retrieves a boolean value indicating whether columns may be defined as non-nullable.
	//
	// Returns:
	// - false: if columns cannot be defined as non-nullable;
	// - true: if columns may be defined as non-nullable.
	SqlInfoSupportsNonNullableColumns = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_NON_NULLABLE_COLUMNS)

	// Retrieves the supported SQL grammar level as per the ODBC specification.
	//
	// Returns an int32 bitmask value representing the supported SQL grammar level.
	// The returned bitmask should be parsed in order to retrieve the supported grammar levels.
	//
	// For instance:
	// - return 0 (\b0)   => [] (SQL grammar is unsupported);
	// - return 1 (\b1)   => [SQL_MINIMUM_GRAMMAR];
	// - return 2 (\b10)  => [SQL_CORE_GRAMMAR];
	// - return 3 (\b11)  => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR];
	// - return 4 (\b100) => [SQL_EXTENDED_GRAMMAR];
	// - return 5 (\b101) => [SQL_MINIMUM_GRAMMAR, SQL_EXTENDED_GRAMMAR];
	// - return 6 (\b110) => [SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR];
	// - return 7 (\b111) => [SQL_MINIMUM_GRAMMAR, SQL_CORE_GRAMMAR, SQL_EXTENDED_GRAMMAR].
	// Valid SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedSqlGrammar`.
	SqlInfoSupportedGrammar = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_GRAMMAR)

	// Retrieves the supported ANSI92 SQL grammar level.
	//
	// Returns an int32 bitmask value representing the supported ANSI92 SQL grammar level.
	// The returned bitmask should be parsed in order to retrieve the supported commands.
	//
	// For instance:
	// - return 0 (\b0)   => [] (ANSI92 SQL grammar is unsupported);
	// - return 1 (\b1)   => [ANSI92_ENTRY_SQL];
	// - return 2 (\b10)  => [ANSI92_INTERMEDIATE_SQL];
	// - return 3 (\b11)  => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL];
	// - return 4 (\b100) => [ANSI92_FULL_SQL];
	// - return 5 (\b101) => [ANSI92_ENTRY_SQL, ANSI92_FULL_SQL];
	// - return 6 (\b110) => [ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL];
	// - return 7 (\b111) => [ANSI92_ENTRY_SQL, ANSI92_INTERMEDIATE_SQL, ANSI92_FULL_SQL].
	// Valid ANSI92 SQL grammar levels are described under `arrow.flight.protocol.sql.SupportedAnsi92SqlGrammarLevel`.
	SqlInfoANSI92SupportedLevel = SqlInfo(pb.SqlInfo_SQL_ANSI92_SUPPORTED_LEVEL)

	// Retrieves a boolean value indicating whether the SQL Integrity Enhancement Facility is supported.
	//
	// Returns:
	// - false: if the SQL Integrity Enhancement Facility is supported;
	// - true: if the SQL Integrity Enhancement Facility is supported.
	SqlInfoSupportsIntegrityEnhancementFacility = SqlInfo(pb.SqlInfo_SQL_SUPPORTS_INTEGRITY_ENHANCEMENT_FACILITY)

	// Retrieves the support level for SQL OUTER JOINs.
	//
	// Returns a int32 ordinal for the SQL ordering being used, as described in
	// `arrow.flight.protocol.sql.SqlOuterJoinsSupportLevel`.
	SqlInfoOuterJoinsSupportLevel = SqlInfo(pb.SqlInfo_SQL_OUTER_JOINS_SUPPORT_LEVEL)

	// Retrieves a UTF-8 string with the preferred term for "schema".
	SqlInfoSchemaTerm = SqlInfo(pb.SqlInfo_SQL_SCHEMA_TERM)
	// Retrieves a UTF-8 string with the preferred term for "procedure".
	SqlInfoProcedureTerm = SqlInfo(pb.SqlInfo_SQL_PROCEDURE_TERM)

	// Retrieves a UTF-8 string with the preferred term for "catalog".
	// If a empty string is returned its assumed that the server does NOT supports catalogs.
	SqlInfoCatalogTerm = SqlInfo(pb.SqlInfo_SQL_CATALOG_TERM)

	// Retrieves a boolean value indicating whether a catalog appears at the start of a fully qualified table name.
	//
	// - false: if a catalog does not appear at the start of a fully qualified table name;
	// - true: if a catalog appears at the start of a fully qualified table name.
	SqlInfoCatalogAtStart = SqlInfo(pb.SqlInfo_SQL_CATALOG_AT_START)

	// Retrieves the supported actions for a SQL schema.
	//
	// Returns an int32 bitmask value representing the supported actions for a SQL schema.
	// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL schema.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported actions for SQL schema);
	// - return 1 (\b1)   => [SQL_ELEMENT_IN_PROCEDURE_CALLS];
	// - return 2 (\b10)  => [SQL_ELEMENT_IN_INDEX_DEFINITIONS];
	// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
	// - return 4 (\b100) => [SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
	// Valid actions for a SQL schema described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
	SqlInfoSchemasSupportedActions = SqlInfo(pb.SqlInfo_SQL_SCHEMAS_SUPPORTED_ACTIONS)

	// Retrieves the supported actions for a SQL schema.
	//
	// Returns an int32 bitmask value representing the supported actions for a SQL catalog.
	// The returned bitmask should be parsed in order to retrieve the supported actions for a SQL catalog.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported actions for SQL catalog);
	// - return 1 (\b1)   => [SQL_ELEMENT_IN_PROCEDURE_CALLS];
	// - return 2 (\b10)  => [SQL_ELEMENT_IN_INDEX_DEFINITIONS];
	// - return 3 (\b11)  => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS];
	// - return 4 (\b100) => [SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 5 (\b101) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 6 (\b110) => [SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS];
	// - return 7 (\b111) => [SQL_ELEMENT_IN_PROCEDURE_CALLS, SQL_ELEMENT_IN_INDEX_DEFINITIONS, SQL_ELEMENT_IN_PRIVILEGE_DEFINITIONS].
	// Valid actions for a SQL catalog are described under `arrow.flight.protocol.sql.SqlSupportedElementActions`.
	SqlInfoCatalogsSupportedActions = SqlInfo(pb.SqlInfo_SQL_CATALOGS_SUPPORTED_ACTIONS)

	// Retrieves the supported SQL positioned commands.
	//
	// Returns an int32 bitmask value representing the supported SQL positioned commands.
	// The returned bitmask should be parsed in order to retrieve the supported SQL positioned commands.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported SQL positioned commands);
	// - return 1 (\b1)   => [SQL_POSITIONED_DELETE];
	// - return 2 (\b10)  => [SQL_POSITIONED_UPDATE];
	// - return 3 (\b11)  => [SQL_POSITIONED_DELETE, SQL_POSITIONED_UPDATE].
	// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedPositionedCommands`.
	SqlInfoSupportedPositionedCommands = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_POSITIONED_COMMANDS)

	// Retrieves a boolean value indicating whether SELECT FOR UPDATE statements are supported.
	//
	// Returns:
	// - false: if SELECT FOR UPDATE statements are unsupported;
	// - true: if SELECT FOR UPDATE statements are supported.
	SqlInfoSelectForUpdateSupported = SqlInfo(pb.SqlInfo_SQL_SELECT_FOR_UPDATE_SUPPORTED)

	// Retrieves a boolean value indicating whether stored procedure calls that use the stored procedure escape syntax
	// are supported.
	//
	// Returns:
	// - false: if stored procedure calls that use the stored procedure escape syntax are unsupported;
	// - true: if stored procedure calls that use the stored procedure escape syntax are supported.
	SqlInfoStoredProceduresSupported = SqlInfo(pb.SqlInfo_SQL_STORED_PROCEDURES_SUPPORTED)

	// Retrieves the supported SQL subqueries.
	//
	// Returns an int32 bitmask value representing the supported SQL subqueries.
	// The returned bitmask should be parsed in order to retrieve the supported SQL subqueries.
	//
	// For instance:
	// - return 0   (\b0)     => [] (no supported SQL subqueries);
	// - return 1   (\b1)     => [SQL_SUBQUERIES_IN_COMPARISONS];
	// - return 2   (\b10)    => [SQL_SUBQUERIES_IN_EXISTS];
	// - return 3   (\b11)    => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS];
	// - return 4   (\b100)   => [SQL_SUBQUERIES_IN_INS];
	// - return 5   (\b101)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS];
	// - return 6   (\b110)   => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_EXISTS];
	// - return 7   (\b111)   => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS];
	// - return 8   (\b1000)  => [SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 9   (\b1001)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 10  (\b1010)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 11  (\b1011)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 12  (\b1100)  => [SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 13  (\b1101)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 14  (\b1110)  => [SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - return 15  (\b1111)  => [SQL_SUBQUERIES_IN_COMPARISONS, SQL_SUBQUERIES_IN_EXISTS, SQL_SUBQUERIES_IN_INS, SQL_SUBQUERIES_IN_QUANTIFIEDS];
	// - ...
	// Valid SQL subqueries are described under `arrow.flight.protocol.sql.SqlSupportedSubqueries`.
	SqlInfoSupportedSubqueries = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_SUBQUERIES)

	// Retrieves a boolean value indicating whether correlated subqueries are supported.
	//
	// Returns:
	// - false: if correlated subqueries are unsupported;
	// - true: if correlated subqueries are supported.
	SqlInfoCorrelatedSubqueriesSupported = SqlInfo(pb.SqlInfo_SQL_CORRELATED_SUBQUERIES_SUPPORTED)

	// Retrieves the supported SQL UNIONs.
	//
	// Returns an int32 bitmask value representing the supported SQL UNIONs.
	// The returned bitmask should be parsed in order to retrieve the supported SQL UNIONs.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported SQL positioned commands);
	// - return 1 (\b1)   => [SQL_UNION];
	// - return 2 (\b10)  => [SQL_UNION_ALL];
	// - return 3 (\b11)  => [SQL_UNION, SQL_UNION_ALL].
	// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlSupportedUnions`.
	SqlInfoSupportedUnions = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_UNIONS)

	// Retrieves a int64 value representing the maximum number of hex characters allowed in an inline binary literal.
	SqlInfoMaxBinaryLiteralLen = SqlInfo(pb.SqlInfo_SQL_MAX_BINARY_LITERAL_LENGTH)
	// Retrieves a int64 value representing the maximum number of characters allowed for a character literal.
	SqlInfoMaxCharLiteralLen = SqlInfo(pb.SqlInfo_SQL_MAX_CHAR_LITERAL_LENGTH)
	// Retrieves a int64 value representing the maximum number of characters allowed for a column name.
	SqlInfoMaxColumnNameLen = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMN_NAME_LENGTH)
	// Retrieves a int64 value representing the maximum number of columns allowed in a GROUP BY clause.
	SqlInfoMaxColumnsInGroupBy = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMNS_IN_GROUP_BY)
	// Retrieves a int64 value representing the maximum number of columns allowed in an index.
	SqlInfoMaxColumnsInIndex = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMNS_IN_INDEX)
	// Retrieves a int64 value representing the maximum number of columns allowed in an ORDER BY clause.
	SqlInfoMaxColumnsInOrderBy = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMNS_IN_ORDER_BY)
	// Retrieves a int64 value representing the maximum number of columns allowed in a SELECT list.
	SqlInfoMaxColumnsInSelect = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMNS_IN_SELECT)
	// Retrieves a int64 value representing the maximum number of columns allowed in a table.
	SqlInfoMaxColumnsInTable = SqlInfo(pb.SqlInfo_SQL_MAX_COLUMNS_IN_TABLE)
	// Retrieves a int64 value representing the maximum number of concurrent connections possible.
	SqlInfoMaxConnections = SqlInfo(pb.SqlInfo_SQL_MAX_CONNECTIONS)
	// Retrieves a int64 value the maximum number of characters allowed in a cursor name.
	SqlInfoMaxCursorNameLen = SqlInfo(pb.SqlInfo_SQL_MAX_CURSOR_NAME_LENGTH)

	// Retrieves a int64 value representing the maximum number of bytes allowed for an index,
	// including all of the parts of the index.
	SqlInfoMaxIndexLen = SqlInfo(pb.SqlInfo_SQL_MAX_INDEX_LENGTH)
	// Retrieves a int64 value representing the maximum number of characters allowed in a schema name.
	SqlInfoDBSchemaNameLen = SqlInfo(pb.SqlInfo_SQL_DB_SCHEMA_NAME_LENGTH)
	// Retrieves a int64 value representing the maximum number of characters allowed in a procedure name.
	SqlInfoMaxProcedureNameLen = SqlInfo(pb.SqlInfo_SQL_MAX_PROCEDURE_NAME_LENGTH)
	// Retrieves a int64 value representing the maximum number of characters allowed in a catalog name.
	SqlInfoMaxCatalogNameLen = SqlInfo(pb.SqlInfo_SQL_MAX_CATALOG_NAME_LENGTH)
	// Retrieves a int64 value representing the maximum number of bytes allowed in a single row.
	SqlInfoMaxRowSize = SqlInfo(pb.SqlInfo_SQL_MAX_ROW_SIZE)

	// Retrieves a boolean indicating whether the return value for the JDBC method getMaxRowSize includes the SQL
	// data types LONGVARCHAR and LONGVARBINARY.
	//
	// Returns:
	// - false: if return value for the JDBC method getMaxRowSize does
	//          not include the SQL data types LONGVARCHAR and LONGVARBINARY;
	// - true: if return value for the JDBC method getMaxRowSize includes
	//         the SQL data types LONGVARCHAR and LONGVARBINARY.
	SqlInfoMaxRowSizeIncludesBlobs = SqlInfo(pb.SqlInfo_SQL_MAX_ROW_SIZE_INCLUDES_BLOBS)

	// Retrieves a int64 value representing the maximum number of characters allowed for an SQL statement;
	// a result of 0 (zero) means that there is no limit or the limit is not known.
	SqlInfoMaxStatementLen = SqlInfo(pb.SqlInfo_SQL_MAX_STATEMENT_LENGTH)
	// Retrieves a int64 value representing the maximum number of active statements that can be open at the same time.
	SqlInfoMaxStatements = SqlInfo(pb.SqlInfo_SQL_MAX_STATEMENTS)
	// Retrieves a int64 value representing the maximum number of characters allowed in a table name.
	SqlInfoMaxTableNameLen = SqlInfo(pb.SqlInfo_SQL_MAX_TABLE_NAME_LENGTH)
	// Retrieves a int64 value representing the maximum number of tables allowed in a SELECT statement.
	SqlInfoMaxTablesInSelect = SqlInfo(pb.SqlInfo_SQL_MAX_TABLES_IN_SELECT)
	// Retrieves a int64 value representing the maximum number of characters allowed in a user name.
	SqlInfoMaxUsernameLen = SqlInfo(pb.SqlInfo_SQL_MAX_USERNAME_LENGTH)

	// Retrieves this database's default transaction isolation level as described in
	// `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
	//
	// Returns a int32 ordinal for the SQL transaction isolation level.
	SqlInfoDefaultTransactionIsolation = SqlInfo(pb.SqlInfo_SQL_DEFAULT_TRANSACTION_ISOLATION)

	// Retrieves a boolean value indicating whether transactions are supported. If not, invoking the method commit is a
	// noop, and the isolation level is `arrow.flight.protocol.sql.SqlTransactionIsolationLevel.TRANSACTION_NONE`.
	//
	// Returns:
	// - false: if transactions are unsupported;
	// - true: if transactions are supported.
	SqlInfoTransactionsSupported = SqlInfo(pb.SqlInfo_SQL_TRANSACTIONS_SUPPORTED)

	// Retrieves the supported transactions isolation levels.
	//
	// Returns an int32 bitmask value representing the supported transactions isolation levels.
	// The returned bitmask should be parsed in order to retrieve the supported transactions isolation levels.
	//
	// For instance:
	// - return 0   (\b0)     => [] (no supported SQL transactions isolation levels);
	// - return 1   (\b1)     => [SQL_TRANSACTION_NONE];
	// - return 2   (\b10)    => [SQL_TRANSACTION_READ_UNCOMMITTED];
	// - return 3   (\b11)    => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED];
	// - return 4   (\b100)   => [SQL_TRANSACTION_REPEATABLE_READ];
	// - return 5   (\b101)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 6   (\b110)   => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 7   (\b111)   => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 8   (\b1000)  => [SQL_TRANSACTION_REPEATABLE_READ];
	// - return 9   (\b1001)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 10  (\b1010)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 11  (\b1011)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 12  (\b1100)  => [SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 13  (\b1101)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 14  (\b1110)  => [SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 15  (\b1111)  => [SQL_TRANSACTION_NONE, SQL_TRANSACTION_READ_UNCOMMITTED, SQL_TRANSACTION_REPEATABLE_READ, SQL_TRANSACTION_REPEATABLE_READ];
	// - return 16  (\b10000) => [SQL_TRANSACTION_SERIALIZABLE];
	// - ...
	// Valid SQL positioned commands are described under `arrow.flight.protocol.sql.SqlTransactionIsolationLevel`.
	SqlInfoSupportedTransactionsIsolationlevels = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_TRANSACTIONS_ISOLATION_LEVELS)

	// Retrieves a boolean value indicating whether a data definition statement within a transaction forces
	// the transaction to commit.
	//
	// Returns:
	// - false: if a data definition statement within a transaction does not force the transaction to commit;
	// - true: if a data definition statement within a transaction forces the transaction to commit.
	SqlInfoDataDefinitionCausesTransactionCommit = SqlInfo(pb.SqlInfo_SQL_DATA_DEFINITION_CAUSES_TRANSACTION_COMMIT)

	// Retrieves a boolean value indicating whether a data definition statement within a transaction is ignored.
	//
	// Returns:
	// - false: if a data definition statement within a transaction is taken into account;
	// - true: a data definition statement within a transaction is ignored.
	SqlInfoDataDefinitionsInTransactionsIgnored = SqlInfo(pb.SqlInfo_SQL_DATA_DEFINITIONS_IN_TRANSACTIONS_IGNORED)

	// Retrieves an int32 bitmask value representing the supported result set types.
	// The returned bitmask should be parsed in order to retrieve the supported result set types.
	//
	// For instance:
	// - return 0   (\b0)     => [] (no supported result set types);
	// - return 1   (\b1)     => [SQL_RESULT_SET_TYPE_UNSPECIFIED];
	// - return 2   (\b10)    => [SQL_RESULT_SET_TYPE_FORWARD_ONLY];
	// - return 3   (\b11)    => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY];
	// - return 4   (\b100)   => [SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
	// - return 5   (\b101)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
	// - return 6   (\b110)   => [SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
	// - return 7   (\b111)   => [SQL_RESULT_SET_TYPE_UNSPECIFIED, SQL_RESULT_SET_TYPE_FORWARD_ONLY, SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE];
	// - return 8   (\b1000)  => [SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE];
	// - ...
	// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetType`.
	SqlInfoSupportedResultSetTypes = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_RESULT_SET_TYPES)

	// Returns an int32 bitmask value concurrency types supported for
	// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_UNSPECIFIED`.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
	// - return 1 (\b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
	// - return 2 (\b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 4 (\b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
	SqlInfoSupportedConcurrenciesForResultSetUnspecified = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_UNSPECIFIED)

	// Returns an int32 bitmask value concurrency types supported for
	// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_FORWARD_ONLY`.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
	// - return 1 (\b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
	// - return 2 (\b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 4 (\b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
	SqlInfoSupportedConcurrenciesForResultSetForwardOnly = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_FORWARD_ONLY)

	// Returns an int32 bitmask value concurrency types supported for
	// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_SENSITIVE`.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
	// - return 1 (\b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
	// - return 2 (\b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 4 (\b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
	SqlInfoSupportedConcurrenciesForResultSetScrollSensitive = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_SENSITIVE)

	// Returns an int32 bitmask value concurrency types supported for
	// `arrow.flight.protocol.sql.SqlSupportedResultSetType.SQL_RESULT_SET_TYPE_SCROLL_INSENSITIVE`.
	//
	// For instance:
	// - return 0 (\b0)   => [] (no supported concurrency types for this result set type)
	// - return 1 (\b1)   => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED]
	// - return 2 (\b10)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 3 (\b11)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY]
	// - return 4 (\b100) => [SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 5 (\b101) => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 6 (\b110)  => [SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// - return 7 (\b111)  => [SQL_RESULT_SET_CONCURRENCY_UNSPECIFIED, SQL_RESULT_SET_CONCURRENCY_READ_ONLY, SQL_RESULT_SET_CONCURRENCY_UPDATABLE]
	// Valid result set types are described under `arrow.flight.protocol.sql.SqlSupportedResultSetConcurrency`.
	SqlInfoSupportedConcurrenciesForResultSetScrollInsensitive = SqlInfo(pb.SqlInfo_SQL_SUPPORTED_CONCURRENCIES_FOR_RESULT_SET_SCROLL_INSENSITIVE)

	// Retrieves a boolean value indicating whether this database supports batch updates.
	//
	// - false: if this database does not support batch updates;
	// - true: if this database supports batch updates.
	SqlInfoBatchUpdatesSupported = SqlInfo(pb.SqlInfo_SQL_BATCH_UPDATES_SUPPORTED)

	// Retrieves a boolean value indicating whether this database supports savepoints.
	//
	// Returns:
	// - false: if this database does not support savepoints;
	// - true: if this database supports savepoints.
	SqlInfoSavePointsSupported = SqlInfo(pb.SqlInfo_SQL_SAVEPOINTS_SUPPORTED)

	// Retrieves a boolean value indicating whether named parameters are supported in callable statements.
	//
	// Returns:
	// - false: if named parameters in callable statements are unsupported;
	// - true: if named parameters in callable statements are supported.
	SqlInfoNamedParametersSupported = SqlInfo(pb.SqlInfo_SQL_NAMED_PARAMETERS_SUPPORTED)

	// Retrieves a boolean value indicating whether updates made to a LOB are made on a copy or directly to the LOB.
	//
	// Returns:
	// - false: if updates made to a LOB are made directly to the LOB;
	// - true: if updates made to a LOB are made on a copy.
	SqlInfoLocatorsUpdateCopy = SqlInfo(pb.SqlInfo_SQL_LOCATORS_UPDATE_COPY)

	// Retrieves a boolean value indicating whether invoking user-defined or vendor functions
	// using the stored procedure escape syntax is supported.
	//
	// Returns:
	// - false: if invoking user-defined or vendor functions using the stored procedure escape syntax is unsupported;
	// - true: if invoking user-defined or vendor functions using the stored procedure escape syntax is supported.
	SqlInfoStoredFunctionsUsingCallSyntaxSupported = SqlInfo(pb.SqlInfo_SQL_STORED_FUNCTIONS_USING_CALL_SYNTAX_SUPPORTED)
)
    SqlInfo enum values

const (
	// Unknown/not indicated/no support
	SqlTransactionNone = pb.SqlSupportedTransaction_SQL_SUPPORTED_TRANSACTION_NONE
	// Transactions, but not savepoints.
	// a savepoint is a mark within a transaction that can be individually
	// rolled back to. Not all databases support savepoints.
	SqlTransactionTransaction = pb.SqlSupportedTransaction_SQL_SUPPORTED_TRANSACTION_TRANSACTION
	// Transactions AND Savepoints supported
	SqlTransactionSavepoint = pb.SqlSupportedTransaction_SQL_SUPPORTED_TRANSACTION_SAVEPOINT
)
const (
	SqlCaseSensitivityUnknown         = pb.SqlSupportedCaseSensitivity_SQL_CASE_SENSITIVITY_UNKNOWN
	SqlCaseSensitivityCaseInsensitive = pb.SqlSupportedCaseSensitivity_SQL_CASE_SENSITIVITY_CASE_INSENSITIVE
	SqlCaseSensitivityUpperCase       = pb.SqlSupportedCaseSensitivity_SQL_CASE_SENSITIVITY_UPPERCASE
	SqlCaseSensitivityLowerCase       = pb.SqlSupportedCaseSensitivity_SQL_CASE_SENSITIVITY_LOWERCASE
)
const (
	SqlNullOrderingSortHigh    = pb.SqlNullOrdering_SQL_NULLS_SORTED_HIGH
	SqlNullOrderingSortLow     = pb.SqlNullOrdering_SQL_NULLS_SORTED_LOW
	SqlNullOrderingSortAtStart = pb.SqlNullOrdering_SQL_NULLS_SORTED_AT_START
	SqlNullOrderingSortAtEnd   = pb.SqlNullOrdering_SQL_NULLS_SORTED_AT_END
)
const (
	SqlConvertBigInt            = pb.SqlSupportsConvert_SQL_CONVERT_BIGINT
	SqlConvertBinary            = pb.SqlSupportsConvert_SQL_CONVERT_BINARY
	SqlConvertBit               = pb.SqlSupportsConvert_SQL_CONVERT_BIT
	SqlConvertChar              = pb.SqlSupportsConvert_SQL_CONVERT_CHAR
	SqlConvertDate              = pb.SqlSupportsConvert_SQL_CONVERT_DATE
	SqlConvertDecimal           = pb.SqlSupportsConvert_SQL_CONVERT_DECIMAL
	SqlConvertFloat             = pb.SqlSupportsConvert_SQL_CONVERT_FLOAT
	SqlConvertInteger           = pb.SqlSupportsConvert_SQL_CONVERT_INTEGER
	SqlConvertIntervalDayTime   = pb.SqlSupportsConvert_SQL_CONVERT_INTERVAL_DAY_TIME
	SqlConvertIntervalYearMonth = pb.SqlSupportsConvert_SQL_CONVERT_INTERVAL_YEAR_MONTH
	SqlConvertLongVarbinary     = pb.SqlSupportsConvert_SQL_CONVERT_LONGVARBINARY
	SqlConvertLongVarchar       = pb.SqlSupportsConvert_SQL_CONVERT_LONGVARCHAR
	SqlConvertNumeric           = pb.SqlSupportsConvert_SQL_CONVERT_NUMERIC
	SqlConvertReal              = pb.SqlSupportsConvert_SQL_CONVERT_REAL
	SqlConvertSmallInt          = pb.SqlSupportsConvert_SQL_CONVERT_SMALLINT
	SqlConvertTime              = pb.SqlSupportsConvert_SQL_CONVERT_TIME
	SqlConvertTimestamp         = pb.SqlSupportsConvert_SQL_CONVERT_TIMESTAMP
	SqlConvertTinyInt           = pb.SqlSupportsConvert_SQL_CONVERT_TINYINT
	SqlConvertVarbinary         = pb.SqlSupportsConvert_SQL_CONVERT_VARBINARY
	SqlConvertVarchar           = pb.SqlSupportsConvert_SQL_CONVERT_VARCHAR
)
const (
	EndTransactionUnspecified = pb.ActionEndTransactionRequest_END_TRANSACTION_UNSPECIFIED
	// Commit the transaction
	EndTransactionCommit = pb.ActionEndTransactionRequest_END_TRANSACTION_COMMIT
	// Roll back the transaction
	EndTransactionRollback = pb.ActionEndTransactionRequest_END_TRANSACTION_ROLLBACK
)
const (
	EndSavepointUnspecified = pb.ActionEndSavepointRequest_END_SAVEPOINT_UNSPECIFIED
	// Release the savepoint
	EndSavepointRelease = pb.ActionEndSavepointRequest_END_SAVEPOINT_RELEASE
	// Roll back to a savepoint
	EndSavepointRollback = pb.ActionEndSavepointRequest_END_SAVEPOINT_ROLLBACK
)
const (
	// The cancellation status is unknown. Servers should avoid using
	// this value (send a NOT_FOUND error if the requested query is
	// not known). Clients can retry the request.
	CancelResultUnspecified = pb.ActionCancelQueryResult_CANCEL_RESULT_UNSPECIFIED
	// The cancellation request is complete. Subsequent requests with
	// the same payload may return CANCELLED or a NOT_FOUND error.
	CancelResultCancelled = pb.ActionCancelQueryResult_CANCEL_RESULT_CANCELLED
	// The cancellation request is in progress. The client may retry
	// the cancellation request.
	CancelResultCancelling = pb.ActionCancelQueryResult_CANCEL_RESULT_CANCELLING
	// The query is not cancellable. The client should not retry the
	// cancellation request.
	CancelResultNotCancellable = pb.ActionCancelQueryResult_CANCEL_RESULT_NOT_CANCELLABLE
)
const (
	TableDefinitionOptionsTableNotExistOptionUnspecified = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_UNSPECIFIED
	TableDefinitionOptionsTableNotExistOptionCreate      = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE
	TableDefinitionOptionsTableNotExistOptionFail        = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_FAIL

	TableDefinitionOptionsTableExistsOptionUnspecified = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_UNSPECIFIED
	TableDefinitionOptionsTableExistsOptionFail        = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_FAIL
	TableDefinitionOptionsTableExistsOptionAppend      = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_APPEND
	TableDefinitionOptionsTableExistsOptionReplace     = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_REPLACE
)

VARIABLES

var (
	ErrInvalidTxn         = fmt.Errorf("%w: missing a valid transaction", arrow.ErrInvalid)
	ErrInvalidSavepoint   = fmt.Errorf("%w: missing a valid savepoint", arrow.ErrInvalid)
	ErrBadServerTxn       = fmt.Errorf("%w: server returned an empty transaction ID", arrow.ErrInvalid)
	ErrBadServerSavepoint = fmt.Errorf("%w: server returned an empty savepoint ID", arrow.ErrInvalid)
)

FUNCTIONS

func CreateStatementQueryTicket(handle []byte) ([]byte, error)
    CreateStatementQueryTicket is a helper that constructs a properly serialized
    TicketStatementQuery containing a given opaque binary handle for use with
    constructing a ticket to return from GetFlightInfoStatement.

func NewFlightServer(srv Server) flight.FlightServer
    NewFlightServer constructs a FlightRPC server from the provided FlightSQL
    Server so that it can be passed to RegisterFlightService.

func NewFlightServerWithAllocator(srv Server, mem memory.Allocator) flight.FlightServer
    NewFlightServerWithAllocator constructs a FlightRPC server from the provided
    FlightSQL Server so that it can be passed to RegisterFlightService,
    setting the provided allocator into the server for use with any allocations
    necessary by the routing.

    Will default to memory.DefaultAllocator if mem is nil


TYPES

type ActionBeginSavepointRequest interface {
	GetTransactionId() []byte
	GetName() string
}

type ActionBeginSavepointResult interface {
	GetSavepointId() []byte
}

type ActionBeginTransactionRequest interface{}

type ActionBeginTransactionResult interface {
	GetTransactionId() []byte
}

type ActionCancelQueryRequest interface {
	GetInfo() *flight.FlightInfo
}

type ActionClosePreparedStatementRequest interface {
	// GetPreparedStatementHandle returns the server-generated opaque
	// identifier for the statement
	GetPreparedStatementHandle() []byte
}
    ActionClosePreparedStatementRequest represents a request to close a prepared
    statement

type ActionCreatePreparedStatementRequest interface {
	GetQuery() string
	GetTransactionId() []byte
}
    ActionCreatePreparedStatementRequest represents a request to construct a new
    prepared statement

type ActionCreatePreparedStatementResult struct {
	Handle          []byte
	DatasetSchema   *arrow.Schema
	ParameterSchema *arrow.Schema
}
    ActionCreatePreparedStatementResult is the result of creating a new prepared
    statement, optionally including the dataset and parameter schemas.

type ActionCreatePreparedSubstraitPlanRequest interface {
	GetPlan() SubstraitPlan
	GetTransactionId() []byte
}

type ActionEndSavepointRequest interface {
	GetSavepointId() []byte
	GetAction() EndSavepointRequestType
}

type ActionEndTransactionRequest interface {
	GetTransactionId() []byte
	GetAction() EndTransactionRequestType
}

type BaseServer struct {

	// Alloc allows specifying a particular allocator to use for any
	// allocations done by the base implementation.
	// Will use memory.DefaultAllocator if nil
	Alloc memory.Allocator
	// Has unexported fields.
}
    BaseServer must be embedded into any FlightSQL Server implementation and
    provides default implementations of all methods returning an unimplemented
    error if called. This allows consumers to gradually implement methods
    as they want instead of requiring all consumers to boilerplate the same
    "unimplemented" methods.

    The base implementation also contains handling for registering sql info and
    serving it up in response to GetSqlInfo requests.

func (BaseServer) BeginSavepoint(context.Context, ActionBeginSavepointRequest) ([]byte, error)

func (BaseServer) BeginTransaction(context.Context, ActionBeginTransactionRequest) ([]byte, error)

func (BaseServer) CancelFlightInfo(context.Context, *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error)

func (BaseServer) ClosePreparedStatement(context.Context, ActionClosePreparedStatementRequest) error

func (BaseServer) CloseSession(context.Context, *flight.CloseSessionRequest) (*flight.CloseSessionResult, error)

func (BaseServer) CreatePreparedStatement(context.Context, ActionCreatePreparedStatementRequest) (res ActionCreatePreparedStatementResult, err error)

func (BaseServer) CreatePreparedSubstraitPlan(context.Context, ActionCreatePreparedSubstraitPlanRequest) (res ActionCreatePreparedStatementResult, err error)

func (BaseServer) DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetCrossReference(context.Context, CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetDBSchemas(context.Context, GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetExportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetImportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetPreparedStatement(context.Context, PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetPrimaryKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (b *BaseServer) DoGetSqlInfo(_ context.Context, cmd GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)
    DoGetSqlInfo returns a flight stream containing the list of sqlinfo results

func (BaseServer) DoGetStatement(context.Context, StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetTables(context.Context, GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoGetXdbcTypeInfo(context.Context, GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)

func (BaseServer) DoPutCommandStatementIngest(context.Context, StatementIngest, flight.MessageReader) (int64, error)

func (BaseServer) DoPutCommandStatementUpdate(context.Context, StatementUpdate) (int64, error)

func (BaseServer) DoPutCommandSubstraitPlan(context.Context, StatementSubstraitPlan) (int64, error)

func (BaseServer) DoPutPreparedStatementQuery(context.Context, PreparedStatementQuery, flight.MessageReader, flight.MetadataWriter) ([]byte, error)

func (BaseServer) DoPutPreparedStatementUpdate(context.Context, PreparedStatementUpdate, flight.MessageReader) (int64, error)

func (BaseServer) EndSavepoint(context.Context, ActionEndSavepointRequest) error

func (BaseServer) EndTransaction(context.Context, ActionEndTransactionRequest) error

func (BaseServer) GetFlightInfoCatalogs(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoCrossReference(context.Context, CrossTableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoExportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoImportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoPrimaryKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoSchemas(context.Context, GetDBSchemas, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (b *BaseServer) GetFlightInfoSqlInfo(_ context.Context, _ GetSqlInfo, desc *flight.FlightDescriptor) (*flight.FlightInfo, error)
    GetFlightInfoSqlInfo is a base implementation of GetSqlInfo by using any
    registered sqlinfo (by calling RegisterSqlInfo). Will return an error if
    there is no sql info registered, otherwise a FlightInfo for retrieving the
    Sql info.

func (BaseServer) GetFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoTableTypes(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoTables(context.Context, GetTables, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetFlightInfoXdbcTypeInfo(context.Context, GetXdbcTypeInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error)

func (BaseServer) GetSchemaPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.SchemaResult, error)

func (BaseServer) GetSchemaStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.SchemaResult, error)

func (BaseServer) GetSchemaSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.SchemaResult, error)

func (BaseServer) GetSessionOptions(context.Context, *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error)

func (BaseServer) PollFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.PollInfo, error)

func (BaseServer) PollFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.PollInfo, error)

func (BaseServer) PollFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.PollInfo, error)

func (BaseServer) PollFlightInfoSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.PollInfo, error)

func (b *BaseServer) RegisterSqlInfo(id SqlInfo, result interface{}) error
    RegisterSqlInfo registers a specific result to return for a given sqlinfo
    id. The result must be one of the following types: string, bool, int64,
    int32, []string, or map[int32][]int32.

    Once registered, this value will be returned for any SqlInfo requests.

func (BaseServer) RenewFlightEndpoint(context.Context, *flight.RenewFlightEndpointRequest) (*flight.FlightEndpoint, error)

func (BaseServer) SetSessionOptions(context.Context, *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error)

type CancelResult = pb.ActionCancelQueryResult_CancelResult

type Client struct {
	Client flight.Client

	Alloc memory.Allocator
}
    Client wraps a regular Flight RPC Client to provide the FlightSQL interface
    functions and methods.

func NewClient(addr string, auth flight.ClientAuthHandler, middleware []flight.ClientMiddleware, opts ...grpc.DialOption) (*Client, error)
    NewClient is a convenience function to automatically construct a
    flight.Client and return a flightsql.Client containing it rather than having
    to manually construct both yourself. It just delegates its arguments to
    flight.NewClientWithMiddleware to create the underlying Flight Client.

func NewClientCtx(ctx context.Context, addr string, auth flight.ClientAuthHandler, middleware []flight.ClientMiddleware, opts ...grpc.DialOption) (*Client, error)

func (c *Client) BeginTransaction(ctx context.Context, opts ...grpc.CallOption) (*Txn, error)

func (c *Client) CancelFlightInfo(ctx context.Context, request *flight.CancelFlightInfoRequest, opts ...grpc.CallOption) (*flight.CancelFlightInfoResult, error)

func (c *Client) CancelQuery(ctx context.Context, info *flight.FlightInfo, opts ...grpc.CallOption) (cancelResult CancelResult, err error)
    Deprecated: In 13.0.0. Use CancelFlightInfo instead if you can assume that
    server requires 13.0.0 or later. Otherwise, you may need to use CancelQuery
    and/or CancelFlightInfo.

func (c *Client) Close() error
    Close will close the underlying flight Client in use by this
    flightsql.Client

func (c *Client) CloseSession(ctx context.Context, request *flight.CloseSessionRequest, opts ...grpc.CallOption) (*flight.CloseSessionResult, error)

func (c *Client) DoGet(ctx context.Context, in *flight.Ticket, opts ...grpc.CallOption) (*flight.Reader, error)
    DoGet uses the provided flight ticket to request the stream of data.
    It returns a recordbatch reader to stream the results. Release should be
    called on the reader when done.

func (c *Client) Execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    Execute executes the desired query on the server and returns a FlightInfo
    object describing where to retrieve the results.

func (c *Client) ExecuteIngest(ctx context.Context, rdr array.RecordReader, reqOptions *ExecuteIngestOpts, opts ...grpc.CallOption) (int64, error)
    ExecuteIngest is for executing a bulk ingestion and only returns the
    number of affected rows. The provided RecordReader will be retained for the
    duration of the call, but it is the caller's responsibility to release the
    original reference.

func (c *Client) ExecutePoll(ctx context.Context, query string, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error)
    ExecutePoll idempotently starts execution of a query/checks for completion.
    To check for completion, pass the FlightDescriptor from the previous call to
    ExecutePoll as the retryDescriptor.

func (c *Client) ExecuteSubstrait(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error)

func (c *Client) ExecuteSubstraitPoll(ctx context.Context, plan SubstraitPlan, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error)

func (c *Client) ExecuteSubstraitUpdate(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (n int64, err error)

func (c *Client) ExecuteUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error)
    ExecuteUpdate is for executing an update query and only returns the number
    of affected rows.

func (c *Client) GetCatalogs(ctx context.Context, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetCatalogs requests the list of catalogs from the server and returns a
    flightInfo object where the response can be retrieved

func (c *Client) GetCatalogsSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetCatalogsSchema requests the schema of GetCatalogs from the server

func (c *Client) GetCrossReference(ctx context.Context, pkTable, fkTable TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetCrossReference retrieves a description of the foreign key columns in
    the specified ForeignKey table that reference the primary key or columns
    representing a restraint of the parent table (could be the same or a
    different table). Returns a FlightInfo object indicating where the response
    can be retrieved with DoGet.

func (c *Client) GetCrossReferenceSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetCrossReferenceSchema requests the schema of GetCrossReference from the
    server.

func (c *Client) GetDBSchemas(ctx context.Context, cmdOpts *GetDBSchemasOpts, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetDBSchemas requests the list of schemas from the database and returns a
    FlightInfo object where the response can be retrieved

func (c *Client) GetDBSchemasSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetDBSchemasSchema requests the schema of GetDBSchemas from the server

func (c *Client) GetExecuteSchema(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetExecuteSchema gets the schema of the result set of a query without
    executing the query itself.

func (c *Client) GetExecuteSubstraitSchema(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (*flight.SchemaResult, error)

func (c *Client) GetExportedKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetExportedKeys retrieves a description about the foreign key columns
    that reference the primary key columns of the specified table. Returns a
    FlightInfo object where the response can be retrieved.

func (c *Client) GetExportedKeysSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetExportedKeysSchema requests the schema of GetExportedKeys from the
    server.

func (c *Client) GetImportedKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetImportedKeys returns the foreign key columns for the specified table.
    Returns a FlightInfo object indicating where the response can be retrieved.

func (c *Client) GetImportedKeysSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetImportedKeysSchema requests the schema of GetImportedKeys from the
    server.

func (c *Client) GetPrimaryKeys(ctx context.Context, ref TableRef, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetPrimaryKeys requests the primary keys for a specific table from the
    server, specified using a TableRef. Returns a FlightInfo object where the
    response can be retrieved.

func (c *Client) GetPrimaryKeysSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetPrimaryKeysSchema requests the schema of GetPrimaryKeys from the server.

func (c *Client) GetSessionOptions(ctx context.Context, request *flight.GetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.GetSessionOptionsResult, error)

func (c *Client) GetSqlInfo(ctx context.Context, info []SqlInfo, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetSqlInfo returns a list of the requested SQL information corresponding to
    the values in the info slice. Returns a FlightInfo object indicating where
    the response can be retrieved.

func (c *Client) GetSqlInfoSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetSqlInfoSchema requests the schema of GetSqlInfo from the server.

func (c *Client) GetTableTypes(ctx context.Context, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetTableTypes requests a list of the types of tables available on this
    server. Returns a FlightInfo object indicating where the response can be
    retrieved.

func (c *Client) GetTableTypesSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetTableTypesSchema requests the schema of GetTableTypes from the server.

func (c *Client) GetTables(ctx context.Context, reqOptions *GetTablesOpts, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetTables requests a list of tables from the server, with the provided
    options describing how to make the request (filter patterns, if the schema
    should be returned, etc.). Returns a FlightInfo object where the response
    can be retrieved.

func (c *Client) GetTablesSchema(ctx context.Context, reqOptions *GetTablesOpts, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetTablesSchema requests the schema of GetTables from the server.

func (c *Client) GetXdbcTypeInfo(ctx context.Context, dataType *int32, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    GetXdbcTypeInfo requests the information about all the data types supported
    (dataType == nil) or a specific data type. Returns a FlightInfo object
    indicating where the response can be retrieved.

func (c *Client) GetXdbcTypeInfoSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetXdbcTypeInfoSchema requests the schema of GetXdbcTypeInfo from the
    server.

func (c *Client) LoadPreparedStatementFromResult(result *CreatePreparedStatementResult) (*PreparedStatement, error)

func (c *Client) Prepare(ctx context.Context, query string, opts ...grpc.CallOption) (prep *PreparedStatement, err error)
    Prepare creates a PreparedStatement object for the specified query. The
    resulting PreparedStatement object should be Closed when no longer needed.
    It will maintain a reference to this Client for use to execute and use the
    specified allocator for any allocations it needs to perform.

func (c *Client) PrepareSubstrait(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (stmt *PreparedStatement, err error)

func (c *Client) RenewFlightEndpoint(ctx context.Context, request *flight.RenewFlightEndpointRequest, opts ...grpc.CallOption) (*flight.FlightEndpoint, error)

func (c *Client) SetSessionOptions(ctx context.Context, request *flight.SetSessionOptionsRequest, opts ...grpc.CallOption) (*flight.SetSessionOptionsResult, error)

type ColumnMetadata struct {
	Data *arrow.Metadata
}
    ColumnMetadata is a helper object for managing and querying the standard SQL
    Column metadata using the expected Metadata Keys. It can be created by just
    Wrapping an existing *arrow.Metadata.

    Each of the methods return a value and a boolean indicating if it was set in
    the metadata or not.

func (c *ColumnMetadata) CatalogName() (string, bool)

func (c *ColumnMetadata) IsAutoIncrement() (bool, bool)

func (c *ColumnMetadata) IsCaseSensitive() (bool, bool)

func (c *ColumnMetadata) IsReadOnly() (bool, bool)

func (c *ColumnMetadata) IsSearchable() (bool, bool)

func (c *ColumnMetadata) Precision() (int32, bool)

func (c *ColumnMetadata) Remarks() (string, bool)

func (c *ColumnMetadata) Scale() (int32, bool)

func (c *ColumnMetadata) SchemaName() (string, bool)

func (c *ColumnMetadata) TableName() (string, bool)

func (c *ColumnMetadata) TypeName() (string, bool)

type ColumnMetadataBuilder struct {
	// Has unexported fields.
}
    ColumnMetadataBuilder is a convenience builder for constructing sql column
    metadata using the expected standard metadata keys. All methods return the
    builder itself so it can be chained to easily construct a final metadata
    object.

func NewColumnMetadataBuilder() *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) Build() ColumnMetadata

func (c *ColumnMetadataBuilder) CatalogName(name string) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) Clear()

func (c *ColumnMetadataBuilder) IsAutoIncrement(v bool) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) IsCaseSensitive(v bool) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) IsReadOnly(v bool) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) IsSearchable(v bool) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) Metadata() arrow.Metadata

func (c *ColumnMetadataBuilder) Precision(prec int32) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) Remarks(remarks string) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) Scale(prec int32) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) SchemaName(name string) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) TableName(name string) *ColumnMetadataBuilder

func (c *ColumnMetadataBuilder) TypeName(name string) *ColumnMetadataBuilder

type CreatePreparedStatementResult = pb.ActionCreatePreparedStatementResult

type CrossTableRef struct {
	PKRef TableRef
	FKRef TableRef
}
    CrossTableRef contains a reference to a Primary Key table and a Foreign Key
    table.

type EndSavepointRequestType = pb.ActionEndSavepointRequest_EndSavepoint

type EndTransactionRequestType = pb.ActionEndTransactionRequest_EndTransaction

type ExecuteIngestOpts pb.CommandStatementIngest
    ExecuteIngestOpts contains the options for executing a bulk ingestion:

    Required: - TableDefinitionOptions: Specifies the behavior for creating or
    updating table definitions - Table: The destination table to load into

    Optional: - Schema: The DB schema containing the destination table -
    Catalog: The catalog containing the destination table - Temporary: Use a
    temporary table as the destination - TransactionId: Ingest as part of this
    transaction - Options: Additional, backend-specific options

type GetDBSchemas interface {
	GetCatalog() *string
	GetDBSchemaFilterPattern() *string
}
    GetDBSchemas represents a request for list of database schemas

type GetDBSchemasOpts pb.CommandGetDbSchemas
    GetDBSchemasOpts contains the options to request Database Schemas:
    an optional Catalog and a Schema Name filter pattern.

type GetSqlInfo interface {
	// GetInfo returns a slice of SqlInfo ids to return information about
	GetInfo() []uint32
}
    GetSqlInfo represents a request for SQL Information

type GetTables interface {
	GetCatalog() *string
	GetDBSchemaFilterPattern() *string
	GetTableNameFilterPattern() *string
	GetTableTypes() []string
	GetIncludeSchema() bool
}
    GetTables represents a request to list the database's tables

type GetTablesOpts pb.CommandGetTables
    GetTablesOpts contains the options for retrieving a list of tables: optional
    Catalog, Schema filter pattern, Table name filter pattern, a filter of table
    types, and whether or not to include the schema in the response.

type GetXdbcTypeInfo interface {
	// GetDataType returns either nil (get for all types)
	// or a specific SQL type ID to fetch information about.
	GetDataType() *int32
}
    GetXdbcTypeInfo represents a request for SQL Data Type information

type PreparedStatement struct {
	// Has unexported fields.
}
    PreparedStatement represents a constructed PreparedStatement on the server
    and maintains a reference to the Client that created it along with the
    prepared statement handle.

    If the server returned the Dataset Schema or Parameter Binding schemas at
    creation, they will also be accessible from this object. Close should be
    called when no longer needed.

func NewPreparedStatement(client *Client, handle []byte) *PreparedStatement
    NewPreparedStatement creates a prepared statement object bound to the
    provided client using the given handle. In general, it should be sufficient
    to use the Prepare function a client and this wouldn't be needed. But this
    can be used to propagate a prepared statement from one client to another if
    needed or if proxying requests.

func (p *PreparedStatement) Close(ctx context.Context, opts ...grpc.CallOption) error
    Close calls release on any parameter binding record and sends a
    ClosePreparedStatement action to the server. After calling Close, the
    PreparedStatement should not be used again.

func (p *PreparedStatement) DatasetSchema() *arrow.Schema
    DatasetSchema may be nil if the server did not return it when creating the
    Prepared Statement.

func (p *PreparedStatement) Execute(ctx context.Context, opts ...grpc.CallOption) (*flight.FlightInfo, error)
    Execute executes the prepared statement on the server and returns a
    FlightInfo indicating where to retrieve the response. If SetParameters has
    been called then the parameter bindings will be sent before execution.

    Will error if already closed.

func (p *PreparedStatement) ExecutePoll(ctx context.Context, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error)
    ExecutePoll executes the prepared statement on the server and returns a
    PollInfo indicating the progress of execution.

    Will error if already closed.

func (p *PreparedStatement) ExecutePut(ctx context.Context, opts ...grpc.CallOption) error
    ExecutePut calls DoPut for the prepared statement on the server.
    If SetParameters has been called then the parameter bindings will be sent
    before execution.

    Will error if already closed.

func (p *PreparedStatement) ExecuteUpdate(ctx context.Context, opts ...grpc.CallOption) (nrecords int64, err error)
    ExecuteUpdate executes the prepared statement update query on the server
    and returns the number of rows affected. If SetParameters was called,
    the parameter bindings will be sent with the request to execute.

func (p *PreparedStatement) GetSchema(ctx context.Context, opts ...grpc.CallOption) (*flight.SchemaResult, error)
    GetSchema re-requests the schema of the result set of the prepared statement
    from the server. It should otherwise be identical to DatasetSchema.

    Will error if already closed.

func (p *PreparedStatement) Handle() []byte
    The handle associated with this PreparedStatement

func (p *PreparedStatement) ParameterSchema() *arrow.Schema
    ParameterSchema may be nil if the server did not return it when creating the
    prepared statement.

func (p *PreparedStatement) SetParameters(binding arrow.RecordBatch)
    SetParameters takes a record batch to send as the parameter bindings when
    executing. It should match the schema from ParameterSchema.

    This will call Retain on the record to ensure it doesn't get released out
    from under the statement. Release will be called on a previous binding
    record or reader if it existed, and will be called upon calling Close on the
    PreparedStatement.

func (p *PreparedStatement) SetRecordReader(binding array.RecordReader)
    SetRecordReader takes a RecordReader to send as the parameter bindings when
    executing. It should match the schema from ParameterSchema.

    This will call Retain on the reader to ensure it doesn't get released out
    from under the statement. Release will be called on a previous binding
    record or reader if it existed, and will be called upon calling Close on the
    PreparedStatement.

type PreparedStatementQuery interface {
	// GetPreparedStatementHandle returns the server-generated opaque
	// identifier for the statement
	GetPreparedStatementHandle() []byte
}
    PreparedStatementQuery represents a prepared query statement

type PreparedStatementUpdate interface {
	// GetPreparedStatementHandle returns the server-generated opaque
	// identifier for the statement
	GetPreparedStatementHandle() []byte
}
    PreparedStatementUpdate represents a prepared update statement

type Savepoint []byte
    Savepoint is a handle for a server-side savepoint

func (sp Savepoint) IsValid() bool

type Server interface {
	// GetFlightInfoStatement returns a FlightInfo for executing the requested sql query
	GetFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// GetFlightInfoSubstraitPlan returns a FlightInfo for executing the requested substrait plan
	GetFlightInfoSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// GetSchemaStatement returns the schema of the result set of the requested sql query
	GetSchemaStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.SchemaResult, error)
	// GetSchemaSubstraitPlan returns the schema of the result set for the requested substrait plan
	GetSchemaSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.SchemaResult, error)
	// DoGetStatement returns a stream containing the query results for the
	// requested statement handle that was populated by GetFlightInfoStatement
	DoGetStatement(context.Context, StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoPreparedStatement returns a FlightInfo for executing an already
	// prepared statement with the provided statement handle.
	GetFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// GetSchemaPreparedStatement returns the schema of the result set of executing an already
	// prepared statement with the provided statement handle.
	GetSchemaPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.SchemaResult, error)
	// DoGetPreparedStatement returns a stream containing the results from executing
	// a prepared statement query with the provided statement handle.
	DoGetPreparedStatement(context.Context, PreparedStatementQuery) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoCatalogs returns a FlightInfo for the listing of all catalogs
	GetFlightInfoCatalogs(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetCatalogs returns the stream containing the list of catalogs
	DoGetCatalogs(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoXdbcTypeInfo returns a FlightInfo for retrieving data type info
	GetFlightInfoXdbcTypeInfo(context.Context, GetXdbcTypeInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetXdbcTypeInfo returns a stream containing the information about the
	// requested supported datatypes
	DoGetXdbcTypeInfo(context.Context, GetXdbcTypeInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoSqlInfo returns a FlightInfo for retrieving SqlInfo from the server
	GetFlightInfoSqlInfo(context.Context, GetSqlInfo, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetSqlInfo returns a stream containing the list of SqlInfo results
	DoGetSqlInfo(context.Context, GetSqlInfo) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoSchemas returns a FlightInfo for requesting a list of schemas
	GetFlightInfoSchemas(context.Context, GetDBSchemas, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetDBSchemas returns a stream containing the list of schemas
	DoGetDBSchemas(context.Context, GetDBSchemas) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoTables returns a FlightInfo for listing the tables available
	GetFlightInfoTables(context.Context, GetTables, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetTables returns a stream containing the list of tables
	DoGetTables(context.Context, GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoTableTypes returns a FlightInfo for retrieving a list
	// of table types supported
	GetFlightInfoTableTypes(context.Context, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetTableTypes returns a stream containing the data related to the table types
	DoGetTableTypes(context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoPrimaryKeys returns a FlightInfo for extracting information about primary keys
	GetFlightInfoPrimaryKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetPrimaryKeys returns a stream containing the data related to primary keys
	DoGetPrimaryKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoExportedKeys returns a FlightInfo for extracting information about foreign keys
	GetFlightInfoExportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetExportedKeys returns a stream containing the data related to foreign keys
	DoGetExportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoImportedKeys returns a FlightInfo for extracting information about imported keys
	GetFlightInfoImportedKeys(context.Context, TableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetImportedKeys returns a stream containing the data related to imported keys
	DoGetImportedKeys(context.Context, TableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// GetFlightInfoCrossReference returns a FlightInfo for extracting data related
	// to primary and foreign keys
	GetFlightInfoCrossReference(context.Context, CrossTableRef, *flight.FlightDescriptor) (*flight.FlightInfo, error)
	// DoGetCrossReference returns a stream of data related to foreign and primary keys
	DoGetCrossReference(context.Context, CrossTableRef) (*arrow.Schema, <-chan flight.StreamChunk, error)
	// DoPutCommandStatementUpdate executes a sql update statement and returns
	// the number of affected rows
	DoPutCommandStatementUpdate(context.Context, StatementUpdate) (int64, error)
	// DoPutCommandSubstraitPlan executes a substrait plan and returns the number
	// of affected rows.
	DoPutCommandSubstraitPlan(context.Context, StatementSubstraitPlan) (int64, error)
	// CreatePreparedStatement constructs a prepared statement from a sql query
	// and returns an opaque statement handle for use.
	CreatePreparedStatement(context.Context, ActionCreatePreparedStatementRequest) (ActionCreatePreparedStatementResult, error)
	// CreatePreparedSubstraitPlan constructs a prepared statement from a substrait
	// plan, and returns an opaque statement handle for use.
	CreatePreparedSubstraitPlan(context.Context, ActionCreatePreparedSubstraitPlanRequest) (ActionCreatePreparedStatementResult, error)
	// ClosePreparedStatement closes the prepared statement identified by the requested
	// opaque statement handle.
	ClosePreparedStatement(context.Context, ActionClosePreparedStatementRequest) error
	// DoPutPreparedStatementQuery binds parameters to a given prepared statement
	// identified by the provided statement handle.
	//
	// The provided MessageReader is a stream of record batches with optional
	// app metadata and flight descriptors to represent the values to bind
	// to the parameters.
	//
	// Currently anything written to the writer will be ignored. It is in the
	// interface for potential future enhancements to avoid having to change
	// the interface in the future.
	DoPutPreparedStatementQuery(context.Context, PreparedStatementQuery, flight.MessageReader, flight.MetadataWriter) ([]byte, error)
	// DoPutPreparedStatementUpdate executes an update SQL Prepared statement
	// for the specified statement handle. The reader allows providing a sequence
	// of uploaded record batches to bind the parameters to. Returns the number
	// of affected records.
	DoPutPreparedStatementUpdate(context.Context, PreparedStatementUpdate, flight.MessageReader) (int64, error)
	// BeginTransaction starts a new transaction and returns the id
	BeginTransaction(context.Context, ActionBeginTransactionRequest) (id []byte, err error)
	// BeginSavepoint initializes a new savepoint and returns the id
	BeginSavepoint(context.Context, ActionBeginSavepointRequest) (id []byte, err error)
	// EndSavepoint releases or rolls back a savepoint
	EndSavepoint(context.Context, ActionEndSavepointRequest) error
	// EndTransaction commits or rolls back a transaction
	EndTransaction(context.Context, ActionEndTransactionRequest) error
	// CancelFlightInfo attempts to explicitly cancel a FlightInfo
	CancelFlightInfo(context.Context, *flight.CancelFlightInfoRequest) (flight.CancelFlightInfoResult, error)
	// RenewFlightEndpoint attempts to extend the expiration of a FlightEndpoint
	RenewFlightEndpoint(context.Context, *flight.RenewFlightEndpointRequest) (*flight.FlightEndpoint, error)
	// PollFlightInfo is a generic handler for PollFlightInfo requests.
	PollFlightInfo(context.Context, *flight.FlightDescriptor) (*flight.PollInfo, error)
	// PollFlightInfoStatement handles polling for query execution.
	PollFlightInfoStatement(context.Context, StatementQuery, *flight.FlightDescriptor) (*flight.PollInfo, error)
	// PollFlightInfoSubstraitPlan handles polling for query execution.
	PollFlightInfoSubstraitPlan(context.Context, StatementSubstraitPlan, *flight.FlightDescriptor) (*flight.PollInfo, error)
	// PollFlightInfoPreparedStatement handles polling for query execution.
	PollFlightInfoPreparedStatement(context.Context, PreparedStatementQuery, *flight.FlightDescriptor) (*flight.PollInfo, error)
	// SetSessionOptions sets option(s) for the current server session.
	SetSessionOptions(context.Context, *flight.SetSessionOptionsRequest) (*flight.SetSessionOptionsResult, error)
	// GetSessionOptions gets option(s) for the current server session.
	GetSessionOptions(context.Context, *flight.GetSessionOptionsRequest) (*flight.GetSessionOptionsResult, error)
	// CloseSession closes/invalidates the current server session.
	CloseSession(context.Context, *flight.CloseSessionRequest) (*flight.CloseSessionResult, error)
	// DoPutCommandStatementIngest executes a bulk ingestion and returns
	// the number of affected rows
	DoPutCommandStatementIngest(context.Context, StatementIngest, flight.MessageReader) (int64, error)

	// Has unexported methods.
}
    Server is the required interface for a FlightSQL server. It is implemented
    by BaseServer which must be embedded in any implementation. The default
    implementation by BaseServer for each of these (except GetSqlInfo)

    GetFlightInfo* methods should return the FlightInfo object representing
    where to retrieve the results for a given request.

    DoGet* methods should return the Schema of the resulting stream along
    with a channel to retrieve stream chunks (each chunk is a record batch
    and optionally a descriptor and app metadata). The channel will be read
    from until it closes, sending each chunk on the stream. Since the channel
    is returned from the method, it should be populated within a goroutine to
    ensure there are no deadlocks.

type SqlInfo uint32
    since we are hiding the Protobuf internals in an internal package, we need
    to provide enum values for the SqlInfo enum here

func (s SqlInfo) String() string

type SqlInfoResultMap map[uint32]interface{}
    SqlInfoResultMap is a mapping of SqlInfo ids to the desired response. This
    is part of a Server and used for registering responses to a SqlInfo request.

type SqlNullOrdering = pb.SqlNullOrdering
    SqlNullOrdering indicates how nulls are sorted

    duplicated from protobuf to avoid relying directly on the protobuf generated
    code, also making them shorter and easier to use

type SqlSupportedCaseSensitivity = pb.SqlSupportedCaseSensitivity
    SqlSupportedCaseSensitivity indicates whether something (e.g. an identifier)
    is case-sensitive

    duplicated from protobuf to avoid relying directly on the protobuf generated
    code, also making them shorter and easier to use

type SqlSupportedTransaction = pb.SqlSupportedTransaction

type SqlSupportsConvert = pb.SqlSupportsConvert
    SqlSupportsConvert indicates support for converting between different types.

    duplicated from protobuf to avoid relying directly on the protobuf generated
    code, also making them shorter and easier to use

type StatementIngest interface {
	GetTableDefinitionOptions() *TableDefinitionOptions
	GetTable() string
	GetSchema() string
	GetCatalog() string
	GetTemporary() bool
	GetTransactionId() []byte
	GetOptions() map[string]string
}
    StatementIngest represents a bulk ingestion request

type StatementQuery interface {
	GetQuery() string
	GetTransactionId() []byte
}
    StatementQuery represents a Sql Query

type StatementQueryTicket interface {
	// GetStatementHandle returns the server-generated opaque
	// identifier for the query
	GetStatementHandle() []byte
}
    StatementQueryTicket represents a request to execute a query

func GetStatementQueryTicket(ticket *flight.Ticket) (result StatementQueryTicket, err error)

type StatementSubstraitPlan interface {
	GetTransactionId() []byte
	GetPlan() SubstraitPlan
}

type StatementUpdate interface {
	GetQuery() string
	GetTransactionId() []byte
}
    StatementUpdate represents a SQL update query

type SubstraitPlan struct {
	// the serialized plan
	Plan []byte
	// the substrait release, e.g. "0.23.0"
	Version string
}
    SubstraitPlan represents a plan to be executed, along with the associated
    metadata

type TableDefinitionOptions = pb.CommandStatementIngest_TableDefinitionOptions

type TableDefinitionOptionsTableExistsOption = pb.CommandStatementIngest_TableDefinitionOptions_TableExistsOption

type TableDefinitionOptionsTableNotExistOption = pb.CommandStatementIngest_TableDefinitionOptions_TableNotExistOption

type TableRef struct {
	// Catalog specifies the catalog this table belongs to.
	// An empty string refers to tables without a catalog.
	// If nil, can reference a table in any catalog.
	Catalog *string
	// DBSchema specifies the database schema the table belongs to.
	// An empty string refers to a table which does not belong to
	// a database schema.
	// If nil, can reference a table in any database schema.
	DBSchema *string
	// Table is the name of the table that is being referenced.
	Table string
}
    TableRef is a helpful struct for referencing a specific Table by its
    catalog, schema, and table name.

type Transaction []byte
    Transaction is a handle for a server-side transaction

func (tx Transaction) IsValid() bool

type Txn struct {
	// Has unexported fields.
}

func (tx *Txn) BeginSavepoint(ctx context.Context, name string, opts ...grpc.CallOption) (Savepoint, error)

func (tx *Txn) Commit(ctx context.Context, opts ...grpc.CallOption) error

func (tx *Txn) Execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error)

func (tx *Txn) ExecutePoll(ctx context.Context, query string, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error)

func (tx *Txn) ExecuteSubstrait(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error)

func (tx *Txn) ExecuteSubstraitPoll(ctx context.Context, plan SubstraitPlan, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error)

func (tx *Txn) ExecuteSubstraitUpdate(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (n int64, err error)

func (tx *Txn) ExecuteUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error)

func (tx *Txn) GetExecuteSchema(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.SchemaResult, error)

func (tx *Txn) GetExecuteSubstraitSchema(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (*flight.SchemaResult, error)

func (tx *Txn) ID() Transaction

func (tx *Txn) Prepare(ctx context.Context, query string, opts ...grpc.CallOption) (prep *PreparedStatement, err error)

func (tx *Txn) PrepareSubstrait(ctx context.Context, plan SubstraitPlan, opts ...grpc.CallOption) (stmt *PreparedStatement, err error)

func (tx *Txn) ReleaseSavepoint(ctx context.Context, sp Savepoint, opts ...grpc.CallOption) error

func (tx *Txn) Rollback(ctx context.Context, opts ...grpc.CallOption) error

func (tx *Txn) RollbackSavepoint(ctx context.Context, sp Savepoint, opts ...grpc.CallOption) error

