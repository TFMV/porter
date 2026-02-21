// Package services contains business logic implementations.
package services

import (
	"fmt"
	"regexp"
	"strings"
	"sync"
)

// StatementType represents the type of SQL statement.
type StatementType int

const (
	StatementTypeDDL     StatementType = iota // CREATE, DROP, ALTER, TRUNCATE
	StatementTypeDML                          // INSERT, UPDATE, DELETE, REPLACE, MERGE
	StatementTypeDQL                          // SELECT, WITH...SELECT
	StatementTypeTCL                          // COMMIT, ROLLBACK, SAVEPOINT, BEGIN
	StatementTypeDCL                          // GRANT, REVOKE, DENY
	StatementTypeUtility                      // SHOW, DESCRIBE, EXPLAIN, ANALYZE, SET, USE, PRAGMA
	StatementTypeOther                        // Unrecognized statements
)

// String returns the string representation of the statement type.
func (st StatementType) String() string {
	switch st {
	case StatementTypeDDL:
		return "DDL"
	case StatementTypeDML:
		return "DML"
	case StatementTypeDQL:
		return "DQL"
	case StatementTypeTCL:
		return "TCL"
	case StatementTypeDCL:
		return "DCL"
	case StatementTypeUtility:
		return "UTILITY"
	case StatementTypeOther:
		return "OTHER"
	default:
		return "UNKNOWN"
	}
}

// StatementComplexity indicates the estimated complexity of a SQL statement.
type StatementComplexity int

const (
	ComplexitySimple StatementComplexity = iota
	ComplexityModerate
	ComplexityComplex
	ComplexityVeryComplex
)

// String returns the string representation of statement complexity.
func (sc StatementComplexity) String() string {
	switch sc {
	case ComplexitySimple:
		return "simple"
	case ComplexityModerate:
		return "moderate"
	case ComplexityComplex:
		return "complex"
	case ComplexityVeryComplex:
		return "very_complex"
	default:
		return "unknown"
	}
}

// StatementInfo provides comprehensive information about a SQL statement.
type StatementInfo struct {
	Type                StatementType
	Complexity          StatementComplexity
	ExpectsResultSet    bool
	ExpectsUpdateCount  bool
	RequiresTransaction bool
	IsReadOnly          bool
	IsDangerous         bool
	HasSQLInjectionRisk bool
	Keywords            []string
	Tables              []string
	Operations          []string
	SecurityRisk        SecurityRiskLevel
}

// SecurityRiskLevel indicates the security risk level of a statement.
type SecurityRiskLevel int

const (
	SecurityRiskNone SecurityRiskLevel = iota
	SecurityRiskLow
	SecurityRiskMedium
	SecurityRiskHigh
	SecurityRiskCritical
)

// String returns the string representation of security risk level.
func (srl SecurityRiskLevel) String() string {
	switch srl {
	case SecurityRiskNone:
		return "none"
	case SecurityRiskLow:
		return "low"
	case SecurityRiskMedium:
		return "medium"
	case SecurityRiskHigh:
		return "high"
	case SecurityRiskCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// EnterpriseStatementClassifier provides comprehensive SQL statement analysis.
type EnterpriseStatementClassifier struct {
	// Compiled regex patterns for performance
	ddlPatterns     []*regexp.Regexp
	dmlPatterns     []*regexp.Regexp
	dqlPatterns     []*regexp.Regexp
	tclPatterns     []*regexp.Regexp
	dclPatterns     []*regexp.Regexp
	utilityPatterns []*regexp.Regexp

	// Security patterns
	dangerousPatterns []*regexp.Regexp
	injectionPatterns []*regexp.Regexp

	// Complexity analysis patterns
	complexityPatterns map[string]int

	// SQL keywords and functions from GizmoSQL
	sqlKeywords      map[string]bool
	numericFunctions map[string]bool
	stringFunctions  map[string]bool
	dateFunctions    map[string]bool
	systemFunctions  map[string]bool

	// Thread safety
	mu sync.RWMutex
}

// NewEnterpriseStatementClassifier creates a new enterprise-grade statement classifier.
func NewEnterpriseStatementClassifier() *EnterpriseStatementClassifier {
	classifier := &EnterpriseStatementClassifier{
		complexityPatterns: make(map[string]int),
		sqlKeywords:        make(map[string]bool),
		numericFunctions:   make(map[string]bool),
		stringFunctions:    make(map[string]bool),
		dateFunctions:      make(map[string]bool),
		systemFunctions:    make(map[string]bool),
	}

	classifier.initializePatterns()
	classifier.initializeSQLKnowledge()

	return classifier
}

// initializePatterns compiles all regex patterns for statement classification.
func (esc *EnterpriseStatementClassifier) initializePatterns() {
	// DDL patterns - Data Definition Language
	esc.ddlPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*CREATE\s+`),
		regexp.MustCompile(`(?i)^\s*DROP\s+`),
		regexp.MustCompile(`(?i)^\s*ALTER\s+`),
		regexp.MustCompile(`(?i)^\s*TRUNCATE\s+`),
		regexp.MustCompile(`(?i)^\s*COMMENT\s+ON\s+`),
		regexp.MustCompile(`(?i)^\s*RENAME\s+`),
	}

	// DML patterns - Data Manipulation Language
	esc.dmlPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*INSERT\s+`),
		regexp.MustCompile(`(?i)^\s*UPDATE\s+`),
		regexp.MustCompile(`(?i)^\s*DELETE\s+`),
		regexp.MustCompile(`(?i)^\s*REPLACE\s+`),
		regexp.MustCompile(`(?i)^\s*MERGE\s+`),
		regexp.MustCompile(`(?i)^\s*UPSERT\s+`),
		regexp.MustCompile(`(?i)^\s*COPY\s+.*\s+FROM\s+`),
		regexp.MustCompile(`(?i)^\s*BULK\s+INSERT\s+`),
	}

	// DQL patterns - Data Query Language
	esc.dqlPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*SELECT\s+`),
		regexp.MustCompile(`(?i)^\s*WITH\s+.*\s+SELECT\s+`),
		regexp.MustCompile(`(?i)^\s*\(\s*SELECT\s+`), // Subquery
		regexp.MustCompile(`(?i)^\s*VALUES\s+`),
		regexp.MustCompile(`(?i)^\s*TABLE\s+`), // TABLE statement
	}

	// TCL patterns - Transaction Control Language
	esc.tclPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*BEGIN\s*`),
		regexp.MustCompile(`(?i)^\s*START\s+TRANSACTION\s*`),
		regexp.MustCompile(`(?i)^\s*COMMIT\s*`),
		regexp.MustCompile(`(?i)^\s*ROLLBACK\s*`),
		regexp.MustCompile(`(?i)^\s*SAVEPOINT\s+`),
		regexp.MustCompile(`(?i)^\s*RELEASE\s+SAVEPOINT\s+`),
		regexp.MustCompile(`(?i)^\s*SET\s+TRANSACTION\s+`),
	}

	// DCL patterns - Data Control Language
	esc.dclPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*GRANT\s+`),
		regexp.MustCompile(`(?i)^\s*REVOKE\s+`),
		regexp.MustCompile(`(?i)^\s*DENY\s+`),
		regexp.MustCompile(`(?i)^\s*CREATE\s+USER\s+`),
		regexp.MustCompile(`(?i)^\s*DROP\s+USER\s+`),
		regexp.MustCompile(`(?i)^\s*ALTER\s+USER\s+`),
		regexp.MustCompile(`(?i)^\s*CREATE\s+ROLE\s+`),
		regexp.MustCompile(`(?i)^\s*DROP\s+ROLE\s+`),
	}

	// Utility patterns
	esc.utilityPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)^\s*SHOW\s+`),
		regexp.MustCompile(`(?i)^\s*DESCRIBE\s+`),
		regexp.MustCompile(`(?i)^\s*DESC\s+`),
		regexp.MustCompile(`(?i)^\s*EXPLAIN\s+`),
		regexp.MustCompile(`(?i)^\s*ANALYZE\s+`),
		regexp.MustCompile(`(?i)^\s*SET\s+`),
		regexp.MustCompile(`(?i)^\s*USE\s+`),
		regexp.MustCompile(`(?i)^\s*PRAGMA\s+`),
		regexp.MustCompile(`(?i)^\s*VACUUM\s*`),
		regexp.MustCompile(`(?i)^\s*REINDEX\s+`),
		regexp.MustCompile(`(?i)^\s*CHECKPOINT\s*`),
		regexp.MustCompile(`(?i)^\s*ATTACH\s+`),
		regexp.MustCompile(`(?i)^\s*DETACH\s+`),
	}

	// Dangerous operation patterns
	esc.dangerousPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)DROP\s+DATABASE`),
		regexp.MustCompile(`(?i)DROP\s+SCHEMA`),
		regexp.MustCompile(`(?i)TRUNCATE\s+.*DATABASE`),
		regexp.MustCompile(`(?i)DELETE\s+FROM\s+.*WHERE\s+1\s*=\s*1`),
		regexp.MustCompile(`(?i)UPDATE\s+.*SET\s+.*WHERE\s+1\s*=\s*1`),
		regexp.MustCompile(`(?i)DROP\s+TABLE\s+.*\*`),
		regexp.MustCompile(`(?i)SHUTDOWN`),
		regexp.MustCompile(`(?i)KILL\s+`),
		regexp.MustCompile(`(?i)FORMAT\s+`),
		regexp.MustCompile(`(?i)RESTORE\s+`),
		regexp.MustCompile(`(?i)BACKUP\s+`),
	}

	// SQL injection patterns
	esc.injectionPatterns = []*regexp.Regexp{
		regexp.MustCompile(`(?i)';`),
		regexp.MustCompile(`(?i)--`),
		regexp.MustCompile(`(?i)/\*`),
		regexp.MustCompile(`(?i)\*/`),
		regexp.MustCompile(`(?i)UNION\s+.*SELECT`),
		regexp.MustCompile(`(?i)OR\s+1\s*=\s*1`),
		regexp.MustCompile(`(?i)AND\s+1\s*=\s*1`),
		regexp.MustCompile(`(?i)'\s*OR\s*'`),
		regexp.MustCompile(`(?i)'\s*AND\s*'`),
		regexp.MustCompile(`(?i)EXEC\s*\(`),
		regexp.MustCompile(`(?i)EXECUTE\s*\(`),
		regexp.MustCompile(`(?i)SP_`),
		regexp.MustCompile(`(?i)XP_`),
	}

	// Complexity analysis patterns with weights
	esc.complexityPatterns = map[string]int{
		"JOIN":         2,
		"LEFT JOIN":    2,
		"RIGHT JOIN":   2,
		"FULL JOIN":    3,
		"CROSS JOIN":   3,
		"INNER JOIN":   2,
		"OUTER JOIN":   3,
		"SUBQUERY":     3,
		"WITH":         2,
		"CTE":          2,
		"WINDOW":       3,
		"OVER":         3,
		"PARTITION BY": 2,
		"GROUP BY":     1,
		"HAVING":       1,
		"ORDER BY":     1,
		"UNION":        2,
		"UNION ALL":    2,
		"INTERSECT":    2,
		"EXCEPT":       2,
		"CASE":         1,
		"EXISTS":       2,
		"NOT EXISTS":   2,
		"IN":           1,
		"NOT IN":       1,
		"ANY":          2,
		"ALL":          2,
		"SOME":         2,
		"RECURSIVE":    4,
		"LATERAL":      3,
	}
}

// initializeSQLKnowledge initializes comprehensive SQL knowledge from GizmoSQL.
func (esc *EnterpriseStatementClassifier) initializeSQLKnowledge() {
	// SQL Keywords from GizmoSQL (400+ keywords)
	keywords := []string{
		"ABORT", "ABSOLUTE", "ACCESS", "ACTION", "ADD", "ADMIN", "AFTER", "AGGREGATE",
		"ALL", "ALSO", "ALTER", "ALWAYS", "ANALYSE", "ANALYZE", "AND", "ANY", "ARRAY",
		"AS", "ASC", "ASSERTION", "ASSIGNMENT", "ASYMMETRIC", "AT", "ATTACH", "ATTRIBUTE",
		"AUTHORIZATION", "BACKWARD", "BEFORE", "BEGIN", "BETWEEN", "BIGINT", "BINARY",
		"BIT", "BOOLEAN", "BOTH", "BY", "CACHE", "CALL", "CALLED", "CASCADE", "CASCADED",
		"CASE", "CAST", "CATALOG", "CHAIN", "CHAR", "CHARACTER", "CHARACTERISTICS",
		"CHECK", "CHECKPOINT", "CLASS", "CLOSE", "CLUSTER", "COALESCE", "COLLATE",
		"COLLATION", "COLUMN", "COLUMNS", "COMMENT", "COMMENTS", "COMMIT", "COMMITTED",
		"CONCURRENTLY", "CONFIGURATION", "CONFLICT", "CONNECTION", "CONSTRAINT",
		"CONSTRAINTS", "CONTENT", "CONTINUE", "CONVERSION", "COPY", "COST", "CREATE",
		"CROSS", "CSV", "CUBE", "CURRENT", "CURRENT_CATALOG", "CURRENT_DATE",
		"CURRENT_ROLE", "CURRENT_SCHEMA", "CURRENT_TIME", "CURRENT_TIMESTAMP",
		"CURRENT_USER", "CURSOR", "CYCLE", "DATA", "DATABASE", "DAY", "DAYS",
		"DEALLOCATE", "DEC", "DECIMAL", "DECLARE", "DEFAULT", "DEFAULTS", "DEFERRABLE",
		"DEFERRED", "DEFINER", "DELETE", "DELIMITER", "DELIMITERS", "DEPENDS", "DESC",
		"DESCRIBE", "DETACH", "DICTIONARY", "DISABLE", "DISCARD", "DISTINCT", "DO",
		"DOCUMENT", "DOMAIN", "DOUBLE", "DROP", "EACH", "ELSE", "ENABLE", "ENCODING",
		"ENCRYPTED", "END", "ENUM", "ESCAPE", "EVENT", "EXCEPT", "EXCLUDE", "EXCLUDING",
		"EXCLUSIVE", "EXECUTE", "EXISTS", "EXPLAIN", "EXPORT", "EXTENSION", "EXTERNAL",
		"EXTRACT", "FALSE", "FAMILY", "FETCH", "FILTER", "FIRST", "FLOAT", "FOLLOWING",
		"FOR", "FORCE", "FOREIGN", "FORWARD", "FREEZE", "FROM", "FULL", "FUNCTION",
		"FUNCTIONS", "GENERATED", "GLOB", "GLOBAL", "GRANT", "GRANTED", "GROUP",
		"GROUPING", "HANDLER", "HAVING", "HEADER", "HOLD", "HOUR", "HOURS", "IDENTITY",
		"IF", "ILIKE", "IMMEDIATE", "IMMUTABLE", "IMPLICIT", "IMPORT", "IN", "INCLUDING",
		"INCREMENT", "INDEX", "INDEXES", "INHERIT", "INHERITS", "INITIALLY", "INLINE",
		"INNER", "INOUT", "INPUT", "INSENSITIVE", "INSERT", "INSTEAD", "INT", "INTEGER",
		"INTERSECT", "INTERVAL", "INTO", "INVOKER", "IS", "ISNULL", "ISOLATION", "JOIN",
		"KEY", "LABEL", "LANGUAGE", "LARGE", "LAST", "LATERAL", "LEADING", "LEAKPROOF",
		"LEFT", "LEVEL", "LIKE", "LIMIT", "LISTEN", "LOAD", "LOCAL", "LOCALTIME",
		"LOCALTIMESTAMP", "LOCATION", "LOCK", "LOCKED", "LOGGED", "MACRO", "MAP",
		"MAPPING", "MATCH", "MATERIALIZED", "MAXVALUE", "METHOD", "MICROSECOND",
		"MICROSECONDS", "MILLISECOND", "MILLISECONDS", "MINUTE", "MINUTES", "MINVALUE",
		"MODE", "MONTH", "MONTHS", "MOVE", "NAME", "NAMES", "NATIONAL", "NATURAL",
		"NCHAR", "NEW", "NEXT", "NO", "NONE", "NOT", "NOTHING", "NOTIFY", "NOTNULL",
		"NOWAIT", "NULL", "NULLIF", "NULLS", "NUMERIC", "OBJECT", "OF", "OFF", "OFFSET",
		"OIDS", "OLD", "ON", "ONLY", "OPERATOR", "OPTION", "OPTIONS", "OR", "ORDER",
		"ORDINALITY", "OUT", "OUTER", "OVER", "OVERLAPS", "OVERLAY", "OVERRIDING",
		"OWNED", "OWNER", "PARALLEL", "PARSER", "PARTIAL", "PARTITION", "PASSING",
		"PASSWORD", "PERCENT", "PLACING", "PLANS", "POLICY", "POSITION", "PRAGMA",
		"PRECEDING", "PRECISION", "PREPARE", "PREPARED", "PRESERVE", "PRIMARY", "PRIOR",
		"PRIVILEGES", "PROCEDURAL", "PROCEDURE", "PROGRAM", "PUBLICATION", "QUOTE",
		"RANGE", "READ", "REAL", "REASSIGN", "RECHECK", "RECURSIVE", "REF", "REFERENCES",
		"REFERENCING", "REFRESH", "REINDEX", "RELATIVE", "RELEASE", "RENAME",
		"REPEATABLE", "REPLACE", "REPLICA", "RESET", "RESTART", "RESTRICT", "RETURNING",
		"RETURNS", "REVOKE", "RIGHT", "ROLE", "ROLLBACK", "ROLLUP", "ROW", "ROWS",
		"RULE", "SAMPLE", "SAVEPOINT", "SCHEMA", "SCHEMAS", "SCROLL", "SEARCH", "SECOND",
		"SECONDS", "SECURITY", "SELECT", "SEQUENCE", "SEQUENCES", "SERIALIZABLE",
		"SERVER", "SESSION", "SESSION_USER", "SET", "SETOF", "SETS", "SHARE", "SHOW",
		"SIMILAR", "SIMPLE", "SKIP", "SMALLINT", "SNAPSHOT", "SOME", "SQL", "STABLE",
		"STANDALONE", "START", "STATEMENT", "STATISTICS", "STDIN", "STDOUT", "STORAGE",
		"STRICT", "STRIP", "STRUCT", "SUBSCRIPTION", "SUBSTRING", "SYMMETRIC", "SYSID",
		"SYSTEM", "TABLE", "TABLES", "TABLESAMPLE", "TABLESPACE", "TEMP", "TEMPLATE",
		"TEMPORARY", "TEXT", "THEN", "TIME", "TIMESTAMP", "TO", "TRAILING", "TRANSACTION",
		"TRANSFORM", "TREAT", "TRIGGER", "TRIM", "TRUE", "TRUNCATE", "TRUSTED", "TRY_CAST",
		"TYPE", "TYPES", "UNBOUNDED", "UNCOMMITTED", "UNENCRYPTED", "UNION", "UNIQUE",
		"UNKNOWN", "UNLISTEN", "UNLOGGED", "UNTIL", "UPDATE", "USER", "USING", "VACUUM",
		"VALID", "VALIDATE", "VALIDATOR", "VALUE", "VALUES", "VARCHAR", "VARIADIC",
		"VARYING", "VERBOSE", "VERSION", "VIEW", "VIEWS", "VOLATILE", "WHEN", "WHERE",
		"WHITESPACE", "WINDOW", "WITH", "WITHIN", "WITHOUT", "WORK", "WRAPPER", "WRITE",
		"XML", "XMLATTRIBUTES", "XMLCONCAT", "XMLELEMENT", "XMLEXISTS", "XMLFOREST",
		"XMLNAMESPACES", "XMLPARSE", "XMLPI", "XMLROOT", "XMLSERIALIZE", "XMLTABLE",
		"YEAR", "YEARS", "YES", "ZONE",
	}

	for _, keyword := range keywords {
		esc.sqlKeywords[keyword] = true
	}

	// Numeric functions
	numericFuncs := []string{
		"ABS", "ACOS", "ASIN", "ATAN", "ATAN2", "CEIL", "CEILING", "COS", "COT",
		"DEGREES", "EXP", "FLOOR", "LOG", "LOG10", "LOG2", "MOD", "PI", "POWER",
		"RADIANS", "RANDOM", "ROUND", "SIGN", "SIN", "SQRT", "TAN", "TRUNCATE",
		"GREATEST", "LEAST", "CBRT", "FACTORIAL", "GAMMA", "LGAMMA", "LN",
		"NEXTAFTER", "POW", "SETSEED", "XOR", "BIT_COUNT", "EVEN",
	}

	for _, fn := range numericFuncs {
		esc.numericFunctions[fn] = true
	}

	// String functions
	stringFuncs := []string{
		"ASCII", "CHAR", "CHAR_LENGTH", "CHARACTER_LENGTH", "CHR", "CONCAT", "CONCAT_WS",
		"CONTAINS", "DIFFERENCE", "FORMAT", "INITCAP", "INSERT", "INSTR", "LCASE",
		"LEFT", "LENGTH", "LIKE", "LOCATE", "LOWER", "LPAD", "LTRIM", "MD5",
		"OCTET_LENGTH", "OVERLAY", "POSITION", "REPEAT", "REPLACE", "REVERSE",
		"RIGHT", "RPAD", "RTRIM", "SOUNDEX", "SPACE", "SPLIT_PART", "STRPOS",
		"SUBSTRING", "TRANSLATE", "TRIM", "UCASE", "UPPER", "BASE64", "FROM_BASE64",
		"TO_BASE64", "REGEXP_MATCHES", "REGEXP_REPLACE", "REGEXP_SPLIT_TO_ARRAY",
		"STRING_SPLIT", "STRING_TO_ARRAY", "ARRAY_TO_STRING", "EDITDIST3",
		"HAMMING", "JACCARD", "LEVENSHTEIN", "MISMATCHES", "NFC_NORMALIZE",
		"STRIP_ACCENTS", "UNICODE", "ORD", "PREFIX", "SUFFIX", "STARTS_WITH",
		"ENDS_WITH", "LIKE_ESCAPE", "NOT_LIKE_ESCAPE",
	}

	for _, fn := range stringFuncs {
		esc.stringFunctions[fn] = true
	}

	// Date/time functions
	dateFuncs := []string{
		"NOW", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP", "LOCALTIME",
		"LOCALTIMESTAMP", "DATE", "TIME", "TIMESTAMP", "EXTRACT", "DATE_PART",
		"DATE_TRUNC", "AGE", "JUSTIFY_DAYS", "JUSTIFY_HOURS", "JUSTIFY_INTERVAL",
		"MAKE_DATE", "MAKE_TIME", "MAKE_TIMESTAMP", "MAKE_TIMESTAMPTZ",
		"TO_TIMESTAMP", "TO_DATE", "EPOCH", "TIMEZONE",
	}

	for _, fn := range dateFuncs {
		esc.dateFunctions[fn] = true
	}

	// System functions
	systemFuncs := []string{
		"VERSION", "USER", "CURRENT_USER", "SESSION_USER", "SYSTEM_USER",
		"DATABASE", "SCHEMA", "CATALOG", "CONNECTION_ID", "LAST_INSERT_ID",
		"ROW_COUNT", "FOUND_ROWS", "UUID", "RANDOM_UUID", "NEWID",
	}

	for _, fn := range systemFuncs {
		esc.systemFunctions[fn] = true
	}
}

// AnalyzeStatement performs comprehensive analysis of a SQL statement.
func (esc *EnterpriseStatementClassifier) AnalyzeStatement(sql string) (*StatementInfo, error) {
	if sql == "" {
		return nil, fmt.Errorf("SQL statement cannot be empty")
	}

	esc.mu.RLock()
	defer esc.mu.RUnlock()

	normalized := strings.TrimSpace(sql)
	upperSQL := strings.ToUpper(normalized)

	info := &StatementInfo{
		Keywords:   esc.extractKeywords(upperSQL),
		Tables:     esc.extractTables(upperSQL),
		Operations: esc.extractOperations(upperSQL),
	}

	// Classify statement type
	info.Type = esc.classifyStatementType(upperSQL)

	// Estimate complexity
	info.Complexity = esc.estimateComplexity(upperSQL)

	// Determine JDBC expectations
	esc.setJDBCExpectations(info)

	// Analyze security risks
	info.IsDangerous = esc.isDangerous(upperSQL)
	info.HasSQLInjectionRisk = esc.hasSQLInjectionRisk(upperSQL)
	info.SecurityRisk = esc.assessSecurityRisk(info)

	// Determine transaction requirements
	info.RequiresTransaction = esc.requiresTransaction(upperSQL, info.Type)

	return info, nil
}

// classifyStatementType determines the statement type using regex patterns.
func (esc *EnterpriseStatementClassifier) classifyStatementType(upperSQL string) StatementType {
	// Check DDL patterns
	for _, pattern := range esc.ddlPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeDDL
		}
	}

	// Check DML patterns
	for _, pattern := range esc.dmlPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeDML
		}
	}

	// Check DQL patterns
	for _, pattern := range esc.dqlPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeDQL
		}
	}

	// Check TCL patterns
	for _, pattern := range esc.tclPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeTCL
		}
	}

	// Check DCL patterns
	for _, pattern := range esc.dclPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeDCL
		}
	}

	// Check utility patterns
	for _, pattern := range esc.utilityPatterns {
		if pattern.MatchString(upperSQL) {
			return StatementTypeUtility
		}
	}

	return StatementTypeOther
}

// estimateComplexity estimates the complexity of a SQL statement.
func (esc *EnterpriseStatementClassifier) estimateComplexity(upperSQL string) StatementComplexity {
	complexityScore := 0

	// Count complexity indicators
	for pattern, weight := range esc.complexityPatterns {
		count := strings.Count(upperSQL, pattern)
		complexityScore += count * weight
	}

	// Additional complexity factors

	// Subqueries (count SELECT occurrences beyond the first)
	selectCount := strings.Count(upperSQL, "SELECT")
	if selectCount > 1 {
		complexityScore += (selectCount - 1) * 3
	}

	// Nested parentheses depth
	maxDepth := esc.calculateParenthesesDepth(upperSQL)
	if maxDepth > 2 {
		complexityScore += (maxDepth - 2) * 2
	}

	// Function calls
	functionCount := esc.countFunctionCalls(upperSQL)
	complexityScore += functionCount / 2

	// Determine complexity level
	switch {
	case complexityScore == 0:
		return ComplexitySimple
	case complexityScore <= 3:
		return ComplexityModerate
	case complexityScore <= 8:
		return ComplexityComplex
	default:
		return ComplexityVeryComplex
	}
}

// setJDBCExpectations sets JDBC-specific expectations based on statement type.
func (esc *EnterpriseStatementClassifier) setJDBCExpectations(info *StatementInfo) {
	switch info.Type {
	case StatementTypeDDL, StatementTypeDML:
		info.ExpectsUpdateCount = true
		info.ExpectsResultSet = false
		info.IsReadOnly = false
	case StatementTypeDQL:
		info.ExpectsUpdateCount = false
		info.ExpectsResultSet = true
		info.IsReadOnly = true
	case StatementTypeTCL:
		info.ExpectsUpdateCount = true
		info.ExpectsResultSet = false
		info.IsReadOnly = false
	case StatementTypeDCL:
		info.ExpectsUpdateCount = true
		info.ExpectsResultSet = false
		info.IsReadOnly = false
	case StatementTypeUtility:
		info.ExpectsUpdateCount = false
		info.ExpectsResultSet = true
		info.IsReadOnly = true
	default:
		info.ExpectsUpdateCount = false
		info.ExpectsResultSet = true
		info.IsReadOnly = true
	}
}

// isDangerous checks if a statement contains dangerous operations.
func (esc *EnterpriseStatementClassifier) isDangerous(upperSQL string) bool {
	for _, pattern := range esc.dangerousPatterns {
		if pattern.MatchString(upperSQL) {
			return true
		}
	}
	return false
}

// hasSQLInjectionRisk checks for SQL injection patterns.
func (esc *EnterpriseStatementClassifier) hasSQLInjectionRisk(upperSQL string) bool {
	for _, pattern := range esc.injectionPatterns {
		if pattern.MatchString(upperSQL) {
			return true
		}
	}
	return false
}

// assessSecurityRisk determines the overall security risk level.
func (esc *EnterpriseStatementClassifier) assessSecurityRisk(info *StatementInfo) SecurityRiskLevel {
	if info.HasSQLInjectionRisk {
		return SecurityRiskCritical
	}

	if info.IsDangerous {
		return SecurityRiskHigh
	}

	switch info.Type {
	case StatementTypeDCL:
		return SecurityRiskHigh
	case StatementTypeDDL:
		return SecurityRiskMedium
	case StatementTypeDML:
		return SecurityRiskLow
	case StatementTypeTCL:
		return SecurityRiskLow
	default:
		return SecurityRiskNone
	}
}

// requiresTransaction determines if a statement requires a transaction context.
func (esc *EnterpriseStatementClassifier) requiresTransaction(upperSQL string, stmtType StatementType) bool {
	// DML always requires transactions
	if stmtType == StatementTypeDML {
		return true
	}

	// Some DDL operations might require transactions
	if stmtType == StatementTypeDDL {
		transactionRequiredPatterns := []string{
			"CREATE.*INDEX",
			"DROP.*INDEX",
			"ALTER.*TABLE.*ADD.*CONSTRAINT",
			"ALTER.*TABLE.*DROP.*CONSTRAINT",
		}

		for _, pattern := range transactionRequiredPatterns {
			matched, _ := regexp.MatchString(pattern, upperSQL)
			if matched {
				return true
			}
		}
	}

	return false
}

// extractKeywords extracts SQL keywords from the statement.
func (esc *EnterpriseStatementClassifier) extractKeywords(upperSQL string) []string {
	words := regexp.MustCompile(`\b\w+\b`).FindAllString(upperSQL, -1)
	var keywords []string

	for _, word := range words {
		if esc.sqlKeywords[word] {
			keywords = append(keywords, word)
		}
	}

	return keywords
}

// extractTables attempts to extract table names from the statement.
func (esc *EnterpriseStatementClassifier) extractTables(upperSQL string) []string {
	var tables []string

	// Simple table extraction patterns
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`(?i)FROM\s+(\w+)`),
		regexp.MustCompile(`(?i)JOIN\s+(\w+)`),
		regexp.MustCompile(`(?i)UPDATE\s+(\w+)`),
		regexp.MustCompile(`(?i)INSERT\s+INTO\s+(\w+)`),
		regexp.MustCompile(`(?i)DELETE\s+FROM\s+(\w+)`),
		regexp.MustCompile(`(?i)CREATE\s+TABLE\s+(\w+)`),
		regexp.MustCompile(`(?i)DROP\s+TABLE\s+(\w+)`),
		regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(\w+)`),
		regexp.MustCompile(`(?i)TRUNCATE\s+TABLE\s+(\w+)`),
	}

	for _, pattern := range patterns {
		matches := pattern.FindAllStringSubmatch(upperSQL, -1)
		for _, match := range matches {
			if len(match) > 1 {
				tables = append(tables, match[1])
			}
		}
	}

	return tables
}

// extractOperations extracts the main operations from the statement.
func (esc *EnterpriseStatementClassifier) extractOperations(upperSQL string) []string {
	var operations []string

	operationPatterns := map[string]*regexp.Regexp{
		"CREATE":   regexp.MustCompile(`(?i)\bCREATE\b`),
		"DROP":     regexp.MustCompile(`(?i)\bDROP\b`),
		"ALTER":    regexp.MustCompile(`(?i)\bALTER\b`),
		"INSERT":   regexp.MustCompile(`(?i)\bINSERT\b`),
		"UPDATE":   regexp.MustCompile(`(?i)\bUPDATE\b`),
		"DELETE":   regexp.MustCompile(`(?i)\bDELETE\b`),
		"SELECT":   regexp.MustCompile(`(?i)\bSELECT\b`),
		"JOIN":     regexp.MustCompile(`(?i)\bJOIN\b`),
		"UNION":    regexp.MustCompile(`(?i)\bUNION\b`),
		"GROUP BY": regexp.MustCompile(`(?i)\bGROUP\s+BY\b`),
		"ORDER BY": regexp.MustCompile(`(?i)\bORDER\s+BY\b`),
		"HAVING":   regexp.MustCompile(`(?i)\bHAVING\b`),
		"WHERE":    regexp.MustCompile(`(?i)\bWHERE\b`),
	}

	for operation, pattern := range operationPatterns {
		if pattern.MatchString(upperSQL) {
			operations = append(operations, operation)
		}
	}

	return operations
}

// calculateParenthesesDepth calculates the maximum nesting depth of parentheses.
func (esc *EnterpriseStatementClassifier) calculateParenthesesDepth(sql string) int {
	maxDepth := 0
	currentDepth := 0
	inString := false
	var stringChar rune

	for _, char := range sql {
		if !inString {
			if char == '\'' || char == '"' {
				inString = true
				stringChar = char
			} else if char == '(' {
				currentDepth++
				if currentDepth > maxDepth {
					maxDepth = currentDepth
				}
			} else if char == ')' {
				currentDepth--
			}
		} else {
			if char == stringChar {
				inString = false
			}
		}
	}

	return maxDepth
}

// countFunctionCalls counts the number of function calls in the statement.
func (esc *EnterpriseStatementClassifier) countFunctionCalls(upperSQL string) int {
	functionPattern := regexp.MustCompile(`\b\w+\s*\(`)
	matches := functionPattern.FindAllString(upperSQL, -1)
	return len(matches)
}

// ValidateStatement performs basic SQL statement validation.
func (esc *EnterpriseStatementClassifier) ValidateStatement(sql string) error {
	if sql == "" {
		return fmt.Errorf("SQL statement cannot be empty")
	}

	normalized := strings.TrimSpace(sql)
	if normalized == "" {
		return fmt.Errorf("SQL statement contains only whitespace")
	}

	// Check for balanced parentheses
	if !esc.hasBalancedParentheses(sql) {
		return fmt.Errorf("SQL statement has unbalanced parentheses")
	}

	// Check for balanced quotes
	if !esc.hasBalancedQuotes(sql) {
		return fmt.Errorf("SQL statement has unbalanced quotes")
	}

	return nil
}

// hasBalancedParentheses checks if parentheses are balanced.
func (esc *EnterpriseStatementClassifier) hasBalancedParentheses(sql string) bool {
	count := 0
	inString := false
	var stringChar rune

	for _, char := range sql {
		if !inString {
			if char == '\'' || char == '"' {
				inString = true
				stringChar = char
			} else if char == '(' {
				count++
			} else if char == ')' {
				count--
				if count < 0 {
					return false
				}
			}
		} else {
			if char == stringChar {
				inString = false
			}
		}
	}

	return count == 0
}

// hasBalancedQuotes checks if quotes are balanced.
func (esc *EnterpriseStatementClassifier) hasBalancedQuotes(sql string) bool {
	singleQuoteCount := 0
	doubleQuoteCount := 0

	for _, char := range sql {
		if char == '\'' {
			singleQuoteCount++
		} else if char == '"' {
			doubleQuoteCount++
		}
	}

	return singleQuoteCount%2 == 0 && doubleQuoteCount%2 == 0
}

// Legacy methods for backward compatibility

// ClassifyStatement determines the type of a SQL statement (legacy method for compatibility).
func (esc *EnterpriseStatementClassifier) ClassifyStatement(sql string) StatementType {
	info, err := esc.AnalyzeStatement(sql)
	if err != nil {
		return StatementTypeOther
	}
	return info.Type
}

// IsUpdateStatement returns true if the statement is a DDL or DML statement
// that should return an update count rather than a result set.
func (esc *EnterpriseStatementClassifier) IsUpdateStatement(sql string) bool {
	stmtType := esc.ClassifyStatement(sql)
	return stmtType == StatementTypeDDL || stmtType == StatementTypeDML
}

// IsQueryStatement returns true if the statement is a DQL statement
// that should return a result set.
func (esc *EnterpriseStatementClassifier) IsQueryStatement(sql string) bool {
	return esc.ClassifyStatement(sql) == StatementTypeDQL
}

// GetExpectedResponseType returns the expected response type for a statement.
func (esc *EnterpriseStatementClassifier) GetExpectedResponseType(sql string) string {
	stmtType := esc.ClassifyStatement(sql)
	switch stmtType {
	case StatementTypeDDL, StatementTypeDML, StatementTypeTCL, StatementTypeDCL:
		return "UPDATE_COUNT"
	case StatementTypeDQL, StatementTypeUtility:
		return "RESULT_SET"
	default:
		return "RESULT_SET"
	}
}

// GetStatementType returns the statement type (alias for ClassifyStatement).
func (esc *EnterpriseStatementClassifier) GetStatementType(sql string) StatementType {
	return esc.ClassifyStatement(sql)
}

// NewStatementClassifier creates a new statement classifier (legacy compatibility).
func NewStatementClassifier() *EnterpriseStatementClassifier {
	return NewEnterpriseStatementClassifier()
}

// StatementClassifier type alias for backward compatibility.
type StatementClassifier = EnterpriseStatementClassifier
