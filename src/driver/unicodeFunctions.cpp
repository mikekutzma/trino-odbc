/*
 * Unicode (W-suffix) ODBC entry points.
 *
 * The Windows Driver Manager prefers W-suffix functions when connecting from
 * Unicode-aware applications such as PowerBI.  Each function here is a thin
 * shim that:
 *   - converts SQLWCHAR* (UTF-16LE) inputs to std::string (UTF-8/ANSI), then
 *     calls the corresponding ANSI entry point, and
 *   - converts any SQLCHAR* string outputs back to SQLWCHAR* (UTF-16LE).
 *
 * Non-string arguments (handles, bitmasks, integers) are passed through as-is
 * — they have no encoding.
 */

#include "../util/windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include <string>
#include <vector>

#include "../util/writeLog.hpp"

// ---------------------------------------------------------------------------
// Internal conversion helpers
// ---------------------------------------------------------------------------

// Convert a SQLWCHAR* (UTF-16LE on Windows) to a UTF-8 std::string.
// length is the number of SQLWCHAR characters; SQL_NTS means null-terminated.
static std::string wideToUtf8(const SQLWCHAR* src, SQLINTEGER length) {
  if (!src) return {};
  int wlen = 0;
  if (length == SQL_NTS) {
    while (src[wlen]) ++wlen;
  } else {
    wlen = static_cast<int>(length);
  }
  if (wlen == 0) return {};
  int needed = WideCharToMultiByte(CP_UTF8, 0,
                                   reinterpret_cast<LPCWSTR>(src), wlen,
                                   nullptr, 0, nullptr, nullptr);
  std::string out(needed, '\0');
  WideCharToMultiByte(CP_UTF8, 0,
                      reinterpret_cast<LPCWSTR>(src), wlen,
                      out.data(), needed, nullptr, nullptr);
  return out;
}

// Copy a UTF-8 string into a SQLWCHAR* output buffer.
// bufferLenBytes is the size of the destination buffer in bytes.
// *writtenBytes receives the number of bytes written (not counting the null).
static void utf8ToWideBuffer(const char* src,
                             SQLSMALLINT srcLen,
                             SQLWCHAR* dest,
                             SQLSMALLINT bufferLenBytes,
                             SQLSMALLINT* writtenBytes) {
  if (!src || srcLen <= 0) {
    if (dest && bufferLenBytes >= static_cast<SQLSMALLINT>(sizeof(SQLWCHAR)))
      dest[0] = 0;
    if (writtenBytes) *writtenBytes = 0;
    return;
  }
  int wcharSlots = bufferLenBytes > 0
                       ? (bufferLenBytes / static_cast<int>(sizeof(SQLWCHAR)) - 1)
                       : 0;
  int written = 0;
  if (dest && wcharSlots > 0) {
    written = MultiByteToWideChar(CP_UTF8, 0, src, srcLen,
                                  reinterpret_cast<LPWSTR>(dest), wcharSlots);
    dest[written] = 0; // null-terminate
  } else {
    // Compute required size only.
    written = MultiByteToWideChar(CP_UTF8, 0, src, srcLen, nullptr, 0);
  }
  if (writtenBytes)
    *writtenBytes = static_cast<SQLSMALLINT>(written * sizeof(SQLWCHAR));
}

// ---------------------------------------------------------------------------
// SQLDriverConnectW
// ---------------------------------------------------------------------------

// Forward declarations for the ANSI functions we delegate to.
SQLRETURN SQL_API SQLDriverConnect(SQLHDBC ConnectionHandle,
                                   SQLHWND WindowHandle,
                                   SQLCHAR* InConnectionString,
                                   SQLSMALLINT StringLength1,
                                   SQLCHAR* OutConnectionString,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLength2Ptr,
                                   SQLUSMALLINT DriverCompletion);

SQLRETURN SQL_API SQLDriverConnectW(SQLHDBC ConnectionHandle,
                                    SQLHWND WindowHandle,
                                    SQLWCHAR* InConnectionString,
                                    SQLSMALLINT StringLength1,
                                    SQLWCHAR* OutConnectionString,
                                    SQLSMALLINT BufferLength,
                                    SQLSMALLINT* StringLength2Ptr,
                                    SQLUSMALLINT DriverCompletion) {
  WriteLog(LL_TRACE, "Entering SQLDriverConnectW");
  std::string inStr = wideToUtf8(InConnectionString, StringLength1);

  // Allocate a temporary narrow buffer for the output connection string.
  SQLSMALLINT ansiOutLen = 0;
  std::vector<char> ansiOut(BufferLength > 0 ? BufferLength : 1024, 0);

  SQLRETURN ret = SQLDriverConnect(ConnectionHandle,
                                   WindowHandle,
                                   reinterpret_cast<SQLCHAR*>(inStr.data()),
                                   static_cast<SQLSMALLINT>(inStr.size()),
                                   reinterpret_cast<SQLCHAR*>(ansiOut.data()),
                                   static_cast<SQLSMALLINT>(ansiOut.size()),
                                   &ansiOutLen,
                                   DriverCompletion);

  if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
    utf8ToWideBuffer(ansiOut.data(), ansiOutLen, OutConnectionString,
                     BufferLength, StringLength2Ptr);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// SQLExecDirectW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLExecDirect(SQLHSTMT StatementHandle,
                                SQLCHAR* StatementText,
                                SQLINTEGER TextLength);

SQLRETURN SQL_API SQLExecDirectW(SQLHSTMT StatementHandle,
                                 SQLWCHAR* StatementText,
                                 SQLINTEGER TextLength) {
  WriteLog(LL_TRACE, "Entering SQLExecDirectW");
  std::string sql = wideToUtf8(StatementText, TextLength);
  return SQLExecDirect(StatementHandle,
                       reinterpret_cast<SQLCHAR*>(sql.data()),
                       static_cast<SQLINTEGER>(sql.size()));
}

// ---------------------------------------------------------------------------
// SQLPrepareW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle,
                             SQLCHAR* StatementText,
                             SQLINTEGER TextLength);

SQLRETURN SQL_API SQLPrepareW(SQLHSTMT StatementHandle,
                              SQLWCHAR* StatementText,
                              SQLINTEGER TextLength) {
  WriteLog(LL_TRACE, "Entering SQLPrepareW");
  std::string sql = wideToUtf8(StatementText, TextLength);
  return SQLPrepare(StatementHandle,
                    reinterpret_cast<SQLCHAR*>(sql.data()),
                    static_cast<SQLINTEGER>(sql.size()));
}

// ---------------------------------------------------------------------------
// SQLGetInfoW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLGetInfo(SQLHDBC ConnectionHandle,
                             SQLUSMALLINT InfoType,
                             SQLPOINTER InfoValue,
                             SQLSMALLINT BufferLength,
                             SQLSMALLINT* StringLengthPtr);

// InfoTypes for which SQLGetInfo writes a null-terminated ANSI string.
static bool isStringInfoType(SQLUSMALLINT t) {
  switch (t) {
    case SQL_DRIVER_NAME:                 // 6
    case SQL_DRIVER_VER:                  // 7
    case SQL_SEARCH_PATTERN_ESCAPE:       // 14
    case SQL_DBMS_NAME:                   // 17
    case SQL_DBMS_VER:                    // 18
    case SQL_ACCESSIBLE_TABLES:           // 19
    case SQL_IDENTIFIER_QUOTE_CHAR:       // 29
    case SQL_OWNER_TERM:                  // 39 (= SQL_SCHEMA_TERM)
    case SQL_QUALIFIER_NAME_SEPARATOR:    // 41 (= SQL_CATALOG_NAME_SEPARATOR)
    case SQL_CATALOG_TERM:                // 42
    case SQL_TABLE_TERM:                  // 45
    case SQL_USER_NAME:                   // 47
    case SQL_DRIVER_ODBC_VER:             // 77
    case SQL_COLUMN_ALIAS:                // 87
    case SQL_ORDER_BY_COLUMNS_IN_SELECT:  // 90
    case SQL_SPECIAL_CHARACTERS:          // 94
    case SQL_KEYWORDS:                    // 89
    case SQL_DATABASE_NAME:               // 16
    case SQL_SERVER_NAME:                 // 13
    case SQL_CATALOG_NAME:                // 10003
      return true;
    default:
      return false;
  }
}

SQLRETURN SQL_API SQLGetInfoW(SQLHDBC ConnectionHandle,
                              SQLUSMALLINT InfoType,
                              SQLPOINTER InfoValuePtr,
                              SQLSMALLINT BufferLength,
                              SQLSMALLINT* StringLengthPtr) {
  WriteLog(LL_TRACE,
           "Entering SQLGetInfoW, InfoType=" + std::to_string(InfoType));

  if (!isStringInfoType(InfoType)) {
    // Numeric output — delegate directly; no conversion needed.
    return SQLGetInfo(ConnectionHandle, InfoType, InfoValuePtr, BufferLength,
                      StringLengthPtr);
  }

  // String output: use a temporary ANSI buffer.
  SQLSMALLINT ansiLen = 0;
  std::vector<char> ansiBuf(1024, 0);
  SQLRETURN ret = SQLGetInfo(ConnectionHandle, InfoType, ansiBuf.data(),
                             static_cast<SQLSMALLINT>(ansiBuf.size()),
                             &ansiLen);
  if (ret == SQL_ERROR) return ret;

  utf8ToWideBuffer(ansiBuf.data(), ansiLen,
                   reinterpret_cast<SQLWCHAR*>(InfoValuePtr),
                   BufferLength, StringLengthPtr);
  return ret;
}

// ---------------------------------------------------------------------------
// SQLColAttributeW
// ---------------------------------------------------------------------------

#if defined(_WIN64)
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber,
                                  SQLUSMALLINT FieldIdentifier,
                                  SQLPOINTER CharacterAttributePtr,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT* StringLengthPtr,
                                  SQLLEN* NumericAttributePtr);

SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ColumnNumber,
                                   SQLUSMALLINT FieldIdentifier,
                                   SQLPOINTER CharacterAttributePtr,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLengthPtr,
                                   SQLLEN* NumericAttributePtr) {
#else
SQLRETURN SQL_API SQLColAttribute(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber,
                                  SQLUSMALLINT FieldIdentifier,
                                  SQLPOINTER CharacterAttributePtr,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT* StringLengthPtr,
                                  SQLPOINTER NumericAttributePtr);

SQLRETURN SQL_API SQLColAttributeW(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ColumnNumber,
                                   SQLUSMALLINT FieldIdentifier,
                                   SQLPOINTER CharacterAttributePtr,
                                   SQLSMALLINT BufferLength,
                                   SQLSMALLINT* StringLengthPtr,
                                   SQLPOINTER NumericAttributePtr) {
#endif
  WriteLog(LL_TRACE, "Entering SQLColAttributeW");

  // Use a temp narrow buffer for the character attribute.
  SQLSMALLINT ansiLen = 0;
  std::vector<char> ansiBuf(BufferLength > 0 ? BufferLength : 512, 0);

  SQLRETURN ret = SQLColAttribute(StatementHandle, ColumnNumber, FieldIdentifier,
                                  ansiBuf.data(),
                                  static_cast<SQLSMALLINT>(ansiBuf.size()),
                                  &ansiLen,
                                  NumericAttributePtr);

  if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) && ansiLen > 0) {
    utf8ToWideBuffer(ansiBuf.data(), ansiLen,
                     reinterpret_cast<SQLWCHAR*>(CharacterAttributePtr),
                     BufferLength, StringLengthPtr);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// SQLDescribeColW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLDescribeCol(SQLHSTMT StatementHandle,
                                 SQLUSMALLINT ColumnNumber,
                                 SQLCHAR* ColumnName,
                                 SQLSMALLINT BufferLength,
                                 SQLSMALLINT* NameLength,
                                 SQLSMALLINT* DataType,
                                 SQLULEN* ColumnSize,
                                 SQLSMALLINT* DecimalDigits,
                                 SQLSMALLINT* Nullable);

SQLRETURN SQL_API SQLDescribeColW(SQLHSTMT StatementHandle,
                                  SQLUSMALLINT ColumnNumber,
                                  SQLWCHAR* ColumnName,
                                  SQLSMALLINT BufferLength,
                                  SQLSMALLINT* NameLength,
                                  SQLSMALLINT* DataType,
                                  SQLULEN* ColumnSize,
                                  SQLSMALLINT* DecimalDigits,
                                  SQLSMALLINT* Nullable) {
  WriteLog(LL_TRACE, "Entering SQLDescribeColW");

  SQLSMALLINT ansiNameLen = 0;
  // BufferLength for the W version is in bytes; use half as narrow char count.
  SQLSMALLINT narrowBufLen =
      BufferLength > 0 ? BufferLength / static_cast<SQLSMALLINT>(sizeof(SQLWCHAR))
                       : 256;
  std::vector<char> nameBuf(narrowBufLen + 1, 0);

  SQLRETURN ret = SQLDescribeCol(StatementHandle, ColumnNumber,
                                 reinterpret_cast<SQLCHAR*>(nameBuf.data()),
                                 static_cast<SQLSMALLINT>(nameBuf.size()),
                                 &ansiNameLen, DataType, ColumnSize,
                                 DecimalDigits, Nullable);

  if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
    utf8ToWideBuffer(nameBuf.data(), ansiNameLen, ColumnName, BufferLength,
                     NameLength);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// SQLGetDiagRecW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLGetDiagRec(SQLSMALLINT HandleType,
                                SQLHANDLE Handle,
                                SQLSMALLINT RecNumber,
                                SQLCHAR* SqlState,
                                SQLINTEGER* NativeError,
                                SQLCHAR* MessageText,
                                SQLSMALLINT BufferLength,
                                SQLSMALLINT* TextLength);

SQLRETURN SQL_API SQLGetDiagRecW(SQLSMALLINT HandleType,
                                 SQLHANDLE Handle,
                                 SQLSMALLINT RecNumber,
                                 SQLWCHAR* SqlState,
                                 SQLINTEGER* NativeError,
                                 SQLWCHAR* MessageText,
                                 SQLSMALLINT BufferLength,
                                 SQLSMALLINT* TextLength) {
  WriteLog(LL_TRACE, "Entering SQLGetDiagRecW");

  // SQLState is always 5 characters + null.
  char ansiState[6]   = {};
  SQLSMALLINT msgLen  = 0;
  SQLSMALLINT narrowBufLen =
      BufferLength > 0 ? BufferLength / static_cast<SQLSMALLINT>(sizeof(SQLWCHAR))
                       : 1024;
  std::vector<char> msgBuf(narrowBufLen + 1, 0);

  SQLRETURN ret = SQLGetDiagRec(HandleType, Handle, RecNumber,
                                reinterpret_cast<SQLCHAR*>(ansiState),
                                NativeError,
                                reinterpret_cast<SQLCHAR*>(msgBuf.data()),
                                static_cast<SQLSMALLINT>(msgBuf.size()),
                                &msgLen);

  if (ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) {
    // Convert the 5-char SQL state (always ASCII).
    if (SqlState) {
      for (int i = 0; i < 6; ++i)
        SqlState[i] = static_cast<SQLWCHAR>(ansiState[i]);
    }
    utf8ToWideBuffer(msgBuf.data(), msgLen, MessageText, BufferLength,
                     TextLength);
  }
  return ret;
}

// ---------------------------------------------------------------------------
// SQLTablesW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLTables(SQLHSTMT StatementHandle,
                            SQLCHAR* CatalogName,
                            SQLSMALLINT NameLength1,
                            SQLCHAR* SchemaName,
                            SQLSMALLINT NameLength2,
                            SQLCHAR* TableName,
                            SQLSMALLINT NameLength3,
                            SQLCHAR* TableType,
                            SQLSMALLINT NameLength4);

SQLRETURN SQL_API SQLTablesW(SQLHSTMT StatementHandle,
                             SQLWCHAR* CatalogName,
                             SQLSMALLINT NameLength1,
                             SQLWCHAR* SchemaName,
                             SQLSMALLINT NameLength2,
                             SQLWCHAR* TableName,
                             SQLSMALLINT NameLength3,
                             SQLWCHAR* TableType,
                             SQLSMALLINT NameLength4) {
  WriteLog(LL_TRACE, "Entering SQLTablesW");
  std::string catalog   = wideToUtf8(CatalogName, NameLength1);
  std::string schema    = wideToUtf8(SchemaName, NameLength2);
  std::string table     = wideToUtf8(TableName, NameLength3);
  std::string tableType = wideToUtf8(TableType, NameLength4);

  return SQLTables(StatementHandle,
                   catalog.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(catalog.data()),
                   static_cast<SQLSMALLINT>(catalog.size()),
                   schema.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(schema.data()),
                   static_cast<SQLSMALLINT>(schema.size()),
                   table.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(table.data()),
                   static_cast<SQLSMALLINT>(table.size()),
                   tableType.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(tableType.data()),
                   static_cast<SQLSMALLINT>(tableType.size()));
}

// ---------------------------------------------------------------------------
// SQLColumnsW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLColumns(SQLHSTMT StatementHandle,
                             SQLCHAR* CatalogName,
                             SQLSMALLINT NameLength1,
                             SQLCHAR* SchemaName,
                             SQLSMALLINT NameLength2,
                             SQLCHAR* TableName,
                             SQLSMALLINT NameLength3,
                             SQLCHAR* ColumnName,
                             SQLSMALLINT NameLength4);

SQLRETURN SQL_API SQLColumnsW(SQLHSTMT StatementHandle,
                              SQLWCHAR* CatalogName,
                              SQLSMALLINT NameLength1,
                              SQLWCHAR* SchemaName,
                              SQLSMALLINT NameLength2,
                              SQLWCHAR* TableName,
                              SQLSMALLINT NameLength3,
                              SQLWCHAR* ColumnName,
                              SQLSMALLINT NameLength4) {
  WriteLog(LL_TRACE, "Entering SQLColumnsW");
  std::string catalog = wideToUtf8(CatalogName, NameLength1);
  std::string schema  = wideToUtf8(SchemaName, NameLength2);
  std::string table   = wideToUtf8(TableName, NameLength3);
  std::string column  = wideToUtf8(ColumnName, NameLength4);

  return SQLColumns(StatementHandle,
                    catalog.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(catalog.data()),
                    static_cast<SQLSMALLINT>(catalog.size()),
                    schema.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(schema.data()),
                    static_cast<SQLSMALLINT>(schema.size()),
                    table.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(table.data()),
                    static_cast<SQLSMALLINT>(table.size()),
                    column.empty() ? nullptr : reinterpret_cast<SQLCHAR*>(column.data()),
                    static_cast<SQLSMALLINT>(column.size()));
}

// ---------------------------------------------------------------------------
// SQLGetConnectAttrW / SQLSetConnectAttrW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLGetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute,
                                    SQLPOINTER ValuePtr,
                                    SQLINTEGER BufferLength,
                                    SQLINTEGER* StringLengthPtr);

SQLRETURN SQL_API SQLGetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute,
                                     SQLPOINTER ValuePtr,
                                     SQLINTEGER BufferLength,
                                     SQLINTEGER* StringLengthPtr) {
  WriteLog(LL_TRACE, "Entering SQLGetConnectAttrW");
  // Most connection attributes are numeric. Pass through directly.
  return SQLGetConnectAttr(ConnectionHandle, Attribute, ValuePtr, BufferLength,
                           StringLengthPtr);
}

SQLRETURN SQL_API SQLSetConnectAttr(SQLHDBC ConnectionHandle,
                                    SQLINTEGER Attribute,
                                    SQLPOINTER ValuePtr,
                                    SQLINTEGER StringLength);

SQLRETURN SQL_API SQLSetConnectAttrW(SQLHDBC ConnectionHandle,
                                     SQLINTEGER Attribute,
                                     SQLPOINTER ValuePtr,
                                     SQLINTEGER StringLength) {
  WriteLog(LL_TRACE, "Entering SQLSetConnectAttrW");
  // Most connection attributes are numeric. For string attributes, the value
  // would need conversion, but the current driver's SQLSetConnectAttr does not
  // handle any string-valued attributes, so pass through as-is.
  return SQLSetConnectAttr(ConnectionHandle, Attribute, ValuePtr, StringLength);
}

// ---------------------------------------------------------------------------
// SQLGetStmtAttrW / SQLSetStmtAttrW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLGetStmtAttr(SQLHSTMT StatementHandle,
                                 SQLINTEGER Attribute,
                                 SQLPOINTER ValuePtr,
                                 SQLINTEGER BufferLength,
                                 SQLINTEGER* StringLengthPtr);

SQLRETURN SQL_API SQLGetStmtAttrW(SQLHSTMT StatementHandle,
                                  SQLINTEGER Attribute,
                                  SQLPOINTER ValuePtr,
                                  SQLINTEGER BufferLength,
                                  SQLINTEGER* StringLengthPtr) {
  WriteLog(LL_TRACE, "Entering SQLGetStmtAttrW");
  return SQLGetStmtAttr(StatementHandle, Attribute, ValuePtr, BufferLength,
                        StringLengthPtr);
}

SQLRETURN SQL_API SQLSetStmtAttr(SQLHSTMT StatementHandle,
                                 SQLINTEGER Attribute,
                                 SQLPOINTER ValuePtr,
                                 SQLINTEGER StringLength);

SQLRETURN SQL_API SQLSetStmtAttrW(SQLHSTMT StatementHandle,
                                  SQLINTEGER Attribute,
                                  SQLPOINTER ValuePtr,
                                  SQLINTEGER StringLength) {
  WriteLog(LL_TRACE, "Entering SQLSetStmtAttrW");
  return SQLSetStmtAttr(StatementHandle, Attribute, ValuePtr, StringLength);
}

// ---------------------------------------------------------------------------
// SQLNativeSqlW
// ---------------------------------------------------------------------------

SQLRETURN SQL_API SQLNativeSql(SQLHDBC ConnectionHandle,
                               SQLCHAR* InStatementText,
                               SQLINTEGER TextLength1,
                               SQLCHAR* OutStatementText,
                               SQLINTEGER BufferLength,
                               SQLINTEGER* TextLength2Ptr);

SQLRETURN SQL_API SQLNativeSqlW(SQLHDBC ConnectionHandle,
                                SQLWCHAR* InStatementText,
                                SQLINTEGER TextLength1,
                                SQLWCHAR* OutStatementText,
                                SQLINTEGER BufferLength,
                                SQLINTEGER* TextLength2Ptr) {
  WriteLog(LL_TRACE, "Entering SQLNativeSqlW");
  std::string inSql = wideToUtf8(InStatementText, TextLength1);

  SQLINTEGER ansiOutLen = 0;
  SQLINTEGER narrowBufLen =
      BufferLength > 0 ? BufferLength / static_cast<SQLINTEGER>(sizeof(SQLWCHAR))
                       : 4096;
  std::vector<char> outBuf(narrowBufLen + 1, 0);

  SQLRETURN ret = SQLNativeSql(ConnectionHandle,
                               reinterpret_cast<SQLCHAR*>(inSql.data()),
                               static_cast<SQLINTEGER>(inSql.size()),
                               reinterpret_cast<SQLCHAR*>(outBuf.data()),
                               static_cast<SQLINTEGER>(outBuf.size()),
                               &ansiOutLen);

  if ((ret == SQL_SUCCESS || ret == SQL_SUCCESS_WITH_INFO) && ansiOutLen > 0) {
    SQLSMALLINT wOut = 0;
    utf8ToWideBuffer(outBuf.data(), static_cast<SQLSMALLINT>(ansiOutLen),
                     OutStatementText,
                     static_cast<SQLSMALLINT>(BufferLength), &wOut);
    if (TextLength2Ptr)
      *TextLength2Ptr = static_cast<SQLINTEGER>(wOut);
  }
  return ret;
}
