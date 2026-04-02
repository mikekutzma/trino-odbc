#include "../util/windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include <sstream>
#include <stdexcept>
#include <string>

#include "../trinoAPIWrapper/trinoQuery.hpp"
#include "../util/writeLog.hpp"
#include "handles/descriptorHandle.hpp"
#include "handles/statementHandle.hpp"

// Format a single bound parameter as a Trino SQL literal.
// Returns a SQL fragment suitable for direct embedding in a query string.
static std::string formatParam(const DescriptorField& field) {
  // Null check via the indicator pointer.
  if (field.bufferStrLenOrIndPtr &&
      *field.bufferStrLenOrIndPtr == SQL_NULL_DATA) {
    return "NULL";
  }
  if (!field.bufferPtr) {
    return "NULL";
  }

  switch (field.bufferCDataType) {
    case SQL_C_CHAR: {
      // Escape any single quotes by doubling them.
      const char* raw = static_cast<const char*>(field.bufferPtr);
      SQLLEN len       = field.bufferStrLenOrIndPtr
                             ? *field.bufferStrLenOrIndPtr
                             : SQL_NTS;
      std::string s    = (len == SQL_NTS) ? std::string(raw)
                                          : std::string(raw, static_cast<size_t>(len));
      std::string escaped;
      escaped.reserve(s.size() + 2);
      for (char c : s) {
        if (c == '\'') escaped += '\'';
        escaped += c;
      }
      return "'" + escaped + "'";
    }
    case SQL_C_WCHAR: {
      // Convert UTF-16LE to UTF-8 using Windows API, then treat like SQL_C_CHAR.
      const SQLWCHAR* raw = static_cast<const SQLWCHAR*>(field.bufferPtr);
      SQLLEN len           = field.bufferStrLenOrIndPtr
                                 ? *field.bufferStrLenOrIndPtr
                                 : SQL_NTS;
      int wlen = 0;
      if (len == SQL_NTS) {
        while (raw[wlen]) wlen++;
      } else {
        wlen = static_cast<int>(len / sizeof(SQLWCHAR));
      }
      int needed = WideCharToMultiByte(CP_UTF8, 0,
                                       reinterpret_cast<LPCWCH>(raw), wlen,
                                       nullptr, 0, nullptr, nullptr);
      std::string s(needed, '\0');
      WideCharToMultiByte(CP_UTF8, 0,
                          reinterpret_cast<LPCWCH>(raw), wlen,
                          s.data(), needed, nullptr, nullptr);
      std::string escaped;
      escaped.reserve(s.size() + 2);
      for (char c : s) {
        if (c == '\'') escaped += '\'';
        escaped += c;
      }
      return "'" + escaped + "'";
    }
    case SQL_C_SLONG: {  // SQL_C_LONG is an alias for SQL_C_SLONG
      SQLINTEGER val = *static_cast<const SQLINTEGER*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_ULONG: {
      SQLUINTEGER val = *static_cast<const SQLUINTEGER*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_SSHORT: {  // SQL_C_SHORT is an alias for SQL_C_SSHORT
      SQLSMALLINT val = *static_cast<const SQLSMALLINT*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_USHORT: {
      SQLUSMALLINT val = *static_cast<const SQLUSMALLINT*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_SBIGINT: {
      SQLBIGINT val = *static_cast<const SQLBIGINT*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_UBIGINT: {
      SQLUBIGINT val = *static_cast<const SQLUBIGINT*>(field.bufferPtr);
      return std::to_string(val);
    }
    case SQL_C_FLOAT: {
      SQLREAL val = *static_cast<const SQLREAL*>(field.bufferPtr);
      std::ostringstream oss;
      oss << val;
      return oss.str();
    }
    case SQL_C_DOUBLE: {
      SQLDOUBLE val = *static_cast<const SQLDOUBLE*>(field.bufferPtr);
      std::ostringstream oss;
      oss << val;
      return oss.str();
    }
    case SQL_C_BIT: {
      SQLCHAR val = *static_cast<const SQLCHAR*>(field.bufferPtr);
      return val ? "TRUE" : "FALSE";
    }
    case SQL_C_TYPE_DATE: {
      const DATE_STRUCT* d = static_cast<const DATE_STRUCT*>(field.bufferPtr);
      std::ostringstream oss;
      oss << "DATE '" << d->year << "-";
      if (d->month < 10) oss << "0";
      oss << d->month << "-";
      if (d->day < 10) oss << "0";
      oss << d->day << "'";
      return oss.str();
    }
    case SQL_C_TYPE_TIMESTAMP: {
      const TIMESTAMP_STRUCT* ts =
          static_cast<const TIMESTAMP_STRUCT*>(field.bufferPtr);
      std::ostringstream oss;
      oss << "TIMESTAMP '" << ts->year << "-";
      if (ts->month < 10) oss << "0";
      oss << ts->month << "-";
      if (ts->day < 10) oss << "0";
      oss << ts->day << " ";
      if (ts->hour < 10) oss << "0";
      oss << ts->hour << ":";
      if (ts->minute < 10) oss << "0";
      oss << ts->minute << ":";
      if (ts->second < 10) oss << "0";
      oss << ts->second << "'";
      return oss.str();
    }
    default: {
      WriteLog(LL_WARN,
               "  SQLExecute: unhandled parameter C type " +
                   std::to_string(field.bufferCDataType) + ", substituting NULL");
      return "NULL";
    }
  }
}

// Replace each '?' in queryTemplate with the corresponding formatted parameter.
static std::string substituteParams(const std::string& queryTemplate,
                                    Descriptor* paramDesc) {
  std::string result;
  result.reserve(queryTemplate.size());
  int paramIndex = 1;
  for (size_t i = 0; i < queryTemplate.size(); ++i) {
    if (queryTemplate[i] == '?' &&
        (i == 0 || queryTemplate[i - 1] != '\\')) {
      if (paramIndex <= paramDesc->Field_Count) {
        result += formatParam(paramDesc->getField(paramIndex));
        ++paramIndex;
      } else {
        result += "NULL";
      }
    } else {
      result += queryTemplate[i];
    }
  }
  return result;
}

SQLRETURN SQL_API SQLExecute(SQLHSTMT StatementHandle) {
  WriteLog(LL_TRACE, "Entering SQLExecute");

  Statement* statement = reinterpret_cast<Statement*>(StatementHandle);

  if (!statement->isPrepared) {
    WriteLog(LL_ERROR,
             "  ERROR: SQLExecute called on a statement that has not been "
             "prepared with SQLPrepare");
    ErrorInfo errorInfo("SQLExecute called without a prior SQLPrepare", "HY010");
    statement->setError(errorInfo);
    return SQL_ERROR;
  }

  try {
    TrinoQuery* trinoQuery = statement->trinoQuery;

    std::string queryText;
    if (statement->appParamDesc->Field_Count > 0) {
      // Substitute bound parameters into the prepared query text.
      queryText = substituteParams(statement->preparedQueryText,
                                   statement->appParamDesc);
      WriteLog(LL_DEBUG, "  Executing prepared query with params: " + queryText);
    } else {
      queryText = statement->preparedQueryText;
      WriteLog(LL_DEBUG, "  Executing prepared query (no params): " + queryText);
    }

    trinoQuery->setQuery(queryText);
    trinoQuery->post();
    statement->executed              = true;
    statement->fetchExecuteConfirmed = false;
    return SQL_SUCCESS;
  } catch (const std::exception& ex) {
    WriteLog(LL_ERROR,
             "  ERROR: Exception in SQLExecute: " + std::string(ex.what()));
    ErrorInfo errorInfo("Exception thrown during SQLExecute: " +
                            std::string(ex.what()),
                        "HY000");
    statement->setError(errorInfo);
    return SQL_ERROR;
  }
}
