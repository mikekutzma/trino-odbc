#include "../util/windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include "../util/writeLog.hpp"

// Forward declaration — SQLFetch is defined in fetch.cpp.
SQLRETURN SQL_API SQLFetch(SQLHSTMT StatementHandle);

SQLRETURN SQL_API SQLFetchScroll(SQLHSTMT StatementHandle,
                                 SQLSMALLINT FetchOrientation,
                                 SQLLEN FetchOffset) {
  WriteLog(LL_TRACE,
           "Entering SQLFetchScroll (orientation=" +
               std::to_string(FetchOrientation) + ")");

  switch (FetchOrientation) {
    case SQL_FETCH_NEXT:
      // The most common case — just advance one row forward.
      return SQLFetch(StatementHandle);

    case SQL_FETCH_FIRST:
    case SQL_FETCH_PRIOR:
    case SQL_FETCH_LAST:
    case SQL_FETCH_ABSOLUTE:
    case SQL_FETCH_RELATIVE:
    case SQL_FETCH_BOOKMARK:
      WriteLog(LL_ERROR,
               "  ERROR: SQLFetchScroll orientation " +
                   std::to_string(FetchOrientation) +
                   " not supported; Trino result sets are forward-only");
      return SQL_ERROR;

    default:
      WriteLog(LL_ERROR,
               "  ERROR: SQLFetchScroll unknown orientation " +
                   std::to_string(FetchOrientation));
      return SQL_ERROR;
  }
}
