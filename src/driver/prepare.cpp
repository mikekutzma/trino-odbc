#include "../util/windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include "../util/stringFromChar.hpp"
#include "../util/writeLog.hpp"
#include "handles/statementHandle.hpp"

SQLRETURN SQL_API SQLPrepare(SQLHSTMT StatementHandle,
                             _In_reads_(TextLength) SQLCHAR* StatementText,
                             SQLINTEGER TextLength) {
  WriteLog(LL_TRACE, "Entering SQLPrepare");

  if (!StatementText) {
    WriteLog(LL_ERROR, "  ERROR: SQLPrepare received null StatementText");
    return SQL_ERROR;
  }

  Statement* statement        = reinterpret_cast<Statement*>(StatementHandle);
  statement->preparedQueryText = stringFromChar(StatementText, TextLength);
  statement->isPrepared        = true;
  // Reset execution state so the statement can be re-executed.
  statement->executed              = false;
  statement->fetchExecuteConfirmed = false;

  WriteLog(LL_DEBUG, "  Prepared query: " + statement->preparedQueryText);
  return SQL_SUCCESS;
}
