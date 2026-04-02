#include "../util/windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include <string>

#include "../util/writeLog.hpp"
#include "handles/statementHandle.hpp"

SQLRETURN SQL_API SQLBindParameter(SQLHSTMT StatementHandle,
                                   SQLUSMALLINT ipar,
                                   SQLSMALLINT fParamType,
                                   SQLSMALLINT fCType,
                                   SQLSMALLINT fSqlType,
                                   SQLULEN cbColDef,
                                   SQLSMALLINT ibScale,
                                   SQLPOINTER rgbValue,
                                   SQLLEN cbValueMax,
                                   SQLLEN* pcbValue) {
  WriteLog(LL_TRACE, "Entering SQLBindParameter");
  WriteLog(LL_TRACE, "  Parameter index: " + std::to_string(ipar));
  WriteLog(LL_TRACE, "  C type: " + std::to_string(fCType));
  WriteLog(LL_TRACE, "  SQL type: " + std::to_string(fSqlType));

  if (ipar == 0) {
    WriteLog(LL_ERROR, "  ERROR: SQLBindParameter parameter index must be >= 1");
    return SQL_ERROR;
  }

  Statement* statement = reinterpret_cast<Statement*>(StatementHandle);
  Descriptor* paramDesc = statement->appParamDesc;

  DescriptorField field = paramDesc->getField(ipar);
  field.bufferCDataType      = fCType;
  field.odbcDataType         = fSqlType;
  field.bufferPtr            = rgbValue;
  field.bufferLength         = cbValueMax;
  field.bufferStrLenOrIndPtr = pcbValue;
  field.precision            = static_cast<SQLCHAR>(cbColDef);
  field.scale                = static_cast<SQLCHAR>(ibScale);
  paramDesc->setField(ipar, field);

  // Track the highest bound parameter index in the descriptor count.
  if (ipar > paramDesc->Field_Count) {
    paramDesc->Field_Count = ipar;
  }

  return SQL_SUCCESS;
}
