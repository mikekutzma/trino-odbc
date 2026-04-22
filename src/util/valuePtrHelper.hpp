#pragma once

#include "windowsLean.hpp"
#include <sql.h>
#include <sqlext.h>

#include <cstring>
#include <string>

/*
Write a string to the buffer at InfoValuePtr,
and the length of the string to StringLengthPtr.

If InfoValuePtr is null, this still writes a length to
StringLengthPtr anyway. This is used by applications that
want to check the required size of an array before
allocating one.
*/
template <class T>
void writeNullTermStringToPtr(SQLPOINTER InfoValuePtr,
                              std::string s,
                              T* StringLengthPtr) {
  size_t length = s.length();
  if (InfoValuePtr) {
    char* infoCharPtr = reinterpret_cast<char*>(InfoValuePtr);
    std::memcpy(infoCharPtr, s.c_str(), length);
    infoCharPtr[length] = '\0';
  }
  if (StringLengthPtr) {
    *StringLengthPtr = static_cast<T>(length);
  }
}
