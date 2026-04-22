#include "b64decoder.hpp"
#include <algorithm>
#include <stdexcept>
#include <unordered_map>
#include <utility>
#include <vector>

#include "writeLog.hpp"

#ifdef _WIN32

#include "windowsLean.hpp"
#include <wincrypt.h>

std::string fromBase64url(const std::string& input) {
  /*
  NOTE: base64url encoding is different from base64 encoding.
  This is an implementation of the former.
  */
  DWORD requiredSize = 0;
  // Calling with a null size pointer causes the function to calculate the
  // required output size instead of encrypting the string.
  if (!CryptStringToBinary(input.c_str(),
                           0,
                           CRYPT_STRING_BASE64URI,
                           NULL,
                           &requiredSize,
                           NULL,
                           NULL)) {
    std::string errorMessage = "Error calculating base64url decoding size";
    WriteLog(LL_ERROR, errorMessage);
    throw std::runtime_error(errorMessage);
  }
  std::vector<BYTE> decodedData(requiredSize);
  if (!CryptStringToBinary(input.c_str(),
                           0,
                           CRYPT_STRING_BASE64,
                           decodedData.data(),
                           &requiredSize,
                           NULL,
                           NULL)) {
    std::string errorMessage = "Error decoding from base64url";
    WriteLog(LL_ERROR, errorMessage);
    throw std::runtime_error(errorMessage);
  }
  return std::string(reinterpret_cast<char*>(decodedData.data()), requiredSize);
}

#else

// Maps each ASCII byte to its 6-bit base64 value. A=0, B=1, ... Z=25, a=26, ...
// z=51, 0=52, ... 9=61, +=62, /=63. Everything else is 255 (invalid).
// The = sign maps to 0 but is handled separately as a stop signal.
static const unsigned char B64_DECODE_TABLE[256] = {
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 62,  255,
    62,  255, 63,  52,  53,  54,  55,  56,  57,  58,  59,  60,  61,  255, 255,
    255, 0,   255, 255, 255, 0,   1,   2,   3,   4,   5,   6,   7,   8,   9,
    10,  11,  12,  13,  14,  15,  16,  17,  18,  19,  20,  21,  22,  23,  24,
    25,  255, 255, 255, 255, 63,  255, 26,  27,  28,  29,  30,  31,  32,  33,
    34,  35,  36,  37,  38,  39,  40,  41,  42,  43,  44,  45,  46,  47,  48,
    49,  50,  51,  255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
    255,
};

std::string fromBase64url(const std::string& input) {
  // Convert base64url to standard base64
  std::string b64 = input;
  std::replace(b64.begin(), b64.end(), '-', '+');
  std::replace(b64.begin(), b64.end(), '_', '/');
  // Add padding
  while (b64.size() % 4 != 0) {
    b64.push_back('=');
  }

  std::vector<unsigned char> out;
  out.reserve(b64.size() * 3 / 4);

  unsigned int buf = 0;
  int bits         = 0;
  for (unsigned char c : b64) {
    if (c == '=') break;
    unsigned char val = B64_DECODE_TABLE[c];
    if (val == 255) continue;
    buf = (buf << 6) | val;
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out.push_back(static_cast<unsigned char>((buf >> bits) & 0xFF));
    }
  }

  return std::string(reinterpret_cast<char*>(out.data()), out.size());
}

#endif
