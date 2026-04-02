#include "passwordAuthProvider.hpp"

#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "../../util/windowsLean.hpp"
#include <wincrypt.h>

#include "../../util/writeLog.hpp"

// Encode bytes as standard base64 using the Windows CryptoAPI.
static std::string toBase64(const std::string& input) {
  DWORD requiredSize = 0;
  if (!CryptBinaryToStringA(reinterpret_cast<const BYTE*>(input.c_str()),
                            static_cast<DWORD>(input.size()),
                            CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF,
                            nullptr,
                            &requiredSize)) {
    throw std::runtime_error("Failed to calculate base64 encoding size");
  }
  std::vector<char> buf(requiredSize);
  if (!CryptBinaryToStringA(reinterpret_cast<const BYTE*>(input.c_str()),
                            static_cast<DWORD>(input.size()),
                            CRYPT_STRING_BASE64 | CRYPT_STRING_NOCRLF,
                            buf.data(),
                            &requiredSize)) {
    throw std::runtime_error("Failed to base64-encode credentials");
  }
  // requiredSize includes the null terminator when output is a string
  return std::string(buf.data(), requiredSize - 1);
}

class PasswordAuthConfig : public AuthConfig {
  public:
    PasswordAuthConfig(std::string hostname,
                       unsigned short port,
                       std::string connectionName,
                       std::string username,
                       std::string password)
        : AuthConfig(hostname, port, connectionName) {
      std::string credentials = username + ":" + password;
      std::string encoded     = toBase64(credentials);
      this->headers.insert({"Authorization", "Basic " + encoded});
      this->headers.insert({"X-Trino-User", username});
    }

    bool const isExpired() override {
      return false;
    }

    void
    refresh(CURL* curl,
            std::string* responseData,
            std::map<std::string, std::string>* responseHeaderData) override {
      // Static credentials never need refreshing.
    }

    ~PasswordAuthConfig() override = default;
};

std::unique_ptr<AuthConfig> getPasswordAuthConfigPtr(std::string hostname,
                                                     unsigned short port,
                                                     std::string connectionName,
                                                     std::string username,
                                                     std::string password) {
  return std::make_unique<PasswordAuthConfig>(
      hostname, port, connectionName, username, password);
}
