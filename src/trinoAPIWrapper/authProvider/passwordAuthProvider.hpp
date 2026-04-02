#pragma once

#include <memory>
#include <string>

#include "authConfig.hpp"

std::unique_ptr<AuthConfig> getPasswordAuthConfigPtr(std::string hostname,
                                                     unsigned short port,
                                                     std::string connectionName,
                                                     std::string username,
                                                     std::string password);
