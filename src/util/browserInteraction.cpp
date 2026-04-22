#include "browserInteraction.hpp"

#ifdef _WIN32
#include "windowsLean.hpp"
#include <shellapi.h>

void openURLInDefaultBrowser(const std::string& url) {

  HINSTANCE result =
      ShellExecute(nullptr, nullptr, url.c_str(), nullptr, nullptr, SW_SHOW);

  if (reinterpret_cast<intptr_t>(result) <= 32) {
    MessageBox(nullptr,
               "Failed to open default browser",
               "Authentication Error",
               MB_ICONERROR);
  }
}

#elif defined(__APPLE__)
#include <cstdlib>

void openURLInDefaultBrowser(const std::string& url) {
  std::string command = "open '" + url + "'";
  system(command.c_str());
}

#else
#include <cstdlib>

void openURLInDefaultBrowser(const std::string& url) {
  std::string command = "xdg-open '" + url + "'";
  system(command.c_str());
}

#endif
