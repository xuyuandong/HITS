#ifndef VERTICAL_OFFLINE_UTIL_H_
#define VERTICAL_OFFLINE_UTIL_H_

#include <map>
#include <string>

namespace crawl {

std::string GetDocIDStringFromUrl(const std::string& url);

std::string GetSSTablePathFromUrl(const std::string& url, 
    std::map<std::string, std::string>* dir_cache_map = NULL);

std::string GetDateString(int days = 1, bool norm = true);

}  // namespace crawl

#endif  // VERTICAL_OFFLINE_UTIL_H_

