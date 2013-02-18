
#include "base/string_util.h"
#include "mapreduce/public/mapred.h"
#include "proto/gen-cpp/hits_doc_types.h"
#include "vertical_offline_util.h"

using namespace std;
using namespace mapreduce;

namespace {

const char site[][10] = {"baidu", "google", "soso", "sogou"};
const int site_num = sizeof(site) / (10 * sizeof(char));

class HitsDocMapper: public BasicMapper {
 public:
  Counter* icounter_;
  void OnCreate(mapreduce::TaskContext* context) {
    icounter_ = context->getCounter("mergeddoc", "map_number");
  }
  virtual ~HitsDocMapper() {
  }
  virtual void Map(mapreduce::MapContext* context) {
    crawl::HitsDoc doc;
    ValueToThrift(&doc);
    string body = doc.url.substr(7);
    if (body[0] == '/')
      body = body.substr(1);
    vector<string> hosts;
    SplitString(body, '/', &hosts);
    if (!hosts.empty()) {
      for (int i = 0; i < site_num; ++i) {
        if (hosts[0].find(site[i]) != string::npos)
          return;
      }
    }
    string key = IntToString(doc.domain_id);
    context->EmitThrift(key, doc);
    context->IncrementCounter(icounter_, 1);
  }
};

class HitsDocReducer: public BasicReducer {
 public:
  Counter* icounter_;
  virtual ~HitsDocReducer() {
  }
  void OnCreate(mapreduce::TaskContext* context) {
    icounter_ = context->getCounter("mergeddoc", "red_number");
  }
  virtual void Reduce(mapreduce::ReduceContext* context) {
    while (NextValue()) {
      crawl::HitsDoc doc;
      ValueToThrift(&doc);
      string docid_str = crawl::GetDocIDStringFromUrl(doc.url);
      context->Emit(docid_str, GetInputValue());
    }
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
