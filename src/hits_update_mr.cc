
#include "base/string_util.h"
#include "mapreduce/public/mapred.h"
#include "proto/gen-cpp/hits_doc_types.h"
#include "vertical_offline_util.h"

using namespace std;
using namespace mapreduce;

namespace {

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
    string key = doc.url;
    if (StartsWithASCII(doc.url, "http", false))
      key = crawl::GetDocIDStringFromUrl(doc.url); 
    context->Emit("D", key, GetInputValue());
    for (size_t i = 0; i < doc.outlinks.size(); ++i) {
      string docid_str = Uint64ToString(doc.outlinks[i].docid);
      context->Emit("H", docid_str, DoubleToString(doc.hub_score));
    }
    for (size_t i = 0; i < doc.inlinks.size(); ++i) {
      string docid_str = Uint64ToString(doc.inlinks[i].docid);
      context->Emit("A", docid_str, DoubleToString(doc.authority_score));
    }
    context->IncrementCounter(icounter_, 1);
  }
};

class HitsDocReducer: public BasicReducer {
 public:
  Counter* icounter_;
  ThriftObjReader thrift_reader_;
  virtual ~HitsDocReducer() {
  }
  void OnCreate(mapreduce::TaskContext* context) {
    icounter_ = context->getCounter("mergeddoc", "red_number");
  }
  virtual void Reduce(mapreduce::ReduceContext* context) {
    crawl::HitsDoc doc;
    double hub_sum = 0, authority_sum = 0;
    while (NextValue()) {
      string type = GetInputType();
      string value = GetInputValue();
      if (type == "H") {
        authority_sum += StringToDouble(value);
      } else if (type == "A") {
        hub_sum += StringToDouble(value);
      } else if (type == "D") {
        thrift_reader_.FromStringToThrift(value, &doc);
      }
    }
    doc.hub_score = hub_sum;
    doc.authority_score = authority_sum;
    context->EmitThrift(GetInputKey(), doc);
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
