
#include <cmath>
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
    string key = IntToString(doc.domain_id);
    double aut_sum = 0.0, hub_sum = 0.0;
    for (size_t i = 0; i < doc.outlinks.size(); ++i) {
      aut_sum += doc.hub_score * doc.hub_score;
    }
    context->Emit("A", key, DoubleToString(aut_sum));
    for (size_t i = 0; i < doc.inlinks.size(); ++i) {
      hub_sum += doc.authority_score * doc.authority_score;
    }
    context->Emit("H", key, DoubleToString(hub_sum));
    context->IncrementCounter(icounter_, 1);
  }
};

/*
class HitsDocCombiner: public BasicReducer {
 public:
  Counter* icounter_;
  virtual ~HitsDocCombiner() {
  }
  void OnCreate(mapreduce::TaskContext* context) {
  }
  virtual void Reduce(mapreduce::ReduceContext* context) {
    double hub_sum = 0, authority_sum = 0;
    while (NextValue()) {
      string type = GetInputType();
      string value = GetInputValue();
      if (type == "H") {
        hub_sum += StringToDouble(value);
      } else if (type == "A") {
        authority_sum += StringToDouble(value);
      } 
    }
    context->Emit("A", GetInputKey(), DoubleToString(authority_sum));
    context->Emit("H", GetInputKey(), DoubleToString(hub_sum));
    context->IncrementCounter(icounter_, 1);
  }
};
*/

class HitsDocReducer: public BasicReducer {
 public:
  Counter* icounter_;
  virtual ~HitsDocReducer() {
  }
  void OnCreate(mapreduce::TaskContext* context) {
    icounter_ = context->getCounter("mergeddoc", "red_number");
  }
  virtual void Reduce(mapreduce::ReduceContext* context) {
    double hub_sum = 0, authority_sum = 0;
    while (NextValue()) {
      string type = GetInputType();
      string value = GetInputValue();
      if (type == "H") {
        hub_sum += StringToDouble(value);
      } else if (type == "A") {
        authority_sum += StringToDouble(value);
      } 
    }
    hub_sum = sqrt(hub_sum + 0.00000001);
    authority_sum = sqrt(authority_sum + 0.00000001);
    string value = DoubleToString(hub_sum) + 
      "\t" + DoubleToString(authority_sum);
    context->Emit(GetInputKey(), value);
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
//REGIST_MAP_RED_COM(HitsDocMapper, HitsDocReducer, HitsDocCombiner);
