
#include "base/string_util.h"
#include "mapreduce/public/mapred.h"
#include "vertical_offline_util.h"
#include "proto/gen-cpp/hits_doc_types.h"

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
    if (doc.hub_score > 1.0) {
      string hubstr = DoubleToString(doc.hub_score);
      context->EmitThrift(hubstr, doc);
      context->IncrementCounter(icounter_, 1);
    }
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
      context->Emit(GetInputKey(), GetInputValue());
    }
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
