
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
    // emit itself
    string docid_str = crawl::GetDocIDStringFromUrl(doc.url);
    context->EmitThrift("Doc", docid_str, doc);
    // generate inlink
    uint64_t uid;
    if (!StringToUint64(docid_str, &uid)){
      VLOG(1) << "failed to convert docid string to uint64";
      return;
    }
    crawl::Link link;
    link.docid = static_cast<int64_t>(uid);
    link.hostid = doc.host_id;
    link.domainid = doc.domain_id;
    // emit outlinks with domainid
    for (size_t i = 0; i < doc.outlinks.size(); ++i) {
      string docid_str = Uint64ToString(doc.outlinks[i].docid);
      context->EmitThrift("Link", docid_str, link);
    }
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
    crawl::HitsDoc doc;
    set<uint64_t> inset;
    vector<crawl::Link> inlinks;
    while (NextValue()) {
      string type = GetInputType();
      string value = GetInputValue();
      if (type == "Link") {
        crawl::Link link;
        ValueToThrift(&link);
        if (inset.find(link.docid) == inset.end()) {
          inset.insert(link.docid);
          inlinks.push_back(link);
        }
      } else if (type == "Doc") {
        ValueToThrift(&doc);
      }
    }
    if (doc.url.empty()) {
      VLOG(1) << "un-mapped outlink mergeddoc";
      doc.url = GetInputKey();
      if (!inlinks.empty()) {
        doc.host_id = inlinks[0].hostid;
        doc.domain_id = inlinks[0].domainid;
      }
    }
    doc.inlinks = inlinks;
    context->EmitThrift(GetInputKey(), doc);
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
