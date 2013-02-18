
#include "base/string_util.h"
#include "mapreduce/public/mapred.h"
#include "proto/gen-cpp/hits_doc_types.h"
#include "vertical_offline_util.h"

using namespace std;
using namespace mapreduce;

namespace {

DEFINE_string(sum_conf, "", "");

class HitsDocMapper: public BasicMapper {
 public:
  Counter* icounter_;
  typedef map<int, pair<double, double> > DomainStatMap;
  DomainStatMap domain_map_;
  void ReadConfig(const char* config) {
    FILE* fin = fopen(config, "r");
    CHECK(fin != NULL) << "failed to open sum conf: " << config;
    char buf[1024];
    while(fgets(buf, 1024, fin)) {
      string line;
      TrimWhitespaceASCII(buf, TRIM_ALL, &line);
      vector<string> terms;
      SplitString(line, '\t', &terms);
      CHECK(terms.size() == 3) << "unexpected domain sum config";
      int domain_id = StringToInt(terms[0]);
      double hub = StringToDouble(terms[1]);
      double aut = StringToDouble(terms[2]);
      domain_map_[domain_id] = make_pair(hub, aut);
    }
    fclose(fin);
  }
  void OnCreate(mapreduce::TaskContext* context) {
    icounter_ = context->getCounter("mergeddoc", "map_number");
    ReadConfig(FLAGS_sum_conf.c_str());
  }
  virtual ~HitsDocMapper() {
  }
  virtual void Map(mapreduce::MapContext* context) {
    crawl::HitsDoc doc;
    ValueToThrift(&doc);
    DomainStatMap::iterator it = domain_map_.find(doc.domain_id);
    if (it != domain_map_.end()) {
      double hub_norm = it->second.first;
      double aut_norm = it->second.second;
      doc.hub_score /= hub_norm;
      doc.authority_score /= aut_norm;
      context->EmitThrift(GetInputKey(), doc);
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
    }
    context->Emit(GetInputKey(), GetInputValue());
    context->IncrementCounter(icounter_, 1);
  }
};

}

REGIST_MAP_RED(HitsDocMapper, HitsDocReducer);
