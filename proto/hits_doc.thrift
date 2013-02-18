namespace cpp crawl

// indomain links
struct Link {
  1: i64 docid,
  2: i32 hostid,
  3: i32 domainid,
}

struct HitsDoc {
  1: string url,
  2: double hub_score = 1,
  3: double authority_score = 1,
  4: i32 host_id,
  5: i32 domain_id,
  6: list<Link> inlinks,
  7: list<Link> outlinks,
}
