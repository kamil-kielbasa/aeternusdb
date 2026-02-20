window.BENCHMARK_DATA = {
  "lastUpdate": 1771577959663,
  "repoUrl": "https://github.com/kamil-kielbasa/aeternusdb",
  "entries": {
    "AeternusDB Benchmarks": [
      {
        "commit": {
          "author": {
            "email": "kamkie1996@gmail.com",
            "name": "Kamil Kielbasa",
            "username": "kamil-kielbasa"
          },
          "committer": {
            "email": "kamkie1996@gmail.com",
            "name": "Kamil Kielbasa",
            "username": "kamil-kielbasa"
          },
          "distinct": true,
          "id": "6f622bd6fca22c488851875b769838236f0972e6",
          "message": "feat: benchmarks",
          "timestamp": "2026-02-19T20:43:20+01:00",
          "tree_id": "30a43d6ce6f51b4bfdd3776cbd1aedc10696e858",
          "url": "https://github.com/kamil-kielbasa/aeternusdb/commit/6f622bd6fca22c488851875b769838236f0972e6"
        },
        "date": 1771531089993,
        "tool": "cargo",
        "benches": [
          {
            "name": "put/memtable_only/128B",
            "value": 435056,
            "range": "± 71874",
            "unit": "ns/iter"
          },
          {
            "name": "put/memtable_only/1K",
            "value": 408589,
            "range": "± 78387",
            "unit": "ns/iter"
          },
          {
            "name": "put/sequential_with_flush",
            "value": 424183,
            "range": "± 68769",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_hit",
            "value": 225,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_miss",
            "value": 256,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_hit",
            "value": 2294,
            "range": "± 72",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_miss",
            "value": 1739,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "delete/point",
            "value": 379252,
            "range": "± 49804",
            "unit": "ns/iter"
          },
          {
            "name": "delete/range",
            "value": 389109,
            "range": "± 46911",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/10_keys",
            "value": 1971,
            "range": "± 30",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/100_keys",
            "value": 15570,
            "range": "± 62",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/1000_keys",
            "value": 154602,
            "range": "± 2453",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/10_keys",
            "value": 10594,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/100_keys",
            "value": 29259,
            "range": "± 496",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/1000_keys",
            "value": 200789,
            "range": "± 2136",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/1000",
            "value": 7219640,
            "range": "± 1219005",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/5000",
            "value": 15617299,
            "range": "± 850201",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/1000",
            "value": 2175327,
            "range": "± 218646",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/10000",
            "value": 1965398,
            "range": "± 267809",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/64B",
            "value": 416667,
            "range": "± 69899",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/256B",
            "value": 323264,
            "range": "± 38192",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/1K",
            "value": 396119,
            "range": "± 40086",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/4K",
            "value": 445709,
            "range": "± 101810",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/load/sequential/10000",
            "value": 4334927427,
            "range": "± 503451637",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/A_50read_50update",
            "value": 1209290397,
            "range": "± 172158009",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/B_95read_5update",
            "value": 131606610,
            "range": "± 34318542",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/C_100read",
            "value": 22415811,
            "range": "± 5587796",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/D_95read_5insert",
            "value": 115451455,
            "range": "± 11958549",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/E_95scan_5insert",
            "value": 186679628,
            "range": "± 10967070",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/F_50read_50rmw",
            "value": 1016528266,
            "range": "± 134509793",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "kamkie1996@gmail.com",
            "name": "Kamil Kielbasa",
            "username": "kamil-kielbasa"
          },
          "committer": {
            "email": "kamkie1996@gmail.com",
            "name": "Kamil Kielbasa",
            "username": "kamil-kielbasa"
          },
          "distinct": true,
          "id": "a409e2068559592f2519d5a0b66a47679238e5e6",
          "message": "feat: benchmarks",
          "timestamp": "2026-02-20T09:47:07+01:00",
          "tree_id": "03d8127ea019a3612dec201ece4490b51ba5d4a3",
          "url": "https://github.com/kamil-kielbasa/aeternusdb/commit/a409e2068559592f2519d5a0b66a47679238e5e6"
        },
        "date": 1771577958463,
        "tool": "cargo",
        "benches": [
          {
            "name": "put/memtable_only/128B",
            "value": 281221,
            "range": "± 22849",
            "unit": "ns/iter"
          },
          {
            "name": "put/memtable_only/1K",
            "value": 284606,
            "range": "± 16840",
            "unit": "ns/iter"
          },
          {
            "name": "put/sequential_with_flush",
            "value": 329071,
            "range": "± 45623",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_hit",
            "value": 239,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_miss",
            "value": 245,
            "range": "± 3",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_hit",
            "value": 2258,
            "range": "± 74",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_miss",
            "value": 1660,
            "range": "± 41",
            "unit": "ns/iter"
          },
          {
            "name": "delete/point",
            "value": 267150,
            "range": "± 15715",
            "unit": "ns/iter"
          },
          {
            "name": "delete/range",
            "value": 268307,
            "range": "± 16973",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/10_keys",
            "value": 1839,
            "range": "± 9",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/100_keys",
            "value": 15058,
            "range": "± 369",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/1000_keys",
            "value": 150517,
            "range": "± 1993",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/10_keys",
            "value": 10613,
            "range": "± 78",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/100_keys",
            "value": 28937,
            "range": "± 194",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/1000_keys",
            "value": 197920,
            "range": "± 2923",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/1000",
            "value": 6152592,
            "range": "± 667388",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/5000",
            "value": 14126580,
            "range": "± 348623",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/1000",
            "value": 1609425,
            "range": "± 51747",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/10000",
            "value": 1639324,
            "range": "± 35701",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/64B",
            "value": 264600,
            "range": "± 20790",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/256B",
            "value": 277912,
            "range": "± 55908",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/1K",
            "value": 278351,
            "range": "± 16890",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/4K",
            "value": 327869,
            "range": "± 24061",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/load/sequential/10000",
            "value": 2813506350,
            "range": "± 39266967",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/A_50read_50update",
            "value": 713219715,
            "range": "± 23514625",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/B_95read_5update",
            "value": 90894537,
            "range": "± 6745140",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/C_100read",
            "value": 16904280,
            "range": "± 4744947",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/D_95read_5insert",
            "value": 85303761,
            "range": "± 6015339",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/E_95scan_5insert",
            "value": 150505689,
            "range": "± 8592329",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/F_50read_50rmw",
            "value": 713289413,
            "range": "± 17763668",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}