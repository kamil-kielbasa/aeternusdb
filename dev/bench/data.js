window.BENCHMARK_DATA = {
  "lastUpdate": 1771587675309,
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
          "id": "6d77205b154db1dae49cf235ea5ceb852b0493c4",
          "message": "chore: hardening",
          "timestamp": "2026-02-20T09:55:26+01:00",
          "tree_id": "8558c68636679d3fe0c6271604ead84cec33b423",
          "url": "https://github.com/kamil-kielbasa/aeternusdb/commit/6d77205b154db1dae49cf235ea5ceb852b0493c4"
        },
        "date": 1771578866059,
        "tool": "cargo",
        "benches": [
          {
            "name": "put/memtable_only/128B",
            "value": 358750,
            "range": "± 45925",
            "unit": "ns/iter"
          },
          {
            "name": "put/memtable_only/1K",
            "value": 322234,
            "range": "± 41063",
            "unit": "ns/iter"
          },
          {
            "name": "put/sequential_with_flush",
            "value": 357641,
            "range": "± 45266",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_hit",
            "value": 258,
            "range": "± 8",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_miss",
            "value": 263,
            "range": "± 4",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_hit",
            "value": 2280,
            "range": "± 65",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_miss",
            "value": 1652,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "delete/point",
            "value": 382438,
            "range": "± 70170",
            "unit": "ns/iter"
          },
          {
            "name": "delete/range",
            "value": 454493,
            "range": "± 60826",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/10_keys",
            "value": 2146,
            "range": "± 24",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/100_keys",
            "value": 15960,
            "range": "± 216",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/1000_keys",
            "value": 154150,
            "range": "± 2348",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/10_keys",
            "value": 10811,
            "range": "± 48",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/100_keys",
            "value": 30051,
            "range": "± 247",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/1000_keys",
            "value": 202757,
            "range": "± 2428",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/1000",
            "value": 8197561,
            "range": "± 810784",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/5000",
            "value": 15846369,
            "range": "± 732732",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/1000",
            "value": 2240178,
            "range": "± 72143",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/10000",
            "value": 2244930,
            "range": "± 123801",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/64B",
            "value": 424723,
            "range": "± 56645",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/256B",
            "value": 427303,
            "range": "± 50088",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/1K",
            "value": 421750,
            "range": "± 55184",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/4K",
            "value": 445129,
            "range": "± 69107",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/load/sequential/10000",
            "value": 4413445446,
            "range": "± 102509781",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/A_50read_50update",
            "value": 1024304514,
            "range": "± 106649286",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/B_95read_5update",
            "value": 116140959,
            "range": "± 17366645",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/C_100read",
            "value": 25158707,
            "range": "± 4420290",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/D_95read_5insert",
            "value": 125349578,
            "range": "± 14863572",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/E_95scan_5insert",
            "value": 193632234,
            "range": "± 14569979",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/F_50read_50rmw",
            "value": 1196441168,
            "range": "± 123291750",
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
          "id": "a46509f8f3c97efa0cc54a7e4a87a35a303840f1",
          "message": "impr: benchmarks",
          "timestamp": "2026-02-20T10:17:12+01:00",
          "tree_id": "bbbcee802b670694e9fe86488038e21382a97c9a",
          "url": "https://github.com/kamil-kielbasa/aeternusdb/commit/a46509f8f3c97efa0cc54a7e4a87a35a303840f1"
        },
        "date": 1771587674272,
        "tool": "cargo",
        "benches": [
          {
            "name": "put/memtable_only/128B",
            "value": 437440,
            "range": "± 85265",
            "unit": "ns/iter"
          },
          {
            "name": "put/memtable_only/1K",
            "value": 481977,
            "range": "± 89180",
            "unit": "ns/iter"
          },
          {
            "name": "put/sequential_with_flush",
            "value": 553286,
            "range": "± 106145",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_hit",
            "value": 241,
            "range": "± 2",
            "unit": "ns/iter"
          },
          {
            "name": "get/memtable_miss",
            "value": 261,
            "range": "± 5",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_hit",
            "value": 2454,
            "range": "± 71",
            "unit": "ns/iter"
          },
          {
            "name": "get/sstable_miss",
            "value": 1691,
            "range": "± 7",
            "unit": "ns/iter"
          },
          {
            "name": "delete/point",
            "value": 452316,
            "range": "± 84881",
            "unit": "ns/iter"
          },
          {
            "name": "delete/range",
            "value": 404434,
            "range": "± 56223",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/10_keys",
            "value": 1949,
            "range": "± 12",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/100_keys",
            "value": 15889,
            "range": "± 186",
            "unit": "ns/iter"
          },
          {
            "name": "scan/memtable/1000_keys",
            "value": 153955,
            "range": "± 1735",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/10_keys",
            "value": 10793,
            "range": "± 90",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/100_keys",
            "value": 29288,
            "range": "± 267",
            "unit": "ns/iter"
          },
          {
            "name": "scan/sstable/1000_keys",
            "value": 201394,
            "range": "± 1658",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/1000",
            "value": 7509976,
            "range": "± 1522943",
            "unit": "ns/iter"
          },
          {
            "name": "compaction/major/5000",
            "value": 17630757,
            "range": "± 1273779",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/1000",
            "value": 2293539,
            "range": "± 267052",
            "unit": "ns/iter"
          },
          {
            "name": "recovery/open_existing/10000",
            "value": 2318976,
            "range": "± 119758",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/64B",
            "value": 438310,
            "range": "± 93649",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/256B",
            "value": 410225,
            "range": "± 53502",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/1K",
            "value": 429456,
            "range": "± 82205",
            "unit": "ns/iter"
          },
          {
            "name": "value_size/put/4K",
            "value": 473772,
            "range": "± 70435",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent/readers/1",
            "value": 16084008,
            "range": "± 777358",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent/readers/2",
            "value": 18175611,
            "range": "± 764067",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent/readers/4",
            "value": 18355050,
            "range": "± 450974",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent/read_under_write/1_writer",
            "value": 97250329,
            "range": "± 7669911",
            "unit": "ns/iter"
          },
          {
            "name": "concurrent/read_under_write/2_writer",
            "value": 193940606,
            "range": "± 18412541",
            "unit": "ns/iter"
          },
          {
            "name": "overwrite/update_memtable",
            "value": 407121,
            "range": "± 69933",
            "unit": "ns/iter"
          },
          {
            "name": "overwrite/update_sstable",
            "value": 432697,
            "range": "± 78932",
            "unit": "ns/iter"
          },
          {
            "name": "dataset_scaling/get/1K",
            "value": 1973,
            "range": "± 23",
            "unit": "ns/iter"
          },
          {
            "name": "dataset_scaling/get/10K",
            "value": 2399,
            "range": "± 20",
            "unit": "ns/iter"
          },
          {
            "name": "dataset_scaling/get/50K",
            "value": 3142,
            "range": "± 107",
            "unit": "ns/iter"
          },
          {
            "name": "dataset_scaling/get/100K",
            "value": 5108,
            "range": "± 159",
            "unit": "ns/iter"
          },
          {
            "name": "tombstone_scan/dense_tombstones/0%",
            "value": 29862,
            "range": "± 259",
            "unit": "ns/iter"
          },
          {
            "name": "tombstone_scan/dense_tombstones/25%",
            "value": 22214,
            "range": "± 168",
            "unit": "ns/iter"
          },
          {
            "name": "tombstone_scan/dense_tombstones/50%",
            "value": 27413,
            "range": "± 830",
            "unit": "ns/iter"
          },
          {
            "name": "tombstone_scan/dense_tombstones/75%",
            "value": 34965,
            "range": "± 2586",
            "unit": "ns/iter"
          },
          {
            "name": "close/empty",
            "value": 1944228,
            "range": "± 81493",
            "unit": "ns/iter"
          },
          {
            "name": "close/with_data/1000",
            "value": 2483633,
            "range": "± 770815",
            "unit": "ns/iter"
          },
          {
            "name": "close/with_data/5000",
            "value": 3326771,
            "range": "± 478783",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/put/16B",
            "value": 414645,
            "range": "± 115217",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/put/64B",
            "value": 477577,
            "range": "± 83125",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/put/256B",
            "value": 447564,
            "range": "± 78997",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/put/512B",
            "value": 489215,
            "range": "± 71604",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/get/16B",
            "value": 2448,
            "range": "± 184",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/get/64B",
            "value": 2227,
            "range": "± 152",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/get/256B",
            "value": 3370,
            "range": "± 203",
            "unit": "ns/iter"
          },
          {
            "name": "key_size/get/512B",
            "value": 4930,
            "range": "± 255",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/load/sequential/10000",
            "value": 4311113275,
            "range": "± 391027502",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/A_50read_50update",
            "value": 1114315704,
            "range": "± 100208549",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/B_95read_5update",
            "value": 113017600,
            "range": "± 14351357",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/C_100read",
            "value": 23090244,
            "range": "± 6052613",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/D_95read_5insert",
            "value": 123451968,
            "range": "± 15191733",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/E_95scan_5insert",
            "value": 182398884,
            "range": "± 17237075",
            "unit": "ns/iter"
          },
          {
            "name": "ycsb/workload/F_50read_50rmw",
            "value": 1193880980,
            "range": "± 107541569",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}