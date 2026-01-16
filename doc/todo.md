# TODO

## 1. Memtable: unify public structures

- Memtable must expose public structures used directly by:
  - memtable flush iterators
  - SSTable builder (as input)
- Avoid translating between memtable flush output and SSTable build input — this is inefficient.

## 2. Sorted String Table modularization

SST should be split into the following modules:
- **SST Core**
- **SST Data Block Iterator**
- **SST Scan Iterator**
- **SST Builder**
