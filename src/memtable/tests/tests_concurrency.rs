#[cfg(test)]
mod concurrency_tests {
    use crate::memtable::Memtable;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_concurrent_puts() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        let mut handles = Vec::new();
        for i in 0..10 {
            let memtable = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("key{}_{}", i, j).into_bytes();
                    let value = format!("value{}_{}", i, j).into_bytes();
                    memtable.put(key, value).unwrap();
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let guard = memtable.inner.read().unwrap();
        assert_eq!(guard.tree.len(), 1000);
    }

    #[test]
    fn test_concurrent_gets_and_puts() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        let memtable_writer = Arc::clone(&memtable);
        let writer = thread::spawn(move || {
            for i in 0..500 {
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                memtable_writer.put(key, value).unwrap();
            }
        });

        let memtable_reader = Arc::clone(&memtable);
        let reader = thread::spawn(move || {
            for i in 0..500 {
                let key = format!("key{}", i).into_bytes();
                let _ = memtable_reader.get(&key).unwrap();
            }
        });

        writer.join().unwrap();
        reader.join().unwrap();

        let guard = memtable.inner.read().unwrap();
        assert_eq!(guard.tree.len(), 500);
    }

    #[test]
    fn test_concurrent_puts_and_deletes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        for i in 0..200 {
            let key = format!("key{}", i).into_bytes();
            memtable.put(key, b"initial".to_vec()).unwrap();
        }

        let memtable_writer = Arc::clone(&memtable);
        let writer = thread::spawn(move || {
            for i in 0..200 {
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                memtable_writer.put(key, value).unwrap();
            }
        });

        let memtable_deleter = Arc::clone(&memtable);
        let deleter = thread::spawn(move || {
            for i in 100..200 {
                let key = format!("key{}", i).into_bytes();
                memtable_deleter.delete(key).unwrap();
            }
        });

        writer.join().unwrap();
        deleter.join().unwrap();

        let guard = memtable.inner.read().unwrap();
        for i in 0..100 {
            let key = format!("key{}", i).into_bytes();
            let versions = guard.tree.get(&key).unwrap();
            let entry = versions.values().next().unwrap();
            assert!(!entry.is_delete, "key {} should not be deleted", i);
        }
        for i in 100..200 {
            let key = format!("key{}", i).into_bytes();
            let versions = guard.tree.get(&key).unwrap();
            let entry = versions.values().next().unwrap();
            assert!(entry.is_delete || entry.value.is_some());
        }
    }
}

#[cfg(test)]
mod scan_concurrent_tests {
    use crate::memtable::Memtable;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    #[test]
    fn test_scan_during_concurrent_puts() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        let mem_clone = Arc::clone(&memtable);
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let key = format!("key{}", i).into_bytes();
                let value = format!("value{}", i).into_bytes();
                mem_clone.put(key, value).unwrap();
            }
        });

        let results: Vec<_> = memtable.scan(b"key0", b"key49\xff").unwrap().collect();

        for (_, entry) in results.iter() {
            assert!(!entry.is_delete);
            assert!(entry.value.is_some());
        }

        handle.join().unwrap();
    }

    #[test]
    fn test_scan_during_concurrent_deletes() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        for i in 0..50 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            memtable.put(key, value).unwrap();
        }

        let mem_clone = Arc::clone(&memtable);
        let handle = thread::spawn(move || {
            for i in 0..50 {
                let key = format!("key{}", i).into_bytes();
                mem_clone.delete(key).unwrap();
            }
        });

        let results: Vec<_> = memtable.scan(b"key0", b"key49\xff").unwrap().collect();

        for (_, entry) in results.iter() {
            assert!(entry.value.is_some() || entry.is_delete);
        }

        handle.join().unwrap();
    }

    #[test]
    fn test_scan_with_multiple_concurrent_writers() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("wal-000000.log");
        let memtable = Arc::new(Memtable::new(&path, None, 1024 * 1024).unwrap());

        let mut handles = vec![];

        for t in 0..4 {
            let mem_clone = Arc::clone(&memtable);
            handles.push(thread::spawn(move || {
                for i in 0..25 {
                    let key = format!("key{}_{}", t, i).into_bytes();
                    let value = format!("value{}_{}", t, i).into_bytes();
                    mem_clone.put(key, value).unwrap();
                    if i % 5 == 0 {
                        let del_key = format!("key{}_{}", t, i / 2).into_bytes();
                        let _ = mem_clone.delete(del_key);
                    }
                }
            }));
        }

        for _ in 0..10 {
            let results: Vec<_> = memtable.scan(b"key0", b"key9_24\xff").unwrap().collect();
            for (_key, entry) in results.iter() {
                assert!(entry.value.is_some() || entry.is_delete);
            }
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}
