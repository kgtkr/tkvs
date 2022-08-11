use bytes::Bytes;

use crate::DB;
use std::{collections::BTreeMap, path::PathBuf};

fn create_random_dir() -> PathBuf {
    use rand::distributions::Alphanumeric;
    use rand::{thread_rng, Rng};

    let mut rng = thread_rng();
    let dirname: String = (0..8).map(|_| rng.sample(Alphanumeric) as char).collect();
    let data_dir = PathBuf::from("test-data").join(dirname);
    std::fs::create_dir(&data_dir).unwrap();
    data_dir
}

#[tokio::test]
async fn put_delete_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx = db.new_trx().await;
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());
    trx.put(key1.clone(), value1.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1));
    trx.delete(key1.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);
}

#[tokio::test]
async fn put_commit_abort_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx = db.new_trx().await;
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());
    let value2 = Bytes::from(b"v2".as_slice());

    trx.put(key1.clone(), value1.clone()).await.unwrap();
    trx.commit().await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1.clone()));

    trx.put(key1.clone(), value2.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value2.clone()));

    trx.abort().await;
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1.clone()));

    trx.put(key1.clone(), value2.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value2.clone()));

    trx.commit().await.unwrap();
    trx.abort().await;
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value2.clone()));
}

#[tokio::test]
async fn delete_commit_abort_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx = db.new_trx().await;
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());

    trx.put(key1.clone(), value1.clone()).await.unwrap();
    trx.commit().await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1.clone()));

    trx.delete(key1.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);

    trx.abort().await;
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1.clone()));

    trx.delete(key1.clone()).await.unwrap();
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);

    trx.commit().await.unwrap();
    trx.abort().await;
    let key1_result = trx.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);
}

#[tokio::test]
async fn durability_test() {
    let data_dir = create_random_dir();
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());

    {
        let db = DB::new(data_dir.clone()).unwrap();
        let mut trx = db.new_trx().await;

        trx.put(key1.clone(), value1.clone()).await.unwrap();
        trx.commit().await.unwrap();
        let key1_result = trx.get(&key1).await.unwrap();
        assert_eq!(key1_result, Some(value1.clone()));
    }

    tokio::task::yield_now().await;

    {
        let db = DB::new(data_dir).unwrap();
        let mut trx = db.new_trx().await;

        let key1_result = trx.get(&key1).await.unwrap();
        assert_eq!(key1_result, Some(value1.clone()));
    }
}

#[tokio::test]
async fn isolation_by_lock_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx1 = db.new_trx().await;
    let mut trx2 = db.new_trx().await;
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());

    let key1_result = trx1.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);

    let trx2_join_handle = {
        let key1 = key1.clone();
        let value1 = value1.clone();
        tokio::spawn(async move {
            trx2.put(key1.clone(), value1.clone()).await.unwrap();
            trx2.commit().await.unwrap();
        })
    };

    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(trx2_join_handle.is_finished(), false);
    let key1_result = trx1.get(&key1).await.unwrap();
    assert_eq!(key1_result, None);

    trx1.commit().await.unwrap();
    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }
    assert_eq!(trx2_join_handle.is_finished(), true);

    let key1_result = trx1.get(&key1).await.unwrap();
    assert_eq!(key1_result, Some(value1.clone()));
}

#[tokio::test]
async fn isolation_phantom_read_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx1 = db.new_trx().await;
    let mut trx2 = db.new_trx().await;
    let key_a = Bytes::from(b"a".as_slice());
    let key_z = Bytes::from(b"z".as_slice());
    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());

    let range_result = trx1.range(&key_a..=&key_z).await.unwrap();
    assert_eq!(range_result, BTreeMap::new());

    trx2.put(key1.clone(), value1.clone()).await.unwrap();
    trx2.commit().await.unwrap();

    let range_result = trx1.range(&key_a..=&key_z).await.unwrap();
    assert_eq!(
        range_result,
        BTreeMap::from_iter([(key1.clone(), value1.clone())])
    );

    assert_eq!(
        trx1.commit().await.unwrap_err().to_string(),
        "serializable error"
    );
}

#[tokio::test]
async fn deadlock_detect_test() {
    let data_dir = create_random_dir();
    let db = DB::new(data_dir).unwrap();
    let mut trx1 = db.new_trx().await;
    let mut trx2 = db.new_trx().await;

    let key1 = Bytes::from(b"k1".as_slice());
    let value1 = Bytes::from(b"v1".as_slice());
    let key2 = Bytes::from(b"k2".as_slice());
    let value2 = Bytes::from(b"v2".as_slice());

    trx1.get(&key1).await.unwrap();
    trx2.get(&key2).await.unwrap();

    let join_handle = tokio::spawn(async move {
        trx1.put(key2.clone(), value2.clone()).await.unwrap();
    });

    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }

    assert_eq!(join_handle.is_finished(), false,);

    let put_result = trx2.put(key1.clone(), value1.clone()).await;
    assert_eq!(put_result.unwrap_err().to_string(), "Deadlock detected");
    trx2.abort().await;

    for _ in 0..1000 {
        tokio::task::yield_now().await;
    }

    assert_eq!(join_handle.is_finished(), true);
}
