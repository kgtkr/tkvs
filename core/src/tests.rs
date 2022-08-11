use bytes::Bytes;

use crate::DB;
use std::path::PathBuf;

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
async fn isolation_by_record_test() {
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
