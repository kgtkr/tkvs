# tkvs
toy kvs

## Run local
```sh
$ TKVS_DATA=data cargo run --bin tkvs-server
```

```sh
$ cargo run --bin tkvs-client -- http://localhost:50051
> help
```

## Public server
```sh
$ cargo run --bin tkvs-client -- https://tkvs.kgtkr.net
```
or

```sh
$ grpcurl tkvs.kgtkr.net:443 list kgtkr.tkvs.Tkvs
```
