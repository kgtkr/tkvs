{ cloud-utils, cdrtools, writeShellScriptBin }:
let
  /*
  cloud-utils はMacでは cdrtools と gptfdisk に依存しているので使うことができない
  しかし, cloud-utils に入っている cloud-localds だけであれば以下の方法でパッチを当てることで使うことができる
  1. gptfdisk は そもそも必要ないので依存から削除
  2. cdrkit に入っている genisoimage の代わりにmkisofsを使う
      * 参考: https://news.ycombinator.com/item?id=30265899
  */
  # genisoimage が呼ばれた時に mkisofs を呼び出す
  fake-genisoimage = writeShellScriptBin "genisoimage" ''
    exec -a "$0" ${cdrtools}/bin/mkisofs "$@"
  '';
  cloud-localds = cloud-utils.override { cdrkit = fake-genisoimage; gptfdisk = null; };
in
  # cloud-localds 以外のスクリプトが使われないようにする
  writeShellScriptBin "cloud-localds" ''
    exec -a "$0" ${cloud-localds}/bin/cloud-localds "$@"
  ''
