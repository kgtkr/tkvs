{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      {
        devShell =
          let
            pkgs = import nixpkgs {
              inherit system;
            };
          in
          with pkgs; mkShell {
            nativeBuildInputs = [
              protobuf
              wget
              qemu
              gnumake
              sshpass
            ];
            buildInputs = lib.optionals stdenv.isDarwin [
              libiconv
            ];
            CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER = "aarch64-unknown-linux-gnu-gcc";
          };
      }
    );
}
