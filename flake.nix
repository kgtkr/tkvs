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
              patchelf
              (callPackage ./cloud-localds.nix {})
            ];
            buildInputs = lib.optionals stdenv.isDarwin [
              libiconv
            ];
          };
        devShells.aarch64-linux-cross =
          let
            pkgs = (import nixpkgs {
              system = if system == "aarch64-darwin" then "x86_64-darwin" else system;
            }).pkgsCross.aarch64-multiplatform-musl.pkgsStatic;
          in
          with pkgs; mkShell {
            nativeBuildInputs = [
              pkg-config
            ];
            buildInputs = [
              libiconv
            ];
            CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER = "${stdenv.cc.targetPrefix}cc";
          };
      }
    );
}
