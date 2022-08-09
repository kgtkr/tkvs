{
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgsArgs = {
          inherit system overlays;
        };
        pkgs = import nixpkgs pkgsArgs;
        rust-toolchain = pkgs: pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        buildRustCrateForPkgs = pkgs: pkgs.buildRustCrate.override {
          rustc = rust-toolchain pkgs;
          defaultCrateOverrides = pkgs.defaultCrateOverrides // {
            "tonic-reflection" = attrs: {
              nativeBuildInputs = [
                pkgs.protobuf
              ];
            };
            "tkvs-protos" = attrs: {
              nativeBuildInputs = [
                pkgs.protobuf
              ];
            };
          };
        };
        cargoNix = import ./Cargo.nix {
          inherit pkgs buildRustCrateForPkgs;
        };
      in
      rec {
        packages = {
          tkvs-server = cargoNix.workspaceMembers.tkvs-server.build;
          tkvs-client = cargoNix.workspaceMembers.tkvs-client.build;
          tkvs-server-docker = pkgs.dockerTools.buildImage {
            name = "tkvs-server";
            copyToRoot = pkgs.buildEnv {
              name = "image-root";
              paths = [
                pkgs.coreutils
                pkgs.bash
                pkgs.cacert
                packages.tkvs-server
              ];
              pathsToLink = [ "/bin" ];
            };
            runAsRoot = ''
              #!${pkgs.runtimeShell}
              mkdir -p /data
            '';
            config = {
              Env = [ "TKVS_IP=0.0.0.0" "TKVS_PORT=50051" "TKVS_DATA=/data" ];
              Entrypoint = [ "/bin/tkvs-server" ];
              Volumes = { "/data" = { }; };
            };
          };
        };
        devShell =
          with pkgs; mkShell {
            nativeBuildInputs = [
              (rust-toolchain pkgs)
              protobuf
              wget
              qemu
              gnumake
              sshpass
              patchelf
              crate2nix
              pkg-config
            ];
            buildInputs = lib.optionals stdenv.isDarwin [
              libiconv
              darwin.apple_sdk.frameworks.SystemConfiguration
              darwin.apple_sdk.frameworks.CoreFoundation
              darwin.apple_sdk.frameworks.Security
            ] ++ lib.optionals stdenv.isLinux [
              openssl
              glibc
            ];
          };
        devShells.x86_64-linux-cross =
          let
            pkgs = (import nixpkgs {
              system = if system == "aarch64-darwin" then "x86_64-darwin" else system;
              crossSystem = "x86_64-linux";
            }).pkgsStatic;
          in
          with pkgs; mkShell {
            nativeBuildInputs = [
              pkg-config
            ];
            buildInputs = [
              libiconv
            ];
            CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER = "${stdenv.cc.targetPrefix}cc";
          };
      }
    );
}
