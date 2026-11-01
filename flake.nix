{
  inputs = {
    nixpkgs.url = "nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, flake-utils, nixpkgs }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = ((import nixpkgs) {
          inherit system;
        });
      in
      {
        formatter = pkgs.nixpkgs-fmt;

        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustc
            cargo
            rustfmt
            clippy
            rust-analyzer
            cargo-outdated

            netcat
            nats-server
            natscli
          ] ++ lib.optional stdenv.isDarwin [
            darwin.apple_sdk.frameworks.SystemConfiguration
            llvmPackages.lldb
            iconv
          ];

          
          RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";

          # if we're darwin, set the lldb debugserver path.
          # This is a hack, see https://github.com/NixOS/nixpkgs/pull/374846 and https://github.com/NixOS/nixpkgs/issues/252838
          LLDB_DEBUGSERVER_PATH = if pkgs.stdenv.isDarwin then "/Applications/Xcode.app/Contents/SharedFrameworks/LLDB.framework/Versions/A/Resources/debugserver" else null;
        };
      }
    );
}
