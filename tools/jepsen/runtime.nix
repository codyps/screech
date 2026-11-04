{ nixpkgs ? "nixpkgs" }:
# Host controller and an isolated Linux guest. Evaluation uses the caller's
# nixpkgs registry; bootstrap.py records its exact locked source in runtime.json.
let
  pkgs = import (builtins.getFlake nixpkgs).outPath { system = "x86_64-linux"; };
  tools = pkgs.symlinkJoin {
    name = "screech-jepsen-tools";
    paths = with pkgs; [ qemu leiningen iproute2 iptables fuse3 gnuplot graphviz
                        cmake pkg-config gtest spdlog tomlplusplus zstd git getent strace ];
  };
in pkgs.writeText "screech-jepsen-runtime.json" (builtins.toJSON {
  tools = "${tools}";
  java = "${pkgs.jdk21}/bin/java";
  lein = "${pkgs.leiningen}/bin/lein";
  busybox = "${pkgs.pkgsStatic.busybox}/bin/busybox";
  kernel = "${pkgs.linux}/bzImage";
  modules = "${pkgs.linux.modules}";
  modprobe = "${pkgs.kmod}/bin/modprobe";
  mkfs = "${pkgs.e2fsprogs}/bin/mkfs.ext4";
  python = "${pkgs.python3}/bin/python3";
  bash = "${pkgs.bash}/bin/bash";
  fuse_dev = "${pkgs.fuse3.dev}";
  fuse_lib = "${pkgs.fuse3.out}";
})
