{ pkgs ? import <nixpkgs> {} }:
pkgs.writeShellApplication {
  name = "tickr-hello";
  text = ''
    echo "$*"
  '';
}
