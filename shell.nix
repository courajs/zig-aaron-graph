# Todo: import default.nix so they don't get out of sync
{ sources ? import ./nix/sources.nix }:
let nixpkgs  = import sources.nixpkgs {}; in
nixpkgs.stdenv.mkDerivation rec {
  pname = "zig-connected-components";
  version = "0.1.0";

  zig = nixpkgs.zig;
  gdb = nixpkgs.gdb;
  buildInputs = [ zig gdb ];

  src = ./.;
  builder = ./builder.sh;
}
