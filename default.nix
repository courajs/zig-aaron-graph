{ sources ? import ./nix/sources.nix }:
let nixpkgs  = import sources.nixpkgs {}; in
nixpkgs.stdenv.mkDerivation rec {
  pname = "zig-connected-components";
  version = "0.1.0";

  zig = nixpkgs.zig;
  buildInputs = [ zig ];

  src = ./.;
  builder = ./builder.sh;
}
