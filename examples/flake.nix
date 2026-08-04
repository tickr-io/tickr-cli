{
  description = "Runnable Tickr onboarding examples";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs = { nixpkgs, ... }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" "aarch64-darwin" ];
      forEachSystem = f: builtins.listToAttrs (map (system: {
        name = system;
        value = f system;
      }) systems);
      packagesFor = system:
        let
          pkgs = import nixpkgs { inherit system; };
          script = name: source: runtimeInputs: pkgs.writeShellApplication {
            inherit name runtimeInputs;
            text = builtins.readFile source;
          };
        in rec {
          hello = pkgs.writeShellApplication {
            name = "tickr-hello";
            text = ''
              echo "$*"
            '';
          };
          choose = script "tickr-example-choose" ./runtime-patch/choose.sh [ pkgs.coreutils ];
          patch = script "tickr-example-patch" ./runtime-patch/patch.sh [ pkgs.coreutils pkgs.curl pkgs.jq ];
          echoPause = script "tickr-example-echo-pause" ./runtime-patch/echo-pause.sh [ pkgs.coreutils ];
          summary = script "tickr-example-summary" ./runtime-patch/summary.sh [ ];
          default = hello;
        };
    in {
      packages = forEachSystem packagesFor;
    };
}
