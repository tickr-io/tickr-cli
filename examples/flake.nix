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
          prebuiltGo = builtins.toString ./polyglot + "/bin/tickr-polyglot-go";
          prebuiltRust = builtins.toString ./polyglot + "/bin/tickr-polyglot-rust";
          installPrebuilt = name: source: pkgs.runCommand name { } ''
            mkdir -p "$out/bin"
            install -m755 ${source} "$out/bin/${name}"
          '';
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
          polyglotPython = pkgs.writeShellApplication {
            name = "tickr-polyglot-python";
            runtimeInputs = [ pkgs.python3 ];
            text = ''
              exec python3 ${./polyglot/greet.py}
            '';
          };
          polyglotJavaScript = pkgs.writeShellApplication {
            name = "tickr-polyglot-javascript";
            runtimeInputs = [ pkgs.nodejs ];
            text = ''
              exec node ${./polyglot/greet.js}
            '';
          };
          polyglotGo =
            if builtins.pathExists prebuiltGo then
              installPrebuilt "tickr-polyglot-go" prebuiltGo
            else
              pkgs.runCommand "tickr-polyglot-go" {
                nativeBuildInputs = [ pkgs.go ];
              } ''
                export HOME="$TMPDIR"
                export CGO_ENABLED=0
                mkdir -p "$out/bin"
                go build -trimpath -ldflags="-s -w" \
                  -o "$out/bin/tickr-polyglot-go" ${./polyglot/greet.go}
              '';
          polyglotRust =
            if builtins.pathExists prebuiltRust then
              installPrebuilt "tickr-polyglot-rust" prebuiltRust
            else
              pkgs.stdenv.mkDerivation {
                pname = "tickr-polyglot-rust";
                version = "1";
                dontUnpack = true;
                nativeBuildInputs = [ pkgs.rustc ];
                buildPhase = ''
                  rustc --edition=2021 -C opt-level=s -C strip=symbols \
                    -o tickr-polyglot-rust ${./polyglot/greet.rs}
                '';
                installPhase = ''
                  mkdir -p "$out/bin"
                  install -m755 tickr-polyglot-rust "$out/bin/tickr-polyglot-rust"
                '';
              };
          default = hello;
        };
    in {
      packages = forEachSystem packagesFor;
    };
}
