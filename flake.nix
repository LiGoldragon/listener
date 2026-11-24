{
  description = "Listener version one: capture, hearing, delivery and fresh Memory";
  inputs.base.url = "github:LiGoldragon/build-base";
  outputs = { self, base }:
    let
      workspace = base.lib.workspaceOutputs {
        cargoNix = ./Cargo.nix;
        memberChecks = { check, ... }: {
          listener-emission = check "listener-emission" [ "datom" ];
          listener-recording = check "listener-recording" [ "datom" ];
          listener-place = check "listener-place" [ "datom" ];
        };
        extraChecks = { pkgs, toolchain, ... }: {
          fmt = pkgs.runCommand "listener-format" { nativeBuildInputs = [ toolchain ]; } ''
            find ${./crates} -type f -name '*.rs' ! -name generated.rs ! -path '*/generated/*' \
              -print0 | xargs -0 rustfmt --edition 2024 --check
            touch "$out"
          '';
        };
      };
    in
    workspace // {
      packages = builtins.mapAttrs (system: packages:
        let
          inherit (base.lib.contexts.${system}) pkgs;
          realtime = import ./translator/openai-realtime-transcription.nix { inherit pkgs; };
        in
        packages // {
          listener-openai-realtime-transcription = realtime;
          default = pkgs.symlinkJoin {
            name = "listener-desktop";
            paths = [ packages.default realtime ];
            postBuild = ''
              rm "$out/bin/listener-translator"
              cp -L "${packages.default}/bin/listener-translator" "$out/bin/"
            '';
          };
        }) workspace.packages;
    };
}
