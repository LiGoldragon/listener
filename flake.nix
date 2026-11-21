{
  description = "Listener version one: capture, hearing, delivery and fresh Memory";
  inputs.base.url = "github:LiGoldragon/build-base";
  outputs = { self, base }: base.lib.workspaceOutputs {
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
}
