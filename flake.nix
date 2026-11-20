{
  description = "Listener version one: pure contracts and fresh Memory candidate";
  inputs.base.url = "github:LiGoldragon/build-base";
  outputs = { self, base }: base.lib.workspaceOutputs {
    cargoNix = ./Cargo.nix;
    memberChecks = { check, ... }: {
      listener-emission = check "listener-emission" [ "datom" ];
      listener-recording = check "listener-recording" [ "datom" ];
    };
    extraChecks = { pkgs, toolchain, ... }: {
      fmt = pkgs.runCommand "listener-memory-format" { nativeBuildInputs = [ toolchain ]; } ''
        rustfmt --edition 2024 --check ${./crates/listener-memory/src}/lib.rs ${./crates/listener-memory/src}/store.rs ${./crates/listener-memory/src}/transition.rs ${./crates/listener-memory/build.rs}
        rustfmt --edition 2024 --check ${./crates/listener-recording/src}/lib.rs ${./crates/listener-recording/build.rs} ${./crates/listener-emission/src}/lib.rs ${./crates/listener-emission/build.rs}
        touch $out
      '';
    };
  };
}
