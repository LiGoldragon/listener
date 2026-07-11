{
  description = "Listener speech-to-text CLI and supervised daemon scaffold.";

  inputs = {
    nixpkgs.url = "github:LiGoldragon/nixpkgs?ref=main";
    fenix.url = "github:nix-community/fenix";
    fenix.inputs.nixpkgs.follows = "nixpkgs";
    crane.url = "github:ipetkov/crane";
  };

  outputs =
    {
      self,
      nixpkgs,
      fenix,
      crane,
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      forSystems = function: nixpkgs.lib.genAttrs systems (system: function system);

      contextFor =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          toolchain = fenix.packages.${system}.stable.withComponents [
            "cargo"
            "rustc"
            "rustfmt"
            "clippy"
            "rust-src"
            "rust-analyzer"
          ];
          craneLib = (crane.mkLib pkgs).overrideToolchain toolchain;
          src =
            let
              transcriptionFixtureSource =
                path: type: type == "regular" && pkgs.lib.hasPrefix "${toString ./tests/fixtures}/" path;
            in
            pkgs.lib.cleanSourceWith {
              src = ./.;
              filter =
                path: type:
                type == "directory"
                || transcriptionFixtureSource path type
                || craneLib.filterCargoSources path type;
              name = "source";
            };
          commonArgs = {
            inherit src;
            strictDeps = true;
            nativeBuildInputs = [ pkgs.ffmpeg ];
          };
          cargoArtifacts = craneLib.buildDepsOnly commonArgs;
        in
        {
          inherit
            pkgs
            toolchain
            craneLib
            src
            commonArgs
            cargoArtifacts
            ;
        };
    in
    {
      packages = forSystems (
        system:
        let
          context = contextFor system;
        in
        {
          default = context.craneLib.buildPackage (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              doCheck = false;
              pname = "listener";
              nativeBuildInputs = [ context.pkgs.makeWrapper ];
              postFixup = ''
                wrapProgram $out/bin/listener-daemon --prefix PATH : ${context.pkgs.ffmpeg}/bin
              '';
              meta.mainProgram = "listener";
            }
          );
        }
      );

      checks = forSystems (
        system:
        let
          context = contextFor system;
        in
        {
          build = context.craneLib.cargoBuild (context.commonArgs // { inherit (context) cargoArtifacts; });
          test = context.craneLib.cargoTest (context.commonArgs // { inherit (context) cargoArtifacts; });
          test-configuration = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test configuration";
            }
          );
          test-transcription = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test transcription";
            }
          );
          test-history = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test history";
            }
          );
          test-recall = context.craneLib.cargoTest (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoTestExtraArgs = "--test recall";
            }
          );
          doc = context.craneLib.cargoDoc (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              RUSTDOCFLAGS = "-D warnings";
            }
          );
          fmt = context.craneLib.cargoFmt { inherit (context) src; };
          clippy = context.craneLib.cargoClippy (
            context.commonArgs
            // {
              inherit (context) cargoArtifacts;
              cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
            }
          );
        }
      );

      apps = forSystems (
        system:
        let
          package = self.packages.${system}.default;
        in
        {
          default = {
            type = "app";
            program = "${package}/bin/listener";
          };
          daemon = {
            type = "app";
            program = "${package}/bin/listener-daemon";
          };
          recall = {
            type = "app";
            program = "${package}/bin/listener-recall";
          };
          transcription-customization = {
            type = "app";
            program = "${package}/bin/listener-transcription-customization";
          };
          meta = {
            type = "app";
            program = "${package}/bin/meta-listener";
          };
        }
      );

      devShells = forSystems (
        system:
        let
          context = contextFor system;
        in
        {
          default = context.pkgs.mkShell {
            packages = [
              context.toolchain
              context.pkgs.jujutsu
              context.pkgs.nix
            ];
          };
        }
      );
    };
}
