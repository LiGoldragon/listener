{ pkgs }:
let
  python = pkgs.python3.withPackages (packages: [ packages.websockets ]);
in
pkgs.writeShellApplication {
  name = "listener-openai-realtime-transcription";
  text = ''
    export SSL_CERT_FILE="''${SSL_CERT_FILE:-${pkgs.cacert}/etc/ssl/certs/ca-bundle.crt}"
    exec ${python}/bin/python3 ${./openai_realtime_transcription.py} --decoder "${pkgs.ffmpeg}/bin/ffmpeg" "$@"
  '';
}
