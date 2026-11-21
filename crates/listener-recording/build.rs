use ethos_zero::{Actualizing, File, Generating, Potential};
fn main() {
    let mut native = cc::Build::new();
    native.file("native/recording.c");
    println!("cargo:rerun-if-changed=native/recording.c");
    println!("cargo:rerun-if-changed=native/recording.h");
    for dependency in ["libpulse", "opus", "ogg"] {
        let library = pkg_config::Config::new()
            .cargo_metadata(false)
            .probe(dependency)
            .expect("native recording dependency");
        native.includes(library.include_paths);
    }
    native.compile("listener_recording");
    for dependency in ["libpulse", "opus", "ogg"] {
        pkg_config::probe_library(dependency).expect("native recording dependency");
    }
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    let source = std::fs::read_to_string(root.join("ethos/recording.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .expect("checked Ethos");
    let generated = file.generate().expect("generated contract");
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated.rs")).expect("committed generation")
    );
}
