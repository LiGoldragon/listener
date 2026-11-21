use ethos_zero::{Actualizing, File, Generating, Potential};
fn main() {
    let root = std::path::PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").expect("manifest"));
    let source = std::fs::read_to_string(root.join("ethos/startup.ethos")).expect("source");
    let file = Potential::<File>::from(source)
        .actualize()
        .expect("checked Ethos");
    let generated = file.generate().expect("generated contract");
    assert_eq!(
        generated,
        std::fs::read_to_string(root.join("src/generated.rs")).expect("committed generation")
    );
}
