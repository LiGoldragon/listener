use listener::MetaCommandLine;

fn main() {
    let command_line = MetaCommandLine::from_environment();
    if let Err(error) = command_line.run(std::io::stdout().lock()) {
        eprintln!("meta-listener: {error}");
        std::process::exit(1);
    }
}
