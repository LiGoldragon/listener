use listener::CommandLine;

fn main() {
    let command_line = CommandLine::from_environment();
    if let Err(error) = command_line.run(std::io::stdout().lock()) {
        eprintln!("listener: {error}");
        std::process::exit(1);
    }
}
