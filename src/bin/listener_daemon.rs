use listener::ListenerDaemon;

fn main() -> std::process::ExitCode {
    ListenerDaemon::run_to_exit_code()
}
