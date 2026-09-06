use std::{
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

fn start_login_options_server(
    login_options_body: &str,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind login-options server");
    listener.set_nonblocking(true).expect("set login-options listener nonblocking");
    let server_url = format!("http://{}", listener.local_addr().expect("server addr"));
    let served = Arc::new(AtomicBool::new(false));
    let served_handle = Arc::clone(&served);
    let body = login_options_body.to_string();

    let handle = thread::spawn(move || {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while std::time::Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut buffer = [0_u8; 4096];
                    let read = stream.read(&mut buffer).expect("read login-options request");
                    let request = String::from_utf8_lossy(&buffer[..read]);
                    assert!(
                        request.contains("GET /v1/api/auth/login-options"),
                        "unexpected request: {request}"
                    );

                    write!(
                        stream,
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: \
                         close\r\nContent-Length: {}\r\n\r\n{}",
                        body.len(),
                        body
                    )
                    .expect("write login-options response");
                    let _ = stream.shutdown(Shutdown::Both);
                    served_handle.store(true, Ordering::SeqCst);
                    return;
                },
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(10));
                },
                Err(error) => panic!("accept login-options request: {error}"),
            }
        }
        panic!("timed out waiting for login-options request");
    });

    (server_url, served, handle)
}

#[test]
fn cli_local_login_explains_disabled_policy_without_password_prompt() {
    let login_options_body = r#"{"local":{"enabled":false},"oidc":null}"#;
    let (server_url, served, server) = start_login_options_server(login_options_body);

    let temp_home = tempfile::tempdir().expect("temp home");
    let credentials_path = temp_home.path().join("credentials.toml");
    let output = std::process::Command::new(crate::common::kalam_bin())
        .arg("--url")
        .arg(&server_url)
        .arg("--timeout")
        .arg("5")
        .arg("login")
        .env("HOME", temp_home.path())
        .env("USERPROFILE", temp_home.path())
        .env("KALAMDB_CREDENTIALS_PATH", &credentials_path)
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1")
        .env_remove("HTTP_PROXY")
        .env_remove("http_proxy")
        .env_remove("HTTPS_PROXY")
        .env_remove("https_proxy")
        .env_remove("ALL_PROXY")
        .env_remove("all_proxy")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output()
        .expect("run login against disabled local policy");

    assert!(
        !output.status.success(),
        "login should fail when local auth is disabled\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("local username/password login is disabled; use `kalam login --oidc`"),
        "expected disabled-policy message, got stderr: {stderr}"
    );

    server.join().expect("join login-options server");
    assert!(served.load(Ordering::SeqCst), "login-options server should have been contacted");
}
