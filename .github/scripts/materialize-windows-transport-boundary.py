#!/usr/bin/env python3
from pathlib import Path


def replace_once(text: str, old: str, new: str, label: str) -> str:
    if text.count(old) != 1 or text.count(new) != 0:
        raise SystemExit(f"unexpected {label} state")
    return text.replace(old, new, 1)


client = Path("src/client.rs")
text = client.read_text(encoding="utf-8")
text = replace_once(
    text,
    "use tokio::net::{TcpStream, UnixStream};",
    "use tokio::net::TcpStream;\n#[cfg(unix)]\nuse tokio::net::UnixStream;",
    "client transport import",
)
text = replace_once(
    text,
    "    pub async fn connect_uds(\n",
    "    #[cfg(unix)]\n    pub async fn connect_uds(\n",
    "client UDS method",
)
start_marker = (
    "    async fn start<S>(stream: S, config: ClientConfig) "
    "-> Result<Self, ClientError>\n"
)
windows_stub = """    #[cfg(not(unix))]
    pub async fn connect_uds(
        path: impl AsRef<Path>,
        config: ClientConfig,
    ) -> Result<Self, ClientError> {
        crate::routine_id!("ddl-routine-client-uds-unsupported-windows-P4n");
        let _ = (path.as_ref(), config);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "Unix-domain sockets are unavailable on this platform; use connect_tcp",
        )
        .into())
    }

"""
if text.count(start_marker) != 1 or windows_stub in text:
    raise SystemExit("unexpected client start/stub state")
text = text.replace(start_marker, windows_stub + start_marker, 1)
client.write_text(text, encoding="utf-8")

server = Path("src/server.rs")
text = server.read_text(encoding="utf-8")
text = replace_once(
    text,
    "use tokio::net::{TcpListener, TcpStream, UnixListener, UnixStream};",
    "use tokio::net::{TcpListener, TcpStream};\n"
    "#[cfg(unix)]\n"
    "use tokio::net::{UnixListener, UnixStream};",
    "server transport import",
)
old_fd = """                        let fd: std::os::fd::RawFd = {
                            use std::os::fd::AsRawFd;
                            sock.as_raw_fd()
                        };"""
text = replace_once(
    text,
    old_fd,
    "                        let fd = crate::sockopt::socket_handle(&sock);",
    "accepted-socket handle",
)
text = replace_once(
    text,
    "        fd: std::os::fd::RawFd,",
    "        fd: crate::sockopt::SocketHandle,",
    "AfterRead handle",
)
text = replace_once(
    text,
    "    if let Some(path) = config.uds_path.clone() {\n",
    "    #[cfg(unix)]\n    if let Some(path) = config.uds_path.clone() {\n",
    "server UDS listener",
)
status_marker = "    let status_info = Arc::new(build_status_info(&config));\n"
windows_uds = """    #[cfg(not(unix))]
    if let Some(path) = config.uds_path.as_ref() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            format!(
                "Unix-domain socket listener {} is unavailable on this platform",
                path.display()
            ),
        ));
    }

"""
if text.count(status_marker) != 1 or windows_uds in text:
    raise SystemExit("unexpected server status/Windows UDS state")
text = text.replace(status_marker, windows_uds + status_marker, 1)
text = replace_once(
    text,
    "#[allow(dead_code)]\nasync fn _ensure_uds_handler_compiles(\n",
    "#[cfg(unix)]\n#[allow(dead_code)]\nasync fn _ensure_uds_handler_compiles(\n",
    "UDS helper",
)
server.write_text(text, encoding="utf-8")

sockopt = Path("src/sockopt.rs")
text = sockopt.read_text(encoding="utf-8")
old_import = """#[cfg(any(test, feature = "tls"))]
#[allow(unused_imports)]
use std::os::fd::AsRawFd;
"""
new_import = """#[cfg(unix)]
use std::os::fd::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

#[cfg(unix)]
pub type SocketHandle = RawFd;
#[cfg(windows)]
pub type SocketHandle = RawSocket;

/// Capture the platform-native socket handle before a stream is moved into
/// an optional TLS wrapper. QUICKACK uses it only on Linux; other platforms
/// retain the handle solely so the shared read hook remains portable.
pub fn socket_handle(stream: &tokio::net::TcpStream) -> SocketHandle {
    crate::routine_id!("ddl-routine-socket-handle-portable-W7m");
    #[cfg(unix)]
    {
        stream.as_raw_fd()
    }
    #[cfg(windows)]
    {
        stream.as_raw_socket()
    }
}
"""
if text.count(old_import) != 1 or "pub type SocketHandle" in text:
    raise SystemExit("unexpected sockopt import/handle state")
text = text.replace(old_import, new_import, 1)
text = replace_once(
    text,
    "pub fn apply_quickack(_fd: std::os::fd::RawFd) -> io::Result<bool> {",
    "pub fn apply_quickack(_fd: SocketHandle) -> io::Result<bool> {",
    "QUICKACK signature",
)
text = replace_once(
    text,
    "        let fd = stream.as_raw_fd();",
    "        let fd = socket_handle(&stream);",
    "QUICKACK test handle",
)
sockopt.write_text(text, encoding="utf-8")
