//! Broker command-line flag reconciliation.
//!
//! The broker keeps the existing `LMX_*` environment contract as its runtime
//! configuration API. CLI flags are parsed through the native `flags2env`
//! parser and reconciled into that same env-shaped map, with CLI values taking
//! precedence over process environment values.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::{Path, PathBuf};

pub const CLI_FLAGS_CONFIG_ENV: &str = "LMX_CLI_FLAGS_CONFIG";
pub const CLI_FLAGS_FILE_NAME: &str = ".cli-flags.toml";
pub const DEFAULT_ETC_CLI_FLAGS_CONFIG_PATH: &str = "/etc/dd-rust-network-mutex/.cli-flags.toml";

const PARSE_ERRORS_ENV: &str = "LMX_CLI_PARSE_ERRORS";
const POSITIONALS_ENV: &str = "LMX_CLI_POSITIONALS";
const UNKNOWN_OPTIONS_ENV: &str = "LMX_CLI_UNKNOWN_OPTIONS";

unsafe extern "C" {
    fn f2e_parse_json_argv_from_file(
        config_path: *const c_char,
        argv_json: *const c_char,
    ) -> *mut c_char;
    fn f2e_help_table_from_file(
        config_path: *const c_char,
        command_name: *const c_char,
        terminal_columns: c_int,
    ) -> *mut c_char;
    fn f2e_free(value: *mut c_char);
}

#[derive(Debug, Clone)]
pub enum BrokerCliConfig {
    Run(BrokerCliEnv),
    Help(BrokerCliHelp),
}

#[derive(Debug, Clone)]
pub struct BrokerCliEnv {
    merged_env: BTreeMap<String, String>,
    cli_overrides: BTreeMap<String, String>,
    source_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
pub struct BrokerCliHelp {
    table: String,
    source_path: PathBuf,
}

#[derive(Debug, thiserror::Error)]
pub enum CliFlagError {
    #[error("failed to serialize broker argv for flags2env: {source}")]
    ArgsJson { source: serde_json::Error },
    #[error(
        "failed to parse flags2env JSON output while {operation}: {source}; raw output: {raw}"
    )]
    NativeJson {
        operation: &'static str,
        raw: String,
        source: serde_json::Error,
    },
    #[error("failed to parse flags2env metadata env {key}: {source}; value: {value}")]
    MetadataJson {
        key: &'static str,
        value: String,
        source: serde_json::Error,
    },
    #[error("broker CLI config path is not valid UTF-8: {path:?}")]
    NonUtf8ConfigPath { path: PathBuf },
    #[error("broker CLI config path contains an interior NUL byte: {path:?}")]
    ConfigPathNul { path: PathBuf },
    #[error("broker CLI command name contains an interior NUL byte: {value:?}")]
    CommandNameNul { value: String },
    #[error("flags2env returned a null pointer while {operation}")]
    NativeNull { operation: &'static str },
    #[error(
        "no .cli-flags.toml found for broker CLI arguments; set LMX_CLI_FLAGS_CONFIG or install /etc/dd-rust-network-mutex/.cli-flags.toml"
    )]
    MissingConfig,
    #[error("broker CLI flags config path does not exist: {path:?}")]
    MissingConfigPath { path: PathBuf },
    #[error("invalid broker CLI flag value(s): {0}")]
    ParseErrors(String),
    #[error("unknown broker CLI option(s): {0}")]
    UnknownOptions(String),
    #[error("unexpected broker positional argument(s): {0}")]
    Positionals(String),
}

impl BrokerCliEnv {
    pub fn get(&self, key: &str) -> Option<&str> {
        self.merged_env.get(key).map(String::as_str)
    }

    pub fn merged_env(&self) -> &BTreeMap<String, String> {
        &self.merged_env
    }

    pub fn cli_overrides(&self) -> &BTreeMap<String, String> {
        &self.cli_overrides
    }

    pub fn source_path(&self) -> Option<&Path> {
        self.source_path.as_deref()
    }

    pub fn apply_cli_overrides_to_process_env(&self) {
        for (key, value) in &self.cli_overrides {
            std::env::set_var(key, value);
        }
    }
}

impl BrokerCliHelp {
    pub fn table(&self) -> &str {
        &self.table
    }

    pub fn source_path(&self) -> &Path {
        &self.source_path
    }
}

pub fn load_broker_cli_config() -> Result<BrokerCliConfig, CliFlagError> {
    load_broker_cli_config_from(std::env::args().collect(), std::env::vars())
}

fn load_broker_cli_config_from<I, K, V>(
    args: Vec<String>,
    env: I,
) -> Result<BrokerCliConfig, CliFlagError>
where
    I: IntoIterator<Item = (K, V)>,
    K: Into<String>,
    V: Into<String>,
{
    let process_env = env
        .into_iter()
        .map(|(key, value)| (key.into(), value.into()))
        .collect::<BTreeMap<_, _>>();

    let user_args = args.iter().skip(1).cloned().collect::<Vec<_>>();
    let config_path = resolve_cli_flags_config_path(&process_env);

    if config_path.is_none() {
        if user_args.is_empty() {
            return Ok(BrokerCliConfig::Run(BrokerCliEnv {
                merged_env: process_env,
                cli_overrides: BTreeMap::new(),
                source_path: None,
            }));
        }
        return Err(CliFlagError::MissingConfig);
    }

    let config_path = config_path.expect("checked above");
    if !config_path.exists() {
        return Err(CliFlagError::MissingConfigPath { path: config_path });
    }
    if args.iter().any(|arg| arg == "--help") {
        return Ok(BrokerCliConfig::Help(BrokerCliHelp {
            table: render_help_table(&config_path, command_name(&args)?)?,
            source_path: config_path,
        }));
    }

    let mut cli_overrides = parse_cli_overrides(&config_path, &user_args)?;
    validate_parser_metadata(&mut cli_overrides)?;

    let mut merged_env = process_env;
    for (key, value) in &cli_overrides {
        merged_env.insert(key.clone(), value.clone());
    }

    Ok(BrokerCliConfig::Run(BrokerCliEnv {
        merged_env,
        cli_overrides,
        source_path: Some(config_path),
    }))
}

fn resolve_cli_flags_config_path(env: &BTreeMap<String, String>) -> Option<PathBuf> {
    if let Some(path) = env
        .get(CLI_FLAGS_CONFIG_ENV)
        .map(|v| v.trim())
        .filter(|v| !v.is_empty())
        .map(PathBuf::from)
    {
        return Some(path);
    }

    find_upward_cli_flags_config().or_else(|| existing_path(DEFAULT_ETC_CLI_FLAGS_CONFIG_PATH))
}

fn find_upward_cli_flags_config() -> Option<PathBuf> {
    let mut dir = std::env::current_dir().ok()?;
    let home = std::env::var_os("HOME").map(PathBuf::from);

    loop {
        if home.as_ref().is_some_and(|home| home == &dir) {
            return None;
        }

        let candidate = dir.join(CLI_FLAGS_FILE_NAME);
        if candidate.exists() {
            return Some(candidate);
        }

        if !dir.pop() {
            return None;
        }
    }
}

fn existing_path(path: &str) -> Option<PathBuf> {
    let path = PathBuf::from(path);
    path.exists().then_some(path)
}

fn parse_cli_overrides(
    config_path: &Path,
    user_args: &[String],
) -> Result<BTreeMap<String, String>, CliFlagError> {
    let argv_json =
        serde_json::to_string(user_args).map_err(|source| CliFlagError::ArgsJson { source })?;
    let config_path = cstring_path(config_path)?;
    let argv_json = CString::new(argv_json).expect("serde_json escaped interior NUL bytes");
    let raw = unsafe {
        take_owned_c_string(
            f2e_parse_json_argv_from_file(config_path.as_ptr(), argv_json.as_ptr()),
            "parsing broker CLI flags",
        )?
    };

    serde_json::from_str(&raw).map_err(|source| CliFlagError::NativeJson {
        operation: "parsing broker CLI flags",
        raw,
        source,
    })
}

fn render_help_table(config_path: &Path, command_name: String) -> Result<String, CliFlagError> {
    let config_path = cstring_path(config_path)?;
    let command_name =
        CString::new(command_name.clone()).map_err(|_| CliFlagError::CommandNameNul {
            value: command_name,
        })?;
    let columns = terminal_columns();
    unsafe {
        take_owned_c_string(
            f2e_help_table_from_file(config_path.as_ptr(), command_name.as_ptr(), columns),
            "rendering broker CLI help",
        )
    }
}

fn validate_parser_metadata(
    cli_overrides: &mut BTreeMap<String, String>,
) -> Result<(), CliFlagError> {
    let parse_errors = take_json_array(cli_overrides, PARSE_ERRORS_ENV)?;
    let unknown_options = take_json_array(cli_overrides, UNKNOWN_OPTIONS_ENV)?;
    let positionals = take_json_array(cli_overrides, POSITIONALS_ENV)?;

    if !parse_errors.is_empty() {
        return Err(CliFlagError::ParseErrors(format_items(&parse_errors)));
    }
    if !unknown_options.is_empty() {
        return Err(CliFlagError::UnknownOptions(format_items(&unknown_options)));
    }
    if !positionals.is_empty() {
        return Err(CliFlagError::Positionals(format_items(&positionals)));
    }

    Ok(())
}

fn take_json_array(
    map: &mut BTreeMap<String, String>,
    key: &'static str,
) -> Result<Vec<String>, CliFlagError> {
    let Some(value) = map.remove(key) else {
        return Ok(Vec::new());
    };

    serde_json::from_str(&value).map_err(|source| CliFlagError::MetadataJson { key, value, source })
}

fn cstring_path(path: &Path) -> Result<CString, CliFlagError> {
    let value = path
        .to_str()
        .ok_or_else(|| CliFlagError::NonUtf8ConfigPath {
            path: path.to_path_buf(),
        })?;
    CString::new(value).map_err(|_| CliFlagError::ConfigPathNul {
        path: path.to_path_buf(),
    })
}

unsafe fn take_owned_c_string(
    ptr: *mut c_char,
    operation: &'static str,
) -> Result<String, CliFlagError> {
    if ptr.is_null() {
        return Err(CliFlagError::NativeNull { operation });
    }
    let value = unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned();
    unsafe { f2e_free(ptr) };
    Ok(value)
}

fn terminal_columns() -> c_int {
    std::env::var("COLUMNS")
        .ok()
        .and_then(|value| value.trim().parse::<c_int>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100)
}

fn command_name(args: &[String]) -> Result<String, CliFlagError> {
    let raw = args
        .first()
        .map(String::as_str)
        .unwrap_or("dd-rust-network-mutex");
    let name = Path::new(raw)
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or(raw)
        .to_string();
    if name.contains('\0') {
        return Err(CliFlagError::CommandNameNul { value: name });
    }
    Ok(name)
}

fn format_items(items: &[String]) -> String {
    items.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest_cli_config() -> String {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(CLI_FLAGS_FILE_NAME)
            .to_string_lossy()
            .into_owned()
    }

    fn env_with_manifest_config(extra: &[(&str, &str)]) -> Vec<(String, String)> {
        let mut env = vec![(CLI_FLAGS_CONFIG_ENV.to_string(), manifest_cli_config())];
        env.extend(
            extra
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string())),
        );
        env
    }

    #[test]
    fn cli_flags_override_env_values() {
        let cfg = load_broker_cli_config_from(
            vec![
                "dd-rust-network-mutex".into(),
                "--tcp-port".into(),
                "7777".into(),
                "--disable-http".into(),
            ],
            env_with_manifest_config(&[("LMX_TCP_PORT", "6970"), ("LMX_DISABLE_HTTP", "false")]),
        )
        .expect("cli config");

        let BrokerCliConfig::Run(env) = cfg else {
            panic!("expected run config");
        };

        assert_eq!(env.get("LMX_TCP_PORT"), Some("7777"));
        assert_eq!(env.get("LMX_DISABLE_HTTP"), Some("true"));
        assert_eq!(
            env.cli_overrides().get("LMX_TCP_PORT"),
            Some(&"7777".into())
        );
        assert_eq!(
            env.cli_overrides().get("LMX_DISABLE_HTTP"),
            Some(&"true".into())
        );
    }

    #[test]
    fn env_values_remain_when_cli_omits_them() {
        let cfg = load_broker_cli_config_from(
            vec!["dd-rust-network-mutex".into()],
            env_with_manifest_config(&[("LMX_HTTP_PORT", "7971")]),
        )
        .expect("cli config");

        let BrokerCliConfig::Run(env) = cfg else {
            panic!("expected run config");
        };

        assert_eq!(env.get("LMX_HTTP_PORT"), Some("7971"));
        assert!(env.cli_overrides().is_empty());
    }

    #[test]
    fn unknown_options_are_rejected() {
        let err = load_broker_cli_config_from(
            vec!["dd-rust-network-mutex".into(), "--not-a-real-flag".into()],
            env_with_manifest_config(&[]),
        )
        .expect_err("unknown flag should fail");

        assert!(matches!(err, CliFlagError::UnknownOptions(_)));
    }

    #[test]
    fn invalid_typed_values_are_rejected() {
        let err = load_broker_cli_config_from(
            vec![
                "dd-rust-network-mutex".into(),
                "--tcp-port".into(),
                "not-a-port".into(),
            ],
            env_with_manifest_config(&[]),
        )
        .expect_err("invalid integer flag should fail");

        assert!(matches!(err, CliFlagError::ParseErrors(_)));
    }

    #[test]
    fn explicit_missing_cli_flags_config_is_rejected() {
        let missing = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/definitely-missing-cli-flags.toml")
            .to_string_lossy()
            .into_owned();
        let err = load_broker_cli_config_from(
            vec![
                "dd-rust-network-mutex".into(),
                "--tcp-port".into(),
                "7970".into(),
            ],
            vec![(CLI_FLAGS_CONFIG_ENV.to_string(), missing)],
        )
        .expect_err("missing explicit config should fail");

        assert!(matches!(err, CliFlagError::MissingConfigPath { .. }));
    }

    #[test]
    fn help_is_rendered_from_cli_flags_config() {
        let cfg = load_broker_cli_config_from(
            vec!["dd-rust-network-mutex".into(), "--help".into()],
            env_with_manifest_config(&[]),
        )
        .expect("help config");

        let BrokerCliConfig::Help(help) = cfg else {
            panic!("expected help config");
        };

        assert!(help.table().contains("--tcp-port"));
        assert!(help.table().contains("LMX_TCP_PORT"));
    }
}
