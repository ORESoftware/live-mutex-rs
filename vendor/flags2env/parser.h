#ifndef F2E_PARSER_H
#define F2E_PARSER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

#define F2E_VERSION "0.1.0"

#if defined(__clang__) || defined(__GNUC__)
#define F2E_WARN_UNUSED_RESULT __attribute__((warn_unused_result))
#else
#define F2E_WARN_UNUSED_RESULT
#endif

#ifndef __has_attribute
#define __has_attribute(attribute_name) 0
#endif

#if defined(__clang__) && __has_attribute(ownership_returns) && __has_attribute(ownership_takes)
#define F2E_OWNED_RESULT __attribute__((ownership_returns(malloc))) F2E_WARN_UNUSED_RESULT
#define F2E_TAKES_OWNED_ARG_1 __attribute__((ownership_takes(malloc, 1)))
#else
#define F2E_OWNED_RESULT F2E_WARN_UNUSED_RESULT
#define F2E_TAKES_OWNED_ARG_1
#endif

const char *f2e_version(void);

/*
 * Parses argv using the nearest .cli-flags.toml found by walking upward from
 * the current working directory. Refuses to use $HOME/.cli-flags.toml. Returns
 * a heap-allocated JSON object string. Call f2e_free() with the returned pointer.
 */
char *f2e_parse(int argc, const char *const argv[]) F2E_OWNED_RESULT;

/*
 * Parses argv using an explicit TOML config path and returns a heap-allocated
 * JSON object string. Call f2e_free() with the returned pointer.
 */
char *f2e_parse_from_file(const char *config_path, int argc, const char *const argv[]) F2E_OWNED_RESULT;

/*
 * Parses the current process command line using the nearest .cli-flags.toml
 * where the host OS exposes process argv. Explicit f2e_parse(...) is still
 * preferred when the caller has already adjusted, sliced, or synthesized argv.
 */
char *f2e_parse_process(void) F2E_OWNED_RESULT;

/*
 * Parses the current process command line using an explicit TOML config path.
 */
char *f2e_parse_process_from_file(const char *config_path) F2E_OWNED_RESULT;

/*
 * FFI-friendly entrypoint: argv_json must be a JSON array of strings.
 * Returns a heap-allocated JSON object string. Call f2e_free().
 */
char *f2e_parse_json_argv(const char *argv_json) F2E_OWNED_RESULT;

/*
 * FFI-friendly entrypoint with an explicit config path.
 */
char *f2e_parse_json_argv_from_file(const char *config_path, const char *argv_json) F2E_OWNED_RESULT;

/*
 * Detects the exact --help token without consuming or parsing other flags.
 * Language clients can use this to expose lazy help-menu behavior.
 */
int f2e_is_help_requested(int argc, const char *const argv[]) F2E_WARN_UNUSED_RESULT;
int f2e_is_help_requested_json_argv(const char *argv_json) F2E_WARN_UNUSED_RESULT;

/*
 * Structured parse: every channel is returned separately instead of packed
 * into env keys, so nothing can be shadowed by real environment variables:
 *   {"flags":{...},"providedFlags":{...},"command":"remote add",
 *    "subcommands":["remote","add"],"extras":["abc"],
 *    "unknownOptions":[],"errors":[]}
 * "flags" is the same default-bearing env map f2e_parse returns.
 * "providedFlags" contains only argv-derived values and command markers, so
 * callers can merge it over the process environment before schema coercion.
 * "extras" holds operand tokens: positionals after the last matched command
 * (including tokens after a bare --); with no command matched, every
 * positional except argv[0].
 * "unknownOptions" and "errors" are collected regardless of the [parse]
 * *_env settings. Returns a heap-allocated string; call f2e_free().
 */
char *f2e_parse_structured(int argc, const char *const argv[]) F2E_OWNED_RESULT;
char *f2e_parse_structured_from_file(const char *config_path, int argc, const char *const argv[]) F2E_OWNED_RESULT;
char *f2e_parse_structured_json_argv(const char *argv_json) F2E_OWNED_RESULT;
char *f2e_parse_structured_json_argv_from_file(const char *config_path, const char *argv_json) F2E_OWNED_RESULT;

/*
 * Resolves the [commands.*] path selected by argv and returns it as its own
 * JSON report, independent of the parsed env map (whose keys can be shadowed
 * by real environment variables): {"path":["remote","add"],"label":"remote add"}.
 * An empty path means argv selected no command or the config declares none.
 * Returns a heap-allocated string; call f2e_free().
 */
char *f2e_resolve_commands(int argc, const char *const argv[]) F2E_OWNED_RESULT;
char *f2e_resolve_commands_from_file(const char *config_path, int argc, const char *const argv[]) F2E_OWNED_RESULT;
char *f2e_resolve_commands_json_argv(const char *argv_json) F2E_OWNED_RESULT;
char *f2e_resolve_commands_json_argv_from_file(const char *config_path, const char *argv_json) F2E_OWNED_RESULT;

/*
 * Generates and prints a terminal-width-aware help table from .cli-flags.toml.
 * Pass terminal_columns <= 0 to auto-detect from $COLUMNS or the active
 * terminal. The returned table is heap-allocated; call f2e_free().
 */
char *f2e_help_table(const char *command_name, int terminal_columns) F2E_OWNED_RESULT;
char *f2e_help_table_from_file(const char *config_path, const char *command_name, int terminal_columns) F2E_OWNED_RESULT;
int f2e_print_table(const char *command_name, int terminal_columns) F2E_WARN_UNUSED_RESULT;
int f2e_print_table_from_file(const char *config_path, const char *command_name, int terminal_columns) F2E_WARN_UNUSED_RESULT;

/*
 * Subcommand-aware help: resolves the [commands.*] path selected by argv
 * (e.g. `git remote add --help`) and renders that command's help table,
 * including its flags, inherited global flags, and nested subcommands.
 * Falls back to the top-level table when argv selects no command.
 */
char *f2e_help_table_for_argv(const char *command_name, int argc, const char *const argv[], int terminal_columns) F2E_OWNED_RESULT;
char *f2e_help_table_for_argv_from_file(const char *config_path, const char *command_name, int argc, const char *const argv[], int terminal_columns) F2E_OWNED_RESULT;
int f2e_print_table_for_argv(const char *command_name, int argc, const char *const argv[], int terminal_columns) F2E_WARN_UNUSED_RESULT;
int f2e_print_table_for_argv_from_file(const char *config_path, const char *command_name, int argc, const char *const argv[], int terminal_columns) F2E_WARN_UNUSED_RESULT;

/*
 * FFI-friendly variants: argv_json must be a JSON array of strings. Invalid
 * argv_json falls back to the top-level help table.
 */
char *f2e_help_table_for_json_argv(const char *command_name, const char *argv_json, int terminal_columns) F2E_OWNED_RESULT;
char *f2e_help_table_for_json_argv_from_file(const char *config_path, const char *command_name, const char *argv_json, int terminal_columns) F2E_OWNED_RESULT;

/*
 * Audits .cli-flags.toml for parse issues, ambiguous aliases, duplicate short
 * flags, env collisions, and boolean value alias conflicts. Returns a
 * heap-allocated JSON report. Call f2e_free().
 */
char *f2e_audit_config(void) F2E_OWNED_RESULT;
char *f2e_audit_config_from_file(const char *config_path) F2E_OWNED_RESULT;
int f2e_audit_config_status(void) F2E_WARN_UNUSED_RESULT;
int f2e_audit_config_status_from_file(const char *config_path) F2E_WARN_UNUSED_RESULT;

/*
 * Generates static shell completion scripts from .cli-flags.toml. The generated
 * scripts are optimized for shell startup/completion speed: they do not invoke
 * flags2env or read TOML at completion time.
 */
char *f2e_completion_script(const char *shell, const char *command_name) F2E_OWNED_RESULT;
char *f2e_completion_script_from_file(const char *config_path, const char *shell, const char *command_name) F2E_OWNED_RESULT;

/*
 * Generates importable types from .cli-flags.toml. Supported language names
 * are typescript, python, go, rust, java, csharp, dart, and json-schema, with
 * common short aliases. type_name defaults to CliConfig when NULL or empty.
 */
char *f2e_generate_types(const char *language, const char *type_name) F2E_OWNED_RESULT;
char *f2e_generate_types_from_file(const char *config_path, const char *language, const char *type_name) F2E_OWNED_RESULT;

/*
 * Coerces declared env keys from a JSON object according to .cli-flags.toml.
 * Runtime flag maps remain string-valued; this explicit boundary returns a
 * JSON report: {"ok":true,"value":{...}} or {"ok":false,"errors":[...]}.
 */
char *f2e_coerce_json(const char *values_json) F2E_OWNED_RESULT;
char *f2e_coerce_json_from_file(const char *config_path, const char *values_json) F2E_OWNED_RESULT;

/*
 * Audits a .env file against the env keys declared by .cli-flags.toml.
 * Unknown .env keys are errors unless ignored by config; declared TOML env
 * keys missing from .env are warnings because they may be optional or supplied
 * elsewhere.
 */
char *f2e_audit_env_file(void) F2E_OWNED_RESULT;
char *f2e_audit_env_file_from_file(const char *config_path, const char *env_path) F2E_OWNED_RESULT;
int f2e_audit_env_file_status(void) F2E_WARN_UNUSED_RESULT;
int f2e_audit_env_file_status_from_file(const char *config_path, const char *env_path) F2E_WARN_UNUSED_RESULT;

void f2e_free(char *value) F2E_TAKES_OWNED_ARG_1;

#ifdef __cplusplus
}
#endif

#endif
