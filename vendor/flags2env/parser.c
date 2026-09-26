#if defined(__linux__) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "parser.h"

#include <ctype.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#if defined(__APPLE__)
#include <sys/ioctl.h>
#include <sys/sysctl.h>
#include <unistd.h>
#elif defined(__unix__)
#include <sys/ioctl.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#include <shellapi.h>
#endif

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

#define F2E_MAX_FLAGS 256
#define F2E_MAX_ALIASES 24
#define F2E_MAX_NAME 96
#define F2E_MAX_ENV 128
#define F2E_MAX_VALUE 1024
#define F2E_MAX_LINE 4096
#define F2E_MAX_LOGICAL_LINE (F2E_MAX_LINE * 32)
#define F2E_MAX_META_PAIRS 4
#define F2E_MAX_COMMANDS 96
#define F2E_MAX_COMMAND_DEPTH 16
#define F2E_SCOPE_ROOT (-1)
/* lenient scope: no subcommand was matched (e.g. a wrapper script stripped
   it), so scoped flags resolve globally when the name is unambiguous */
#define F2E_SCOPE_LENIENT (-2)
#define F2E_MAX_PAIRS (F2E_MAX_FLAGS + F2E_MAX_META_PAIRS + F2E_MAX_COMMANDS)
#define F2E_MAX_ENV_FILE_KEYS 512
#define F2E_DEFAULT_COMMAND_ENV "FLAGS2ENV_COMMAND"

#define F2E_HELP_COL_OPTIONS (1u << 0)
#define F2E_HELP_COL_ENV (1u << 1)
#define F2E_HELP_COL_TYPE (1u << 2)
#define F2E_HELP_COL_DEFAULT (1u << 3)
#define F2E_HELP_COL_DESCRIPTION (1u << 4)
#define F2E_HELP_COL_DEFAULTS \
  (F2E_HELP_COL_OPTIONS | F2E_HELP_COL_ENV | F2E_HELP_COL_TYPE | F2E_HELP_COL_DEFAULT | F2E_HELP_COL_DESCRIPTION)

typedef enum {
  F2E_TYPE_STRING = 0,
  F2E_TYPE_BOOL = 1,
  F2E_TYPE_INT = 2,
  F2E_TYPE_FLOAT = 3,
  F2E_TYPE_JSON = 4,
  F2E_TYPE_ARRAY = 5,
  F2E_TYPE_MAP = 6
} F2EValueType;

typedef struct {
  char name[F2E_MAX_NAME];
  char env[F2E_MAX_ENV];
  char aliases[F2E_MAX_ALIASES][F2E_MAX_NAME];
  size_t alias_count;
  char true_aliases[F2E_MAX_ALIASES][F2E_MAX_NAME];
  size_t true_alias_count;
  char false_aliases[F2E_MAX_ALIASES][F2E_MAX_NAME];
  size_t false_alias_count;
  char short_name;
  F2EValueType type;
  int invalid_type;
  char type_value[F2E_MAX_VALUE];
  int has_default;
  char default_value[F2E_MAX_VALUE];
  char help[F2E_MAX_VALUE];
  int command; /* index into F2EConfig.commands; F2E_SCOPE_ROOT for global flags */
} F2EFlag;

typedef struct {
  char name[F2E_MAX_NAME];
  char aliases[F2E_MAX_ALIASES][F2E_MAX_NAME];
  size_t alias_count;
  char env[F2E_MAX_ENV];
  char help[F2E_MAX_VALUE];
  int parent; /* index into F2EConfig.commands; F2E_SCOPE_ROOT for top-level commands */
  int allow_unknown;
  int allow_unknown_set; /* command overrides [parse] allow_unknown for its scope */
} F2ECommand;

typedef struct {
  F2EFlag flags[F2E_MAX_FLAGS];
  size_t flag_count;
  F2ECommand commands[F2E_MAX_COMMANDS];
  size_t command_count;
  char command_env[F2E_MAX_ENV];
  int too_many_commands;
  char invalid_command_table[F2E_MAX_VALUE];
  int has_invalid_command_table;
  int allow_separated_values;
  int stop_at_first_positional;
  char positionals_env[F2E_MAX_ENV];
  char unknown_options_env[F2E_MAX_ENV];
  char errors_env[F2E_MAX_ENV];
  int allow_unknown;
  char env_audit_ignored_keys[F2E_MAX_ENV_FILE_KEYS][F2E_MAX_ENV];
  size_t env_audit_ignored_count;
  int invalid_env_audit_ignore;
  char help_url[F2E_MAX_VALUE];
  unsigned help_columns;
  int help_columns_configured;
  unsigned help_exclude_columns;
  int invalid_help_columns;
  int invalid_help_exclude_columns;
} F2EConfig;

typedef struct {
  char key[F2E_MAX_ENV];
  char value[F2E_MAX_VALUE];
  int set;
} F2EPair;

typedef struct {
  char *data;
  size_t len;
  size_t cap;
} F2EBuffer;

typedef struct {
  F2EBuffer errors;
  F2EBuffer warnings;
  size_t error_count;
  size_t warning_count;
  int failed;
} F2EAudit;

typedef enum {
  F2E_SECTION_NONE = 0,
  F2E_SECTION_PARSE = 1,
  F2E_SECTION_FLAG = 2,
  F2E_SECTION_HELP = 3,
  F2E_SECTION_ENV_AUDIT = 4,
  F2E_SECTION_COMMAND = 5
} F2EConfigSection;

typedef struct {
  int commands[F2E_MAX_COMMAND_DEPTH];
  size_t depth;
} F2ECommandPath;

/* defined alongside the parse loop; used earlier by help rendering */
static void f2e_resolve_command_path(F2EConfig *config, int argc, const char *const argv[], F2ECommandPath *path_out);
/* defined alongside help rendering; used earlier by completion generation */
static size_t f2e_help_collect_scope_flags(const F2EConfig *config, int scope, size_t out[F2E_MAX_FLAGS]);

typedef struct {
  F2EBuffer buffer;
  size_t count;
  int initialized;
  int failed;
} F2EJsonList;

static size_t f2e_strlcpy(char *dst, const char *src, size_t dst_size) {
  size_t src_len = src ? strlen(src) : 0;
  if (dst_size > 0) {
    size_t copy_len = src_len >= dst_size ? dst_size - 1 : src_len;
    if (copy_len > 0 && src) {
      memcpy(dst, src, copy_len);
    }
    dst[copy_len] = '\0';
  }
  return src_len;
}

static char *f2e_strdup(const char *value) {
  size_t len = value ? strlen(value) : 0;
  char *copy = (char *)malloc(len + 1);
  if (!copy) {
    return NULL;
  }
  if (len > 0 && value) {
    memcpy(copy, value, len);
  }
  copy[len] = '\0';
  return copy;
}

static int f2e_streq(const char *a, const char *b) {
  return strcmp(a, b) == 0;
}

static int f2e_env_name_is_valid(const char *value) {
  if (!value || value[0] == '\0') {
    return 0;
  }
  if (!(isalpha((unsigned char)value[0]) || value[0] == '_')) {
    return 0;
  }
  for (const char *cursor = value + 1; *cursor; cursor++) {
    if (!(isalnum((unsigned char)*cursor) || *cursor == '_')) {
      return 0;
    }
  }
  return 1;
}

static int f2e_option_name_is_valid(const char *value) {
  if (!value || value[0] == '\0') {
    return 0;
  }
  for (const unsigned char *cursor = (const unsigned char *)value; *cursor; cursor++) {
    if (!(isalnum(*cursor) || *cursor == '-' || *cursor == '_' || *cursor == '.')) {
      return 0;
    }
  }
  return value[0] != '-';
}

static int f2e_shell_word_chars_are_valid(const char *value, size_t len) {
  if (!value || len == 0) {
    return 0;
  }
  for (size_t i = 0; i < len; i++) {
    unsigned char ch = (unsigned char)value[i];
    if (!(isalnum(ch) || ch == '-' || ch == '_' || ch == '.')) {
      return 0;
    }
  }
  return 1;
}

static int f2e_shell_word_is_valid(const char *value) {
  return value ? f2e_shell_word_chars_are_valid(value, strlen(value)) : 0;
}

static int f2e_path_basename_copy(const char *value, char *out, size_t out_size) {
  int used_default = !value || value[0] == '\0';
  const char *path = used_default ? "flags2env" : value;
  size_t len = strlen(path);
  while (len > 0 && (path[len - 1] == '/' || path[len - 1] == '\\')) {
    len--;
  }
  if (len == 0) {
    if (!used_default) {
      return 0;
    }
    path = "flags2env";
    len = strlen(path);
  }

  size_t start = 0;
  for (size_t i = len; i > 0; i--) {
    if (path[i - 1] == '/' || path[i - 1] == '\\') {
      start = i;
      break;
    }
  }

  size_t base_len = len - start;
  if (!out || out_size == 0 || base_len == 0 || base_len >= out_size ||
      !f2e_shell_word_chars_are_valid(path + start, base_len)) {
    return 0;
  }
  memcpy(out, path + start, base_len);
  out[base_len] = '\0';
  return 1;
}

static char *f2e_empty_json_object(void);
static const char *f2e_audit_flag_name(const F2EFlag *flag);

static char *f2e_trim_left(char *value) {
  while (*value && isspace((unsigned char)*value)) {
    value++;
  }
  return value;
}

static void f2e_trim_right(char *value) {
  size_t len = strlen(value);
  while (len > 0 && isspace((unsigned char)value[len - 1])) {
    value[len - 1] = '\0';
    len--;
  }
}

static char *f2e_trim(char *value) {
  char *left = f2e_trim_left(value);
  f2e_trim_right(left);
  return left;
}

static void f2e_strip_comment(char *line) {
  int in_quote = 0;
  int escaped = 0;
  for (char *cursor = line; *cursor; cursor++) {
    if (escaped) {
      escaped = 0;
      continue;
    }
    if (*cursor == '\\' && in_quote) {
      escaped = 1;
      continue;
    }
    if (*cursor == '"') {
      in_quote = !in_quote;
      continue;
    }
    if (*cursor == '#' && !in_quote) {
      *cursor = '\0';
      return;
    }
  }
}

static int f2e_array_value_is_complete(const char *value) {
  const char *cursor = f2e_trim_left((char *)value);
  if (*cursor != '[') {
    return 1;
  }

  int depth = 0;
  int in_quote = 0;
  int escaped = 0;
  for (; *cursor; cursor++) {
    if (escaped) {
      escaped = 0;
      continue;
    }
    if (*cursor == '\\' && in_quote) {
      escaped = 1;
      continue;
    }
    if (*cursor == '"') {
      in_quote = !in_quote;
      continue;
    }
    if (in_quote) {
      continue;
    }
    if (*cursor == '[') {
      depth++;
    } else if (*cursor == ']') {
      depth--;
      if (depth <= 0) {
        return 1;
      }
    }
  }
  return 0;
}

static int f2e_append_logical_config_line(char *target,
                                          size_t target_size,
                                          const char *fragment) {
  size_t used = strlen(target);
  size_t fragment_len = strlen(fragment);
  size_t separator = used > 0 ? 1 : 0;
  if (used + separator + fragment_len + 1 > target_size) {
    return 0;
  }
  if (separator) {
    target[used++] = '\n';
  }
  memcpy(target + used, fragment, fragment_len + 1);
  return 1;
}

static int f2e_parse_quoted_string(const char *input, char *out, size_t out_size) {
  const char *cursor = f2e_trim_left((char *)input);
  size_t len = 0;
  if (*cursor != '"') {
    return 0;
  }
  cursor++;
  while (*cursor && *cursor != '"') {
    char ch = *cursor++;
    if (ch == '\\' && *cursor) {
      char escaped = *cursor++;
      switch (escaped) {
        case 'n':
          ch = '\n';
          break;
        case 'r':
          ch = '\r';
          break;
        case 't':
          ch = '\t';
          break;
        case '"':
        case '\\':
        case '/':
          ch = escaped;
          break;
        default:
          ch = escaped;
          break;
      }
    }
    if (len + 1 < out_size) {
      out[len++] = ch;
    }
  }
  if (*cursor != '"') {
    return 0;
  }
  if (out_size > 0) {
    out[len] = '\0';
  }
  return 1;
}

static int f2e_parse_bare_value(const char *input, char *out, size_t out_size) {
  char tmp[F2E_MAX_VALUE];
  f2e_strlcpy(tmp, input, sizeof(tmp));
  char *trimmed = f2e_trim(tmp);
  if (*trimmed == '"') {
    return f2e_parse_quoted_string(trimmed, out, out_size);
  }
  f2e_strlcpy(out, trimmed, out_size);
  return out[0] != '\0';
}

static int f2e_add_alias_to_list(char aliases[][F2E_MAX_NAME], size_t *alias_count, const char *alias) {
  if (!alias || alias[0] == '\0') {
    return 0;
  }
  for (size_t i = 0; i < *alias_count; i++) {
    if (f2e_streq(aliases[i], alias)) {
      return 1;
    }
  }
  if (*alias_count >= F2E_MAX_ALIASES) {
    return 0;
  }
  f2e_strlcpy(aliases[*alias_count], alias, F2E_MAX_NAME);
  (*alias_count)++;
  return 1;
}

static int f2e_add_alias(F2EFlag *flag, const char *alias) {
  return f2e_add_alias_to_list(flag->aliases, &flag->alias_count, alias);
}

static F2EFlag *f2e_add_flag(F2EConfig *config, const char *name) {
  if (config->flag_count >= F2E_MAX_FLAGS) {
    return NULL;
  }
  F2EFlag *flag = &config->flags[config->flag_count++];
  memset(flag, 0, sizeof(*flag));
  flag->type = F2E_TYPE_STRING;
  flag->command = F2E_SCOPE_ROOT;
  f2e_strlcpy(flag->name, name, sizeof(flag->name));
  f2e_add_alias(flag, name);
  return flag;
}

static int f2e_scope_parent(const F2EConfig *config, int scope) {
  if (scope < 0 || (size_t)scope >= config->command_count) {
    return F2E_SCOPE_ROOT;
  }
  return config->commands[scope].parent;
}

/* Searches every scope; sets *ambiguous when distinct flags share the name. */
static const F2EFlag *f2e_find_flag_any_scope_by_alias(const F2EConfig *config, const char *alias, int *ambiguous) {
  const F2EFlag *found = NULL;
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    for (size_t j = 0; j < flag->alias_count; j++) {
      if (!f2e_streq(flag->aliases[j], alias)) {
        continue;
      }
      if (found && found != flag) {
        if (ambiguous) {
          *ambiguous = 1;
        }
        return NULL;
      }
      found = flag;
    }
  }
  return found;
}

static const F2EFlag *f2e_find_flag_any_scope_by_short(const F2EConfig *config, char short_name, int *ambiguous) {
  const F2EFlag *found = NULL;
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (flag->short_name != short_name) {
      continue;
    }
    if (found && found != flag) {
      if (ambiguous) {
        *ambiguous = 1;
      }
      return NULL;
    }
    found = flag;
  }
  return found;
}

/*
 * Flag lookups resolve against a command scope: the scope's own flags win,
 * then each ancestor up to the global (root) flags. A subcommand can thereby
 * reuse an alias or short flag that means something else elsewhere.
 *
 * F2E_SCOPE_LENIENT is used when commands are declared but argv selected
 * none (a wrapper may have consumed the subcommand): root flags win, and a
 * name that is unambiguous across all scopes resolves as if it were global.
 */
static const F2EFlag *f2e_find_flag_by_alias_const(const F2EConfig *config, int scope, const char *alias) {
  if (scope == F2E_SCOPE_LENIENT) {
    const F2EFlag *root = f2e_find_flag_by_alias_const(config, F2E_SCOPE_ROOT, alias);
    if (root) {
      return root;
    }
    return f2e_find_flag_any_scope_by_alias(config, alias, NULL);
  }
  for (;;) {
    for (size_t i = 0; i < config->flag_count; i++) {
      const F2EFlag *flag = &config->flags[i];
      if (flag->command != scope) {
        continue;
      }
      for (size_t j = 0; j < flag->alias_count; j++) {
        if (f2e_streq(flag->aliases[j], alias)) {
          return flag;
        }
      }
    }
    if (scope < 0) {
      return NULL;
    }
    scope = f2e_scope_parent(config, scope);
  }
}

static F2EFlag *f2e_find_flag_by_alias(F2EConfig *config, int scope, const char *alias) {
  return (F2EFlag *)f2e_find_flag_by_alias_const(config, scope, alias);
}

static F2EFlag *f2e_find_flag_by_short(F2EConfig *config, int scope, char short_name) {
  if (scope == F2E_SCOPE_LENIENT) {
    F2EFlag *root = f2e_find_flag_by_short(config, F2E_SCOPE_ROOT, short_name);
    if (root) {
      return root;
    }
    return (F2EFlag *)f2e_find_flag_any_scope_by_short(config, short_name, NULL);
  }
  for (;;) {
    for (size_t i = 0; i < config->flag_count; i++) {
      if (config->flags[i].command == scope && config->flags[i].short_name == short_name) {
        return &config->flags[i];
      }
    }
    if (scope < 0) {
      return NULL;
    }
    scope = f2e_scope_parent(config, scope);
  }
}

static int f2e_find_command_by_name(const F2EConfig *config, int parent, const char *name) {
  for (size_t i = 0; i < config->command_count; i++) {
    if (config->commands[i].parent == parent && f2e_streq(config->commands[i].name, name)) {
      return (int)i;
    }
  }
  return -1;
}

static int f2e_find_command_by_token(const F2EConfig *config, int parent, const char *token) {
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->parent != parent) {
      continue;
    }
    if (f2e_streq(command->name, token)) {
      return (int)i;
    }
    for (size_t j = 0; j < command->alias_count; j++) {
      if (f2e_streq(command->aliases[j], token)) {
        return (int)i;
      }
    }
  }
  return -1;
}

static int f2e_command_has_children(const F2EConfig *config, int scope) {
  for (size_t i = 0; i < config->command_count; i++) {
    if (config->commands[i].parent == scope) {
      return 1;
    }
  }
  return 0;
}

static size_t f2e_command_depth(const F2EConfig *config, int index) {
  size_t depth = 0;
  while (index >= 0 && (size_t)index < config->command_count && depth <= F2E_MAX_COMMANDS) {
    depth++;
    index = config->commands[index].parent;
  }
  return depth;
}

static int f2e_command_path_label(const F2EConfig *config, int index, char *out, size_t out_size) {
  if (!out || out_size == 0) {
    return 0;
  }
  out[0] = '\0';
  int chain[F2E_MAX_COMMANDS];
  size_t depth = 0;
  while (index >= 0 && (size_t)index < config->command_count && depth < F2E_MAX_COMMANDS) {
    chain[depth++] = index;
    index = config->commands[index].parent;
  }
  size_t used = 0;
  for (size_t i = depth; i > 0; i--) {
    const char *name = config->commands[chain[i - 1]].name;
    size_t name_len = strlen(name);
    if (used + name_len + (used > 0 ? 1 : 0) + 1 > out_size) {
      return 0;
    }
    if (used > 0) {
      out[used++] = ' ';
    }
    memcpy(out + used, name, name_len);
    used += name_len;
  }
  out[used] = '\0';
  return 1;
}

static int f2e_find_or_add_command(F2EConfig *config, int parent, const char *name) {
  if (!name || name[0] == '\0') {
    return -1;
  }
  int existing = f2e_find_command_by_name(config, parent, name);
  if (existing >= 0) {
    return existing;
  }
  if (config->command_count >= F2E_MAX_COMMANDS) {
    config->too_many_commands = 1;
    return -1;
  }
  F2ECommand *command = &config->commands[config->command_count];
  memset(command, 0, sizeof(*command));
  f2e_strlcpy(command->name, name, sizeof(command->name));
  command->parent = parent;
  return (int)config->command_count++;
}

static int f2e_parse_alias_list(char aliases[][F2E_MAX_NAME], size_t *alias_count, const char *value) {
  const char *cursor = f2e_trim_left((char *)value);
  if (*cursor != '[') {
    return 0;
  }
  cursor++;
  while (*cursor) {
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ']') {
      return 1;
    }
    char alias[F2E_MAX_NAME];
    if (!f2e_parse_quoted_string(cursor, alias, sizeof(alias))) {
      return 0;
    }
    f2e_add_alias_to_list(aliases, alias_count, alias);
    cursor++;
    int escaped = 0;
    while (*cursor) {
      if (escaped) {
        escaped = 0;
      } else if (*cursor == '\\') {
        escaped = 1;
      } else if (*cursor == '"') {
        cursor++;
        break;
      }
      cursor++;
    }
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ',') {
      cursor++;
    }
  }
  return 0;
}

static int f2e_parse_aliases(F2EFlag *flag, const char *value) {
  return f2e_parse_alias_list(flag->aliases, &flag->alias_count, value);
}

static int f2e_add_env_key_to_list(char keys[][F2E_MAX_ENV], size_t *key_count, const char *key) {
  if (!keys || !key_count || !key) {
    return 0;
  }
  for (size_t i = 0; i < *key_count; i++) {
    if (f2e_streq(keys[i], key)) {
      return 1;
    }
  }
  if (*key_count >= F2E_MAX_ENV_FILE_KEYS) {
    return 0;
  }
  f2e_strlcpy(keys[*key_count], key, F2E_MAX_ENV);
  (*key_count)++;
  return 1;
}

static int f2e_parse_env_key_list(char keys[][F2E_MAX_ENV], size_t *key_count, const char *value) {
  size_t original_count = key_count ? *key_count : 0;
  const char *cursor = f2e_trim_left((char *)value);
  if (*cursor != '[') {
    return 0;
  }
  cursor++;
  while (*cursor) {
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ']') {
      return 1;
    }
    char key[F2E_MAX_ENV];
    if (!f2e_parse_quoted_string(cursor, key, sizeof(key))) {
      if (key_count) {
        *key_count = original_count;
      }
      return 0;
    }
    if (!f2e_add_env_key_to_list(keys, key_count, key)) {
      if (key_count) {
        *key_count = original_count;
      }
      return 0;
    }
    cursor++;
    int escaped = 0;
    while (*cursor) {
      if (escaped) {
        escaped = 0;
      } else if (*cursor == '\\') {
        escaped = 1;
      } else if (*cursor == '"') {
        cursor++;
        break;
      }
      cursor++;
    }
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ',') {
      cursor++;
    }
  }
  if (key_count) {
    *key_count = original_count;
  }
  return 0;
}

static unsigned f2e_help_column_mask_for_name(const char *name) {
  if (f2e_streq(name, "options") || f2e_streq(name, "option") ||
      f2e_streq(name, "flags") || f2e_streq(name, "names")) {
    return F2E_HELP_COL_OPTIONS;
  }
  if (f2e_streq(name, "env") || f2e_streq(name, "environment")) {
    return F2E_HELP_COL_ENV;
  }
  if (f2e_streq(name, "type")) {
    return F2E_HELP_COL_TYPE;
  }
  if (f2e_streq(name, "default") || f2e_streq(name, "defaults")) {
    return F2E_HELP_COL_DEFAULT;
  }
  if (f2e_streq(name, "description") || f2e_streq(name, "help")) {
    return F2E_HELP_COL_DESCRIPTION;
  }
  return 0;
}

static int f2e_parse_help_column_list(const char *value, unsigned *mask_out) {
  const char *cursor = f2e_trim_left((char *)value);
  unsigned mask = 0;
  if (*cursor != '[') {
    return 0;
  }
  cursor++;
  while (*cursor) {
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ']') {
      *mask_out = mask;
      return 1;
    }
    char column[F2E_MAX_NAME];
    if (!f2e_parse_quoted_string(cursor, column, sizeof(column))) {
      return 0;
    }
    unsigned column_mask = f2e_help_column_mask_for_name(column);
    if (column_mask == 0) {
      return 0;
    }
    mask |= column_mask;
    cursor++;
    int escaped = 0;
    while (*cursor) {
      if (escaped) {
        escaped = 0;
      } else if (*cursor == '\\') {
        escaped = 1;
      } else if (*cursor == '"') {
        cursor++;
        break;
      }
      cursor++;
    }
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ',') {
      cursor++;
    }
  }
  return 0;
}

static int f2e_parse_true_aliases(F2EFlag *flag, const char *value) {
  return f2e_parse_alias_list(flag->true_aliases, &flag->true_alias_count, value);
}

static int f2e_parse_false_aliases(F2EFlag *flag, const char *value) {
  return f2e_parse_alias_list(flag->false_aliases, &flag->false_alias_count, value);
}

static int f2e_parse_type(const char *value, F2EValueType *type) {
  char parsed[F2E_MAX_VALUE];
  if (!f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
    return 0;
  }
  if (f2e_streq(parsed, "bool") || f2e_streq(parsed, "boolean") || f2e_streq(parsed, "flag")) {
    *type = F2E_TYPE_BOOL;
    return 1;
  }
  if (f2e_streq(parsed, "string") || f2e_streq(parsed, "value")) {
    *type = F2E_TYPE_STRING;
    return 1;
  }
  if (f2e_streq(parsed, "int") || f2e_streq(parsed, "integer")) {
    *type = F2E_TYPE_INT;
    return 1;
  }
  if (f2e_streq(parsed, "float") || f2e_streq(parsed, "double") ||
      f2e_streq(parsed, "number") || f2e_streq(parsed, "decimal")) {
    *type = F2E_TYPE_FLOAT;
    return 1;
  }
  if (f2e_streq(parsed, "json")) {
    *type = F2E_TYPE_JSON;
    return 1;
  }
  if (f2e_streq(parsed, "array") || f2e_streq(parsed, "list") ||
      f2e_streq(parsed, "json-array")) {
    *type = F2E_TYPE_ARRAY;
    return 1;
  }
  if (f2e_streq(parsed, "map") || f2e_streq(parsed, "object") ||
      f2e_streq(parsed, "dictionary") || f2e_streq(parsed, "json-object")) {
    *type = F2E_TYPE_MAP;
    return 1;
  }
  return 0;
}

static int f2e_parse_config_bool(const char *value, int *out) {
  char parsed[F2E_MAX_VALUE];
  if (!f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
    return 0;
  }
  if (f2e_streq(parsed, "true") || f2e_streq(parsed, "1") || f2e_streq(parsed, "yes") || f2e_streq(parsed, "on")) {
    *out = 1;
    return 1;
  }
  if (f2e_streq(parsed, "false") || f2e_streq(parsed, "0") || f2e_streq(parsed, "no") || f2e_streq(parsed, "off")) {
    *out = 0;
    return 1;
  }
  return 0;
}

static void f2e_json_skip_ws(const char **cursor) {
  while (**cursor == ' ' || **cursor == '\n' || **cursor == '\r' || **cursor == '\t') {
    (*cursor)++;
  }
}

static int f2e_json_is_hex(char ch) {
  return (ch >= '0' && ch <= '9') ||
         (ch >= 'a' && ch <= 'f') ||
         (ch >= 'A' && ch <= 'F');
}

static int f2e_json_parse_value(const char **cursor, int depth);

static int f2e_json_parse_string_value(const char **cursor) {
  if (**cursor != '"') {
    return 0;
  }
  (*cursor)++;
  while (**cursor) {
    unsigned char ch = (unsigned char)**cursor;
    if (ch == '"') {
      (*cursor)++;
      return 1;
    }
    if (ch < 0x20) {
      return 0;
    }
    if (ch == '\\') {
      (*cursor)++;
      switch (**cursor) {
        case '"':
        case '\\':
        case '/':
        case 'b':
        case 'f':
        case 'n':
        case 'r':
        case 't':
          (*cursor)++;
          break;
        case 'u':
          (*cursor)++;
          for (int i = 0; i < 4; i++) {
            if (!f2e_json_is_hex((*cursor)[i])) {
              return 0;
            }
          }
          *cursor += 4;
          break;
        default:
          return 0;
      }
      continue;
    }
    (*cursor)++;
  }
  return 0;
}

static int f2e_json_parse_number(const char **cursor) {
  const char *start = *cursor;
  if (**cursor == '-') {
    (*cursor)++;
  }
  if (**cursor == '0') {
    (*cursor)++;
  } else if (**cursor >= '1' && **cursor <= '9') {
    while (**cursor >= '0' && **cursor <= '9') {
      (*cursor)++;
    }
  } else {
    return 0;
  }
  if (**cursor == '.') {
    (*cursor)++;
    if (!(**cursor >= '0' && **cursor <= '9')) {
      return 0;
    }
    while (**cursor >= '0' && **cursor <= '9') {
      (*cursor)++;
    }
  }
  if (**cursor == 'e' || **cursor == 'E') {
    (*cursor)++;
    if (**cursor == '+' || **cursor == '-') {
      (*cursor)++;
    }
    if (!(**cursor >= '0' && **cursor <= '9')) {
      return 0;
    }
    while (**cursor >= '0' && **cursor <= '9') {
      (*cursor)++;
    }
  }
  return *cursor > start;
}

static int f2e_json_parse_literal(const char **cursor, const char *literal) {
  size_t len = strlen(literal);
  if (strncmp(*cursor, literal, len) != 0) {
    return 0;
  }
  *cursor += len;
  return 1;
}

static int f2e_json_parse_array(const char **cursor, int depth) {
  if (**cursor != '[') {
    return 0;
  }
  (*cursor)++;
  f2e_json_skip_ws(cursor);
  if (**cursor == ']') {
    (*cursor)++;
    return 1;
  }
  while (**cursor) {
    if (!f2e_json_parse_value(cursor, depth + 1)) {
      return 0;
    }
    f2e_json_skip_ws(cursor);
    if (**cursor == ',') {
      (*cursor)++;
      f2e_json_skip_ws(cursor);
      if (**cursor == ']') {
        return 0;
      }
      continue;
    }
    if (**cursor == ']') {
      (*cursor)++;
      return 1;
    }
    return 0;
  }
  return 0;
}

static int f2e_json_parse_object(const char **cursor, int depth) {
  if (**cursor != '{') {
    return 0;
  }
  (*cursor)++;
  f2e_json_skip_ws(cursor);
  if (**cursor == '}') {
    (*cursor)++;
    return 1;
  }
  while (**cursor) {
    if (!f2e_json_parse_string_value(cursor)) {
      return 0;
    }
    f2e_json_skip_ws(cursor);
    if (**cursor != ':') {
      return 0;
    }
    (*cursor)++;
    if (!f2e_json_parse_value(cursor, depth + 1)) {
      return 0;
    }
    f2e_json_skip_ws(cursor);
    if (**cursor == ',') {
      (*cursor)++;
      f2e_json_skip_ws(cursor);
      if (**cursor == '}') {
        return 0;
      }
      continue;
    }
    if (**cursor == '}') {
      (*cursor)++;
      return 1;
    }
    return 0;
  }
  return 0;
}

static int f2e_json_parse_value(const char **cursor, int depth) {
  if (depth > 64) {
    return 0;
  }
  f2e_json_skip_ws(cursor);
  switch (**cursor) {
    case '"':
      return f2e_json_parse_string_value(cursor);
    case '{':
      return f2e_json_parse_object(cursor, depth);
    case '[':
      return f2e_json_parse_array(cursor, depth);
    case 't':
      return f2e_json_parse_literal(cursor, "true");
    case 'f':
      return f2e_json_parse_literal(cursor, "false");
    case 'n':
      return f2e_json_parse_literal(cursor, "null");
    default:
      if (**cursor == '-' || (**cursor >= '0' && **cursor <= '9')) {
        return f2e_json_parse_number(cursor);
      }
      return 0;
  }
}

static int f2e_json_value_is_valid(const char *value) {
  if (!value) {
    return 0;
  }
  const char *cursor = value;
  if (!f2e_json_parse_value(&cursor, 0)) {
    return 0;
  }
  f2e_json_skip_ws(&cursor);
  return *cursor == '\0';
}

static int f2e_table_keyword_is_commands(const char *word) {
  return f2e_streq(word, "commands") || f2e_streq(word, "command") ||
         f2e_streq(word, "subcommands") || f2e_streq(word, "subcommand");
}

static int f2e_table_keyword_is_flags(const char *word) {
  return f2e_streq(word, "flags") || f2e_streq(word, "flag");
}

static void f2e_record_invalid_command_table(F2EConfig *config, const char *table) {
  if (!config->has_invalid_command_table) {
    config->has_invalid_command_table = 1;
    f2e_strlcpy(config->invalid_command_table, table, sizeof(config->invalid_command_table));
  }
}

/*
 * Parses a [commands.*] table header such as:
 *   [commands.add]
 *   [commands.add.flags.all]
 *   [commands.remote.commands.add.flags.fetch]
 * Nesting is arbitrary: each `commands.<name>` segment descends one level and
 * an optional trailing `flags.<name>` declares a flag scoped to that command.
 * Keyword and name positions strictly alternate, so command names can even be
 * the literal words "commands" or "flags" without ambiguity.
 * Returns 0 when the table is not commands-shaped; otherwise returns 1 and
 * sets *section_out (plus *flag_out or *command_out).
 */
static int f2e_load_commands_table(F2EConfig *config,
                                   const char *table,
                                   F2EConfigSection *section_out,
                                   F2EFlag **flag_out,
                                   int *command_out) {
  char copy[F2E_MAX_LINE];
  f2e_strlcpy(copy, table, sizeof(copy));
  char *segments[2 * F2E_MAX_COMMAND_DEPTH + 2];
  size_t segment_count = 0;
  for (char *cursor = copy; cursor;) {
    if (segment_count >= sizeof(segments) / sizeof(segments[0])) {
      return 0;
    }
    char *dot = strchr(cursor, '.');
    if (dot) {
      *dot = '\0';
    }
    segments[segment_count++] = f2e_trim(cursor);
    cursor = dot ? dot + 1 : NULL;
  }

  if (segment_count < 2 || !f2e_table_keyword_is_commands(segments[0])) {
    return 0;
  }

  *section_out = F2E_SECTION_NONE;
  *flag_out = NULL;
  *command_out = F2E_SCOPE_ROOT;

  int scope = F2E_SCOPE_ROOT;
  size_t index = 0;
  while (index < segment_count) {
    if (f2e_table_keyword_is_commands(segments[index])) {
      if (index + 1 >= segment_count || segments[index + 1][0] == '\0') {
        f2e_record_invalid_command_table(config, table);
        return 1;
      }
      int next = f2e_find_or_add_command(config, scope, segments[index + 1]);
      if (next < 0) {
        return 1;
      }
      scope = next;
      index += 2;
      continue;
    }
    if (f2e_table_keyword_is_flags(segments[index])) {
      if (index + 2 != segment_count || segments[index + 1][0] == '\0') {
        f2e_record_invalid_command_table(config, table);
        return 1;
      }
      F2EFlag *flag = f2e_add_flag(config, segments[index + 1]);
      if (flag) {
        flag->command = scope;
        *flag_out = flag;
        *section_out = F2E_SECTION_FLAG;
      }
      return 1;
    }
    /* a bare segment where a keyword belongs — usually a shorthand like
       [commands.publish.init.flags.x]; nesting must spell out the keyword
       ([commands.publish.commands.init.flags.x]) so command names can never
       clash with the "commands"/"flags" keywords */
    f2e_record_invalid_command_table(config, table);
    return 1;
  }

  *command_out = scope;
  *section_out = F2E_SECTION_COMMAND;
  return 1;
}

static int f2e_load_config(const char *config_path, F2EConfig *config) {
  memset(config, 0, sizeof(*config));
  config->allow_separated_values = 1;
  config->help_columns = F2E_HELP_COL_DEFAULTS;

  FILE *file = fopen(config_path, "r");
  if (!file) {
    return 0;
  }

  F2EFlag *current = NULL;
  int current_command = F2E_SCOPE_ROOT;
  F2EConfigSection section = F2E_SECTION_NONE;
  char line[F2E_MAX_LINE];
  char logical_line[F2E_MAX_LOGICAL_LINE];
  while (fgets(line, sizeof(line), file)) {
    f2e_strip_comment(line);
    char *trimmed = f2e_trim(line);
    if (trimmed[0] == '\0') {
      continue;
    }

    f2e_strlcpy(logical_line, trimmed, sizeof(logical_line));
    char *logical_eq = strchr(logical_line, '=');
    if (logical_eq) {
      char *logical_value = f2e_trim(logical_eq + 1);
      while (*logical_value == '[' && !f2e_array_value_is_complete(logical_value)) {
        if (!fgets(line, sizeof(line), file)) {
          break;
        }
        f2e_strip_comment(line);
        char *continuation = f2e_trim(line);
        if (continuation[0] == '\0') {
          continue;
        }
        if (!f2e_append_logical_config_line(logical_line,
                                            sizeof(logical_line),
                                            continuation)) {
          fclose(file);
          return 0;
        }
        logical_eq = strchr(logical_line, '=');
        logical_value = f2e_trim(logical_eq + 1);
      }
    }
    trimmed = f2e_trim(logical_line);

    if (trimmed[0] == '[') {
      char *end = strchr(trimmed, ']');
      if (!end) {
        current = NULL;
        continue;
      }
      *end = '\0';
      char *table = f2e_trim(trimmed + 1);
      current_command = F2E_SCOPE_ROOT;
      const char prefix[] = "flags.";
      const char global_prefix[] = "global.flags.";
      if (strncmp(table, prefix, sizeof(prefix) - 1) == 0) {
        char *name = f2e_trim(table + sizeof(prefix) - 1);
        current = f2e_add_flag(config, name);
        section = F2E_SECTION_FLAG;
      } else if (strncmp(table, global_prefix, sizeof(global_prefix) - 1) == 0) {
        /* explicit spelling of the global namespace: [global.flags.x] is the
           same as [flags.x] and applies to every subcommand scope */
        char *name = f2e_trim(table + sizeof(global_prefix) - 1);
        current = f2e_add_flag(config, name);
        section = F2E_SECTION_FLAG;
      } else if (f2e_load_commands_table(config, table, &section, &current, &current_command)) {
        /* section, current, and current_command are set by the helper */
      } else if (f2e_streq(table, "parse") || f2e_streq(table, "parser")) {
        current = NULL;
        section = F2E_SECTION_PARSE;
      } else if (f2e_streq(table, "help") || f2e_streq(table, "help_menu")) {
        current = NULL;
        section = F2E_SECTION_HELP;
      } else if (f2e_streq(table, "env") ||
                 f2e_streq(table, "env_audit") ||
                 f2e_streq(table, "audit_env") ||
                 f2e_streq(table, "audit.env") ||
                 f2e_streq(table, "dotenv")) {
        current = NULL;
        section = F2E_SECTION_ENV_AUDIT;
      } else {
        current = NULL;
        section = F2E_SECTION_NONE;
      }
      continue;
    }

    char *eq = strchr(trimmed, '=');
    if (!eq) {
      continue;
    }
    *eq = '\0';
    char *key = f2e_trim(trimmed);
    char *value = f2e_trim(eq + 1);

    if (section == F2E_SECTION_PARSE) {
      if (f2e_streq(key, "allow_separated_values") || f2e_streq(key, "allow_space_values")) {
        int parsed = 0;
        if (f2e_parse_config_bool(value, &parsed)) {
          config->allow_separated_values = parsed;
        }
      } else if (f2e_streq(key, "require_equals")) {
        int parsed = 0;
        if (f2e_parse_config_bool(value, &parsed)) {
          config->allow_separated_values = !parsed;
        }
      } else if (f2e_streq(key, "stop_at_first_positional")) {
        int parsed = 0;
        if (f2e_parse_config_bool(value, &parsed)) {
          config->stop_at_first_positional = parsed;
        }
      } else if (f2e_streq(key, "allow_unknown") ||
                 f2e_streq(key, "allow_hidden") ||
                 f2e_streq(key, "allow_unrecognized") ||
                 f2e_streq(key, "allow_unknown_options")) {
        int parsed = 0;
        if (f2e_parse_config_bool(value, &parsed)) {
          config->allow_unknown = parsed;
        }
      } else if (f2e_streq(key, "positionals_env") || f2e_streq(key, "extras_env")) {
        char parsed[F2E_MAX_ENV];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->positionals_env, parsed, sizeof(config->positionals_env));
        }
      } else if (f2e_streq(key, "unknown_options_env")) {
        char parsed[F2E_MAX_ENV];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->unknown_options_env, parsed, sizeof(config->unknown_options_env));
        }
      } else if (f2e_streq(key, "errors_env") || f2e_streq(key, "parse_errors_env")) {
        char parsed[F2E_MAX_ENV];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->errors_env, parsed, sizeof(config->errors_env));
        }
      } else if (f2e_streq(key, "command_env") ||
                 f2e_streq(key, "commands_env") ||
                 f2e_streq(key, "subcommand_env") ||
                 f2e_streq(key, "command_path_env")) {
        char parsed[F2E_MAX_ENV];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->command_env, parsed, sizeof(config->command_env));
        }
      } else if (f2e_streq(key, "help_url") || f2e_streq(key, "url")) {
        char parsed[F2E_MAX_VALUE];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->help_url, parsed, sizeof(config->help_url));
        }
      } else if (f2e_streq(key, "env_audit_ignore") ||
                 f2e_streq(key, "env_audit_ignore_keys") ||
                 f2e_streq(key, "ignore_env") ||
                 f2e_streq(key, "ignore_envs") ||
                 f2e_streq(key, "ignored_envs") ||
                 f2e_streq(key, "ignored_env")) {
        if (!f2e_parse_env_key_list(config->env_audit_ignored_keys,
                                    &config->env_audit_ignored_count,
                                    value)) {
          config->invalid_env_audit_ignore = 1;
        }
      }
      continue;
    }

    if (section == F2E_SECTION_HELP) {
      if (f2e_streq(key, "url") || f2e_streq(key, "help_url")) {
        char parsed[F2E_MAX_VALUE];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(config->help_url, parsed, sizeof(config->help_url));
        }
      } else if (f2e_streq(key, "columns") ||
                 f2e_streq(key, "include") ||
                 f2e_streq(key, "include_columns") ||
                 f2e_streq(key, "table_columns")) {
        unsigned parsed = 0;
        if (f2e_parse_help_column_list(value, &parsed)) {
          config->help_columns = parsed;
          config->help_columns_configured = 1;
        } else {
          config->invalid_help_columns = 1;
        }
      } else if (f2e_streq(key, "exclude") ||
                 f2e_streq(key, "exclude_columns") ||
                 f2e_streq(key, "table_exclude")) {
        unsigned parsed = 0;
        if (f2e_parse_help_column_list(value, &parsed)) {
          config->help_exclude_columns |= parsed;
        } else {
          config->invalid_help_exclude_columns = 1;
        }
      }
      continue;
    }

    if (section == F2E_SECTION_ENV_AUDIT) {
      if (f2e_streq(key, "ignore") ||
          f2e_streq(key, "ignored") ||
          f2e_streq(key, "ignore_keys") ||
          f2e_streq(key, "ignored_keys") ||
          f2e_streq(key, "ignore_env") ||
          f2e_streq(key, "ignore_envs") ||
          f2e_streq(key, "ignored_env") ||
          f2e_streq(key, "ignored_envs") ||
          f2e_streq(key, "env_audit_ignore") ||
          f2e_streq(key, "env_audit_ignore_keys")) {
        if (!f2e_parse_env_key_list(config->env_audit_ignored_keys,
                                    &config->env_audit_ignored_count,
                                    value)) {
          config->invalid_env_audit_ignore = 1;
        }
      }
      continue;
    }

    if (section == F2E_SECTION_COMMAND) {
      if (current_command < 0 || (size_t)current_command >= config->command_count) {
        continue;
      }
      F2ECommand *command = &config->commands[current_command];
      if (f2e_streq(key, "help") || f2e_streq(key, "description") || f2e_streq(key, "summary")) {
        char parsed[F2E_MAX_VALUE];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(command->help, parsed, sizeof(command->help));
        }
      } else if (f2e_streq(key, "aliases")) {
        f2e_parse_alias_list(command->aliases, &command->alias_count, value);
      } else if (f2e_streq(key, "env")) {
        char parsed[F2E_MAX_ENV];
        if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
          f2e_strlcpy(command->env, parsed, sizeof(command->env));
        }
      } else if (f2e_streq(key, "allow_unknown") ||
                 f2e_streq(key, "allow_hidden") ||
                 f2e_streq(key, "allow_unrecognized") ||
                 f2e_streq(key, "allow_unknown_options")) {
        int parsed = 0;
        if (f2e_parse_config_bool(value, &parsed)) {
          command->allow_unknown = parsed;
          command->allow_unknown_set = 1;
        }
      }
      continue;
    }

    if (section != F2E_SECTION_FLAG || !current) {
      continue;
    }

    if (f2e_streq(key, "env")) {
      char parsed[F2E_MAX_ENV];
      if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
        f2e_strlcpy(current->env, parsed, sizeof(current->env));
      }
    } else if (f2e_streq(key, "aliases")) {
      f2e_parse_aliases(current, value);
    } else if (f2e_streq(key, "true_aliases")) {
      f2e_parse_true_aliases(current, value);
    } else if (f2e_streq(key, "false_aliases")) {
      f2e_parse_false_aliases(current, value);
    } else if (f2e_streq(key, "short")) {
      char parsed[F2E_MAX_VALUE];
      if (f2e_parse_bare_value(value, parsed, sizeof(parsed)) && parsed[0] != '\0') {
        current->short_name = parsed[0];
      }
    } else if (f2e_streq(key, "type")) {
      char parsed[F2E_MAX_VALUE];
      if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
        f2e_strlcpy(current->type_value, parsed, sizeof(current->type_value));
        current->invalid_type = !f2e_parse_type(value, &current->type);
      } else {
        f2e_strlcpy(current->type_value, value, sizeof(current->type_value));
        current->invalid_type = 1;
      }
    } else if (f2e_streq(key, "default")) {
      char parsed[F2E_MAX_VALUE];
      if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
        current->has_default = 1;
        f2e_strlcpy(current->default_value, parsed, sizeof(current->default_value));
      }
    } else if (f2e_streq(key, "help") || f2e_streq(key, "description") || f2e_streq(key, "example")) {
      char parsed[F2E_MAX_VALUE];
      if (f2e_parse_bare_value(value, parsed, sizeof(parsed))) {
        f2e_strlcpy(current->help, parsed, sizeof(current->help));
      }
    }
  }

  fclose(file);
  if (config->command_count > 0 && config->command_env[0] == '\0') {
    f2e_strlcpy(config->command_env, F2E_DEFAULT_COMMAND_ENV, sizeof(config->command_env));
  }
  return 1;
}

static char *f2e_default_config_path(void) {
  char dir[PATH_MAX];
  char home[PATH_MAX];
  const char *pwd = getenv("PWD");
  const char *home_env = getenv("HOME");

#if defined(_WIN32)
  if (GetCurrentDirectoryA(sizeof(dir), dir) == 0) {
    if (pwd && pwd[0] != '\0') {
      f2e_strlcpy(dir, pwd, sizeof(dir));
    } else {
      f2e_strlcpy(dir, ".", sizeof(dir));
    }
  }
#elif defined(__unix__) || defined(__APPLE__)
  if (!getcwd(dir, sizeof(dir))) {
    if (pwd && pwd[0] != '\0') {
      f2e_strlcpy(dir, pwd, sizeof(dir));
    } else {
      f2e_strlcpy(dir, ".", sizeof(dir));
    }
  }
#else
  if (pwd && pwd[0] != '\0') {
    f2e_strlcpy(dir, pwd, sizeof(dir));
  } else {
    f2e_strlcpy(dir, ".", sizeof(dir));
  }
#endif

  if (home_env && home_env[0] != '\0') {
    f2e_strlcpy(home, home_env, sizeof(home));
  } else {
    home[0] = '\0';
  }

  while (dir[0] != '\0') {
    size_t dir_len = strlen(dir);
    while (dir_len > 1 && (dir[dir_len - 1] == '/' || dir[dir_len - 1] == '\\')) {
      dir[--dir_len] = '\0';
    }

    if (home[0] != '\0') {
      size_t home_len = strlen(home);
      while (home_len > 1 && (home[home_len - 1] == '/' || home[home_len - 1] == '\\')) {
        home[--home_len] = '\0';
      }
      if (f2e_streq(dir, home)) {
        return NULL;
      }
    }

    const char suffix[] = "/.cli-flags.toml";
    char *candidate = (char *)malloc(dir_len + sizeof(suffix));
    if (!candidate) {
      return NULL;
    }
    memcpy(candidate, dir, dir_len);
    memcpy(candidate + dir_len, suffix, sizeof(suffix));

    FILE *file = fopen(candidate, "r");
    if (file) {
      fclose(file);
      return candidate;
    }
    free(candidate);

    char *slash = strrchr(dir, '/');
#if defined(_WIN32)
    char *backslash = strrchr(dir, '\\');
    if (!slash || (backslash && backslash > slash)) {
      slash = backslash;
    }
#endif
    if (!slash) {
      break;
    }
    if (slash == dir) {
      dir[1] = '\0';
      if (dir_len == 1) {
        break;
      }
    } else {
      *slash = '\0';
    }
  }

  return NULL;
}

static F2EPair *f2e_find_pair(F2EPair *pairs, size_t pair_count, const char *key) {
  for (size_t i = 0; i < pair_count; i++) {
    if (pairs[i].set && f2e_streq(pairs[i].key, key)) {
      return &pairs[i];
    }
  }
  return NULL;
}

static void f2e_set_pair(F2EPair *pairs, size_t pair_count, const char *key, const char *value) {
  if (!pairs || !key || key[0] == '\0') {
    return;
  }
  F2EPair *pair = f2e_find_pair(pairs, pair_count, key);
  if (!pair) {
    for (size_t i = 0; i < pair_count; i++) {
      if (!pairs[i].set) {
        pair = &pairs[i];
        pair->set = 1;
        f2e_strlcpy(pair->key, key, sizeof(pair->key));
        break;
      }
    }
  }
  if (pair) {
    f2e_strlcpy(pair->value, value ? value : "", sizeof(pair->value));
  }
}

static int f2e_buffer_init(F2EBuffer *buffer) {
  buffer->cap = 128;
  buffer->len = 0;
  buffer->data = (char *)malloc(buffer->cap);
  if (!buffer->data) {
    return 0;
  }
  buffer->data[0] = '\0';
  return 1;
}

static int f2e_buffer_reserve(F2EBuffer *buffer, size_t extra) {
  if (extra > SIZE_MAX - buffer->len - 1) {
    return 0;
  }
  size_t needed = buffer->len + extra + 1;
  if (needed <= buffer->cap) {
    return 1;
  }
  size_t next = buffer->cap;
  while (needed > next) {
    if (next > SIZE_MAX / 2) {
      next = needed;
      break;
    }
    next *= 2;
  }
  char *data = (char *)realloc(buffer->data, next);
  if (!data) {
    return 0;
  }
  buffer->data = data;
  buffer->cap = next;
  return 1;
}

static int f2e_buffer_append_char(F2EBuffer *buffer, char ch) {
  if (!f2e_buffer_reserve(buffer, 1)) {
    return 0;
  }
  buffer->data[buffer->len++] = ch;
  buffer->data[buffer->len] = '\0';
  return 1;
}

static int f2e_buffer_append(F2EBuffer *buffer, const char *value) {
  size_t len = strlen(value);
  if (!f2e_buffer_reserve(buffer, len)) {
    return 0;
  }
  memcpy(buffer->data + buffer->len, value, len);
  buffer->len += len;
  buffer->data[buffer->len] = '\0';
  return 1;
}

static int f2e_buffer_append_json_string(F2EBuffer *buffer, const char *value) {
  if (!f2e_buffer_append_char(buffer, '"')) {
    return 0;
  }
  for (const unsigned char *cursor = (const unsigned char *)value; *cursor; cursor++) {
    switch (*cursor) {
      case '"':
        if (!f2e_buffer_append(buffer, "\\\"")) {
          return 0;
        }
        break;
      case '\\':
        if (!f2e_buffer_append(buffer, "\\\\")) {
          return 0;
        }
        break;
      case '\b':
        if (!f2e_buffer_append(buffer, "\\b")) {
          return 0;
        }
        break;
      case '\f':
        if (!f2e_buffer_append(buffer, "\\f")) {
          return 0;
        }
        break;
      case '\n':
        if (!f2e_buffer_append(buffer, "\\n")) {
          return 0;
        }
        break;
      case '\r':
        if (!f2e_buffer_append(buffer, "\\r")) {
          return 0;
        }
        break;
      case '\t':
        if (!f2e_buffer_append(buffer, "\\t")) {
          return 0;
        }
        break;
      default:
        if (*cursor < 0x20) {
          char escaped[7];
          snprintf(escaped, sizeof(escaped), "\\u%04x", *cursor);
          if (!f2e_buffer_append(buffer, escaped)) {
            return 0;
          }
        } else if (!f2e_buffer_append_char(buffer, (char)*cursor)) {
          return 0;
        }
        break;
    }
  }
  return f2e_buffer_append_char(buffer, '"');
}

static int f2e_buffer_append_shell_single_quoted(F2EBuffer *buffer, const char *value) {
  if (!f2e_buffer_append_char(buffer, '\'')) {
    return 0;
  }
  for (const char *cursor = value ? value : ""; *cursor; cursor++) {
    if (*cursor == '\'') {
      if (!f2e_buffer_append(buffer, "'\\''")) {
        return 0;
      }
    } else if (!f2e_buffer_append_char(buffer, *cursor)) {
      return 0;
    }
  }
  return f2e_buffer_append_char(buffer, '\'');
}

static int f2e_json_list_init(F2EJsonList *list) {
  memset(list, 0, sizeof(*list));
  if (!f2e_buffer_init(&list->buffer)) {
    list->failed = 1;
    return 0;
  }
  list->initialized = 1;
  if (!f2e_buffer_append_char(&list->buffer, '[')) {
    list->failed = 1;
    return 0;
  }
  return 1;
}

static void f2e_json_list_discard(F2EJsonList *list) {
  if (list && list->initialized) {
    free(list->buffer.data);
  }
  if (list) {
    memset(list, 0, sizeof(*list));
  }
}

static int f2e_json_list_append(F2EJsonList *list, const char *value) {
  if (!list || list->failed || !list->initialized) {
    return 0;
  }
  if (list->count > 0 && !f2e_buffer_append_char(&list->buffer, ',')) {
    list->failed = 1;
    return 0;
  }
  if (!f2e_buffer_append_json_string(&list->buffer, value ? value : "")) {
    list->failed = 1;
    return 0;
  }
  list->count++;
  return 1;
}

static int f2e_json_list_finish(F2EJsonList *list, char *out, size_t out_size) {
  if (!list || list->failed || !list->initialized || !out || out_size == 0) {
    return 0;
  }
  if (!f2e_buffer_append_char(&list->buffer, ']')) {
    list->failed = 1;
    return 0;
  }
  if (list->buffer.len >= out_size) {
    list->failed = 1;
    return 0;
  }
  f2e_strlcpy(out, list->buffer.data, out_size);
  return 1;
}

static int f2e_audit_init(F2EAudit *audit) {
  memset(audit, 0, sizeof(*audit));
  if (!f2e_buffer_init(&audit->errors)) {
    return 0;
  }
  if (!f2e_buffer_init(&audit->warnings)) {
    free(audit->errors.data);
    memset(audit, 0, sizeof(*audit));
    return 0;
  }
  if (!f2e_buffer_append_char(&audit->errors, '[') || !f2e_buffer_append_char(&audit->warnings, '[')) {
    audit->failed = 1;
  }
  return 1;
}

static void f2e_audit_discard(F2EAudit *audit) {
  free(audit->errors.data);
  free(audit->warnings.data);
  memset(audit, 0, sizeof(*audit));
}

static void f2e_audit_add(F2EAudit *audit, int is_error, const char *format, ...) {
  if (!audit || audit->failed) {
    return;
  }

  char message[512];
  va_list args;
  va_start(args, format);
  vsnprintf(message, sizeof(message), format, args);
  va_end(args);

  F2EBuffer *target = is_error ? &audit->errors : &audit->warnings;
  size_t *count = is_error ? &audit->error_count : &audit->warning_count;
  if (*count > 0 && !f2e_buffer_append_char(target, ',')) {
    audit->failed = 1;
    return;
  }
  if (!f2e_buffer_append_json_string(target, message)) {
    audit->failed = 1;
    return;
  }
  (*count)++;
}

static char *f2e_audit_report(F2EAudit *audit, int *status_out) {
  if (!audit || audit->failed || !audit->errors.data || !audit->warnings.data) {
    if (status_out) {
      *status_out = 1;
    }
    if (audit) {
      f2e_audit_discard(audit);
    }
    const char failure_json[] = "{\"ok\":false,\"errorCount\":1,\"warningCount\":0,\"errors\":[\"audit allocation failed\"],\"warnings\":[]}";
    char *failed = (char *)malloc(sizeof(failure_json));
    if (failed) {
      f2e_strlcpy(failed, failure_json, sizeof(failure_json));
    }
    return failed;
  }

  if (!f2e_buffer_append_char(&audit->errors, ']') || !f2e_buffer_append_char(&audit->warnings, ']')) {
    if (status_out) {
      *status_out = 1;
    }
    f2e_audit_discard(audit);
    return f2e_empty_json_object();
  }

  F2EBuffer report = {0};
  if (!f2e_buffer_init(&report)) {
    if (status_out) {
      *status_out = 1;
    }
    f2e_audit_discard(audit);
    return f2e_empty_json_object();
  }

  char counts[96];
  int ok = audit->error_count == 0;
  snprintf(counts, sizeof(counts), "{\"ok\":%s,\"errorCount\":%lu,\"warningCount\":%lu,\"errors\":",
           ok ? "true" : "false",
           (unsigned long)audit->error_count,
           (unsigned long)audit->warning_count);
  if (!f2e_buffer_append(&report, counts) ||
      !f2e_buffer_append(&report, audit->errors.data) ||
      !f2e_buffer_append(&report, ",\"warnings\":") ||
      !f2e_buffer_append(&report, audit->warnings.data) ||
      !f2e_buffer_append_char(&report, '}')) {
    free(report.data);
    if (status_out) {
      *status_out = 1;
    }
    f2e_audit_discard(audit);
    return f2e_empty_json_object();
  }

  if (status_out) {
    *status_out = ok ? 0 : 1;
  }
  f2e_audit_discard(audit);
  return report.data;
}

static char *f2e_pairs_to_json(F2EPair *pairs, size_t pair_count) {
  F2EBuffer buffer = {0};
  if (!f2e_buffer_init(&buffer)) {
    return NULL;
  }
  if (!f2e_buffer_append_char(&buffer, '{')) {
    free(buffer.data);
    return NULL;
  }
  int wrote = 0;
  for (size_t i = 0; i < pair_count; i++) {
    if (!pairs[i].set) {
      continue;
    }
    if (wrote) {
      if (!f2e_buffer_append_char(&buffer, ',')) {
        free(buffer.data);
        return NULL;
      }
    }
    if (!f2e_buffer_append_json_string(&buffer, pairs[i].key) ||
        !f2e_buffer_append_char(&buffer, ':') ||
        !f2e_buffer_append_json_string(&buffer, pairs[i].value)) {
      free(buffer.data);
      return NULL;
    }
    wrote = 1;
  }
  if (!f2e_buffer_append_char(&buffer, '}')) {
    free(buffer.data);
    return NULL;
  }
  return buffer.data;
}

static int f2e_bool_value_alias(const F2EFlag *flag, const char *value, const char **canonical) {
  if (!flag || !value) {
    return 0;
  }
  if (f2e_streq(value, "true")) {
    *canonical = "true";
    return 1;
  }
  if (f2e_streq(value, "false")) {
    *canonical = "false";
    return 1;
  }
  for (size_t i = 0; i < flag->true_alias_count; i++) {
    if (f2e_streq(flag->true_aliases[i], value)) {
      *canonical = "true";
      return 1;
    }
  }
  for (size_t i = 0; i < flag->false_alias_count; i++) {
    if (f2e_streq(flag->false_aliases[i], value)) {
      *canonical = "false";
      return 1;
    }
  }
  return 0;
}

static int f2e_int_value_is_valid(const char *value) {
  if (!value || value[0] == '\0') {
    return 0;
  }
  const char *cursor = value;
  if (*cursor == '+' || *cursor == '-') {
    cursor++;
  }
  if (!isdigit((unsigned char)*cursor)) {
    return 0;
  }
  while (isdigit((unsigned char)*cursor)) {
    cursor++;
  }
  if (*cursor != '\0') {
    return 0;
  }

  errno = 0;
  char *end = NULL;
  (void)strtoll(value, &end, 10);
  return errno != ERANGE && end && *end == '\0';
}

static int f2e_float_value_is_valid(const char *value) {
  if (!value || value[0] == '\0') {
    return 0;
  }
  const char *cursor = value;
  if (*cursor == '+' || *cursor == '-') {
    cursor++;
  }
  if (!isdigit((unsigned char)*cursor)) {
    return 0;
  }
  while (isdigit((unsigned char)*cursor)) {
    cursor++;
  }
  if (*cursor == '.') {
    cursor++;
    if (!isdigit((unsigned char)*cursor)) {
      return 0;
    }
    while (isdigit((unsigned char)*cursor)) {
      cursor++;
    }
  }
  if (*cursor == 'e' || *cursor == 'E') {
    cursor++;
    if (*cursor == '+' || *cursor == '-') {
      cursor++;
    }
    if (!isdigit((unsigned char)*cursor)) {
      return 0;
    }
    while (isdigit((unsigned char)*cursor)) {
      cursor++;
    }
  }
  if (*cursor != '\0') {
    return 0;
  }

  errno = 0;
  char *end = NULL;
  (void)strtod(value, &end);
  return errno != ERANGE && end && *end == '\0';
}

static int f2e_json_container_is_valid(const char *value, char opening) {
  if (!value) {
    return 0;
  }
  const char *cursor = value;
  f2e_json_skip_ws(&cursor);
  if (*cursor != opening || !f2e_json_parse_value(&cursor, 0)) {
    return 0;
  }
  f2e_json_skip_ws(&cursor);
  return *cursor == '\0';
}

static const char *f2e_value_type_name(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "bool";
    case F2E_TYPE_INT:
      return "integer";
    case F2E_TYPE_FLOAT:
      return "double";
    case F2E_TYPE_JSON:
      return "JSON";
    case F2E_TYPE_ARRAY:
      return "array";
    case F2E_TYPE_MAP:
      return "map";
    case F2E_TYPE_STRING:
    default:
      return "string";
  }
}

static int f2e_normalize_flag_value(const F2EFlag *flag, const char *value, char *out, size_t out_size) {
  if (!flag || !value || !out || out_size == 0) {
    return 0;
  }
  switch (flag->type) {
    case F2E_TYPE_BOOL: {
      const char *canonical = NULL;
      if (!f2e_bool_value_alias(flag, value, &canonical)) {
        return 0;
      }
      f2e_strlcpy(out, canonical, out_size);
      return 1;
    }
    case F2E_TYPE_INT:
      if (!f2e_int_value_is_valid(value)) {
        return 0;
      }
      f2e_strlcpy(out, value, out_size);
      return 1;
    case F2E_TYPE_FLOAT:
      if (!f2e_float_value_is_valid(value)) {
        return 0;
      }
      f2e_strlcpy(out, value, out_size);
      return 1;
    case F2E_TYPE_JSON:
      if (!f2e_json_value_is_valid(value)) {
        return 0;
      }
      f2e_strlcpy(out, value, out_size);
      return 1;
    case F2E_TYPE_ARRAY:
      if (!f2e_json_container_is_valid(value, '[')) {
        return 0;
      }
      f2e_strlcpy(out, value, out_size);
      return 1;
    case F2E_TYPE_MAP:
      if (!f2e_json_container_is_valid(value, '{')) {
        return 0;
      }
      f2e_strlcpy(out, value, out_size);
      return 1;
    case F2E_TYPE_STRING:
    default:
      f2e_strlcpy(out, value, out_size);
      return 1;
  }
}

static void f2e_report_invalid_value(F2EJsonList *errors, const F2EFlag *flag, const char *value) {
  if (!errors || !errors->initialized || !flag) {
    return;
  }
  char message[512];
  if (flag->type == F2E_TYPE_ARRAY) {
    snprintf(message, sizeof(message), "flags.%s value \"%s\" is not a valid JSON array", f2e_audit_flag_name(flag), value ? value : "");
  } else if (flag->type == F2E_TYPE_MAP) {
    snprintf(message, sizeof(message), "flags.%s value \"%s\" is not a valid JSON object", f2e_audit_flag_name(flag), value ? value : "");
  } else if (flag->type == F2E_TYPE_JSON) {
    snprintf(message, sizeof(message), "flags.%s value \"%s\" is not valid JSON", f2e_audit_flag_name(flag), value ? value : "");
  } else {
    snprintf(message, sizeof(message), "flags.%s value \"%s\" is not a valid %s",
             f2e_audit_flag_name(flag),
             value ? value : "",
             f2e_value_type_name(flag->type));
  }
  f2e_json_list_append(errors, message);
}

static int f2e_try_set_flag_value(F2EFlag *flag, F2EPair *pairs, size_t pair_count, const char *value, F2EJsonList *errors) {
  char normalized[F2E_MAX_VALUE];
  if (!f2e_normalize_flag_value(flag, value, normalized, sizeof(normalized))) {
    f2e_report_invalid_value(errors, flag, value);
    return 0;
  }
  f2e_set_pair(pairs, pair_count, flag->env, normalized);
  return 1;
}

static int f2e_try_set_bool_value(F2EFlag *flag, F2EPair *pairs, size_t pair_count, const char *value) {
  const char *canonical = NULL;
  if (!f2e_bool_value_alias(flag, value, &canonical)) {
    return 0;
  }
  f2e_set_pair(pairs, pair_count, flag->env, canonical);
  return 1;
}

static int f2e_token_looks_like_known_option(F2EConfig *config, int scope, const char *token) {
  if (!token || token[0] != '-' || token[1] == '\0') {
    return 0;
  }
  if (token[1] == '-') {
    const char *name = token + 2;
    char copy[F2E_MAX_NAME];
    f2e_strlcpy(copy, name, sizeof(copy));
    char *eq = strchr(copy, '=');
    if (eq) {
      *eq = '\0';
    }
    if (f2e_find_flag_by_alias(config, scope, copy)) {
      return 1;
    }
    if (scope == F2E_SCOPE_LENIENT) {
      /* an ambiguous name is still a declared option; it is accepted but not
         applied, rather than reported as unknown */
      int ambiguous = 0;
      f2e_find_flag_any_scope_by_alias(config, copy, &ambiguous);
      if (ambiguous) {
        return 1;
      }
    }
    if (strncmp(copy, "no-", 3) == 0) {
      F2EFlag *flag = f2e_find_flag_by_alias(config, scope, copy + 3);
      return flag && flag->type == F2E_TYPE_BOOL;
    }
    return 0;
  }
  if (f2e_find_flag_by_short(config, scope, token[1])) {
    return 1;
  }
  if (scope == F2E_SCOPE_LENIENT) {
    int ambiguous = 0;
    f2e_find_flag_any_scope_by_short(config, token[1], &ambiguous);
    return ambiguous;
  }
  return 0;
}

static int f2e_token_looks_like_option(const char *token) {
  return token && token[0] == '-' && token[1] != '\0';
}

static int f2e_can_consume_separated_value(const F2EFlag *flag, const char *token) {
  if (!token || strcmp(token, "--") == 0) {
    return 0;
  }
  if (!f2e_token_looks_like_option(token)) {
    return 1;
  }
  if (flag && flag->type == F2E_TYPE_INT) {
    return f2e_int_value_is_valid(token);
  }
  if (flag && flag->type == F2E_TYPE_FLOAT) {
    return f2e_float_value_is_valid(token);
  }
  if (flag && (flag->type == F2E_TYPE_JSON || flag->type == F2E_TYPE_ARRAY || flag->type == F2E_TYPE_MAP)) {
    return f2e_json_value_is_valid(token);
  }
  return 0;
}

static int f2e_parse_runtime_bool(const char *value, int *out) {
  if (!value || value[0] == '\0') {
    return 0;
  }
  return f2e_parse_config_bool(value, out);
}

static int f2e_runtime_bool_from_env(const char *name, int *out) {
  const char *value = getenv(name);
  return value && value[0] != '\0' && f2e_parse_runtime_bool(value, out);
}

static int f2e_token_sets_allow_unknown(const char *token, int *out) {
  if (!token) {
    return 0;
  }
  if (f2e_streq(token, "--allow-unknown") || f2e_streq(token, "--allow-hidden")) {
    *out = 1;
    return 1;
  }
  if (f2e_streq(token, "--no-allow-unknown") || f2e_streq(token, "--no-allow-hidden")) {
    *out = 0;
    return 1;
  }

  const char allow_unknown[] = "--allow-unknown=";
  const char allow_hidden[] = "--allow-hidden=";
  if (strncmp(token, allow_unknown, sizeof(allow_unknown) - 1) == 0) {
    return f2e_parse_runtime_bool(token + sizeof(allow_unknown) - 1, out);
  }
  if (strncmp(token, allow_hidden, sizeof(allow_hidden) - 1) == 0) {
    return f2e_parse_runtime_bool(token + sizeof(allow_hidden) - 1, out);
  }
  return 0;
}

/*
 * Resolves the starting allow-unknown state. forced_out reports whether a
 * runtime source (env var or --allow-unknown token) chose the value; a forced
 * value applies everywhere, while a config-derived value can still be
 * overridden per command scope by [commands.*] allow_unknown.
 */
static int f2e_resolve_allow_unknown(const F2EConfig *config, int argc, const char *const argv[], int *forced_out) {
  int allow_unknown = config ? config->allow_unknown : 0;
  int forced = 0;
  int parsed = 0;
  if (f2e_runtime_bool_from_env("FLAGS2ENV_ALLOW_UNKNOWN", &parsed) ||
      f2e_runtime_bool_from_env("F2E_ALLOW_UNKNOWN", &parsed) ||
      f2e_runtime_bool_from_env("FLAGS2ENV_ALLOW_HIDDEN", &parsed) ||
      f2e_runtime_bool_from_env("F2E_ALLOW_HIDDEN", &parsed)) {
    allow_unknown = parsed;
    forced = 1;
  }

  for (int i = 0; i < argc; i++) {
    const char *token = argv && argv[i] ? argv[i] : NULL;
    if (!token) {
      continue;
    }
    if (f2e_streq(token, "--")) {
      break;
    }
    if (f2e_token_sets_allow_unknown(token, &parsed)) {
      allow_unknown = parsed;
      forced = 1;
    }
  }
  if (forced_out) {
    *forced_out = forced;
  }
  return allow_unknown;
}

static void f2e_apply_defaults(F2EConfig *config, F2EPair *pairs, size_t pair_count) {
  for (size_t i = 0; i < config->flag_count; i++) {
    F2EFlag *flag = &config->flags[i];
    if (flag->env[0] != '\0' && flag->has_default) {
      char normalized[F2E_MAX_VALUE];
      if (f2e_normalize_flag_value(flag, flag->default_value, normalized, sizeof(normalized))) {
        f2e_set_pair(pairs, pair_count, flag->env, normalized);
      }
    }
  }
}

static int f2e_command_path_contains(const F2ECommandPath *path, int index) {
  if (!path) {
    return 0;
  }
  for (size_t i = 0; i < path->depth; i++) {
    if (path->commands[i] == index) {
      return 1;
    }
  }
  return 0;
}

/* Defaults only apply for global flags and flags scoped to an active command. */
static void f2e_apply_defaults_for_path(F2EConfig *config, F2EPair *pairs, size_t pair_count, const F2ECommandPath *path) {
  for (size_t i = 0; i < config->flag_count; i++) {
    F2EFlag *flag = &config->flags[i];
    if (flag->command != F2E_SCOPE_ROOT && !f2e_command_path_contains(path, flag->command)) {
      continue;
    }
    if (flag->env[0] != '\0' && flag->has_default) {
      char normalized[F2E_MAX_VALUE];
      if (f2e_normalize_flag_value(flag, flag->default_value, normalized, sizeof(normalized))) {
        f2e_set_pair(pairs, pair_count, flag->env, normalized);
      }
    }
  }
}

static int f2e_can_bundle_bool_shorts(F2EConfig *config, int scope, const char *shorts) {
  if (!shorts || shorts[0] == '\0') {
    return 0;
  }
  for (const char *cursor = shorts; *cursor; cursor++) {
    F2EFlag *flag = f2e_find_flag_by_short(config, scope, *cursor);
    if (!flag || flag->env[0] == '\0' || flag->type != F2E_TYPE_BOOL) {
      return 0;
    }
  }
  return 1;
}

static void f2e_apply_bool_short_bundle(F2EConfig *config, int scope, F2EPair *pairs, size_t pair_count, const char *shorts) {
  for (const char *cursor = shorts; *cursor; cursor++) {
    F2EFlag *flag = f2e_find_flag_by_short(config, scope, *cursor);
    if (flag && flag->env[0] != '\0' && flag->type == F2E_TYPE_BOOL) {
      f2e_set_pair(pairs, pair_count, flag->env, "true");
    }
  }
}

static void f2e_apply_long_arg(F2EConfig *config, int scope, F2EPair *pairs, size_t pair_count, const char *token, int *index, int argc, const char *const argv[], F2EJsonList *errors) {
  char name[F2E_MAX_NAME];
  char inline_value[F2E_MAX_VALUE];
  int has_inline_value = 0;
  int negated = 0;

  const char *raw = token + 2;
  f2e_strlcpy(name, raw, sizeof(name));
  char *eq = strchr(name, '=');
  if (eq) {
    *eq = '\0';
    f2e_strlcpy(inline_value, eq + 1, sizeof(inline_value));
    has_inline_value = 1;
  }

  F2EFlag *flag = f2e_find_flag_by_alias(config, scope, name);
  if (!flag && strncmp(name, "no-", 3) == 0) {
    flag = f2e_find_flag_by_alias(config, scope, name + 3);
    if (flag && flag->type == F2E_TYPE_BOOL) {
      negated = 1;
    } else {
      return;
    }
  }
  if (!flag || flag->env[0] == '\0') {
    return;
  }

  if (flag->type == F2E_TYPE_BOOL) {
    if (negated) {
      f2e_set_pair(pairs, pair_count, flag->env, "false");
    } else if (has_inline_value) {
      f2e_try_set_flag_value(flag, pairs, pair_count, inline_value, errors);
    } else if (config->allow_separated_values &&
               *index + 1 < argc &&
               f2e_can_consume_separated_value(flag, argv[*index + 1]) &&
               f2e_try_set_bool_value(flag, pairs, pair_count, argv[*index + 1])) {
      (*index)++;
    } else {
      f2e_set_pair(pairs, pair_count, flag->env, "true");
    }
    return;
  }

  if (has_inline_value) {
    f2e_try_set_flag_value(flag, pairs, pair_count, inline_value, errors);
  } else if (config->allow_separated_values &&
             *index + 1 < argc &&
             f2e_can_consume_separated_value(flag, argv[*index + 1])) {
    (*index)++;
    f2e_try_set_flag_value(flag, pairs, pair_count, argv[*index], errors);
  }
}

static void f2e_apply_short_arg(F2EConfig *config, int scope, F2EPair *pairs, size_t pair_count, const char *token, int *index, int argc, const char *const argv[], F2EJsonList *errors) {
  if (token[1] == '\0') {
    return;
  }

  char short_name = token[1];
  F2EFlag *first = f2e_find_flag_by_short(config, scope, short_name);
  if (!first || first->env[0] == '\0') {
    return;
  }

  const char *rest = token + 2;
  int has_inline_value = 0;
  if (*rest == '=') {
    has_inline_value = 1;
    rest++;
  }

  if (first->type != F2E_TYPE_BOOL) {
    if (*rest) {
      f2e_try_set_flag_value(first, pairs, pair_count, rest, errors);
    } else if (config->allow_separated_values &&
               *index + 1 < argc &&
               f2e_can_consume_separated_value(first, argv[*index + 1])) {
      (*index)++;
      f2e_try_set_flag_value(first, pairs, pair_count, argv[*index], errors);
    }
    return;
  }

  if (has_inline_value) {
    f2e_try_set_flag_value(first, pairs, pair_count, rest, errors);
    return;
  }

  if (*rest == '\0') {
    if (config->allow_separated_values &&
        *index + 1 < argc &&
        f2e_can_consume_separated_value(first, argv[*index + 1]) &&
        f2e_try_set_bool_value(first, pairs, pair_count, argv[*index + 1])) {
      (*index)++;
      return;
    }
    f2e_set_pair(pairs, pair_count, first->env, "true");
    return;
  }

  if (f2e_can_bundle_bool_shorts(config, scope, token + 1)) {
    f2e_apply_bool_short_bundle(config, scope, pairs, pair_count, token + 1);
    return;
  }

  f2e_try_set_flag_value(first, pairs, pair_count, rest, errors);
}

static const char *f2e_audit_flag_name(const F2EFlag *flag) {
  return flag && flag->name[0] != '\0' ? flag->name : "<unnamed>";
}

static void f2e_audit_bool_value_aliases(const F2EFlag *flag, F2EAudit *audit) {
  if (flag->type != F2E_TYPE_BOOL) {
    if (flag->true_alias_count > 0 || flag->false_alias_count > 0) {
      f2e_audit_add(audit, 0, "flags.%s declares boolean value aliases but type is not bool", f2e_audit_flag_name(flag));
    }
    if (flag->has_default && flag->type == F2E_TYPE_INT && !f2e_int_value_is_valid(flag->default_value)) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not a valid integer",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
    if (flag->has_default && flag->type == F2E_TYPE_FLOAT && !f2e_float_value_is_valid(flag->default_value)) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not a valid double",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
    if (flag->has_default && flag->type == F2E_TYPE_JSON && !f2e_json_value_is_valid(flag->default_value)) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not valid JSON",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
    if (flag->has_default && flag->type == F2E_TYPE_ARRAY && !f2e_json_container_is_valid(flag->default_value, '[')) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not a valid JSON array",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
    if (flag->has_default && flag->type == F2E_TYPE_MAP && !f2e_json_container_is_valid(flag->default_value, '{')) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not a valid JSON object",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
    return;
  }

  for (size_t i = 0; i < flag->true_alias_count; i++) {
    if (!f2e_shell_word_is_valid(flag->true_aliases[i])) {
      f2e_audit_add(audit, 1, "flags.%s true_aliases contains unsafe shell token \"%s\"",
                    f2e_audit_flag_name(flag),
                    flag->true_aliases[i]);
    }
    if (f2e_streq(flag->true_aliases[i], "false")) {
      f2e_audit_add(audit, 1, "flags.%s true_aliases contains canonical false", f2e_audit_flag_name(flag));
    } else if (f2e_streq(flag->true_aliases[i], "true")) {
      f2e_audit_add(audit, 0, "flags.%s true_aliases redundantly contains canonical true", f2e_audit_flag_name(flag));
    }
  }

  for (size_t i = 0; i < flag->false_alias_count; i++) {
    if (!f2e_shell_word_is_valid(flag->false_aliases[i])) {
      f2e_audit_add(audit, 1, "flags.%s false_aliases contains unsafe shell token \"%s\"",
                    f2e_audit_flag_name(flag),
                    flag->false_aliases[i]);
    }
    if (f2e_streq(flag->false_aliases[i], "true")) {
      f2e_audit_add(audit, 1, "flags.%s false_aliases contains canonical true", f2e_audit_flag_name(flag));
    } else if (f2e_streq(flag->false_aliases[i], "false")) {
      f2e_audit_add(audit, 0, "flags.%s false_aliases redundantly contains canonical false", f2e_audit_flag_name(flag));
    }
  }

  for (size_t i = 0; i < flag->true_alias_count; i++) {
    for (size_t j = 0; j < flag->false_alias_count; j++) {
      if (f2e_streq(flag->true_aliases[i], flag->false_aliases[j])) {
        f2e_audit_add(audit, 1, "flags.%s value alias \"%s\" appears in both true_aliases and false_aliases",
                      f2e_audit_flag_name(flag),
                      flag->true_aliases[i]);
      }
    }
  }

  if (flag->has_default) {
    const char *canonical = NULL;
    if (!f2e_bool_value_alias(flag, flag->default_value, &canonical)) {
      f2e_audit_add(audit, 1, "flags.%s default \"%s\" is not a valid bool value",
                    f2e_audit_flag_name(flag),
                    flag->default_value);
    }
  }
}

static void f2e_audit_command_semantics(const F2EConfig *config, F2EAudit *audit) {
  if (config->too_many_commands) {
    f2e_audit_add(audit, 1, "too many [commands.*] tables declared (max %d)", F2E_MAX_COMMANDS);
  }
  if (config->has_invalid_command_table) {
    f2e_audit_add(audit, 1,
                  "[%s] is not a valid commands table; nest subcommands with an explicit keyword, e.g. [commands.<name>.commands.<name>.flags.<flag>]",
                  config->invalid_command_table);
  }
  if (config->command_count > 0) {
    if (config->command_env[0] == '\0' || !f2e_env_name_is_valid(config->command_env)) {
      f2e_audit_add(audit, 1, "parse.command_env \"%s\" is not a valid env var name", config->command_env);
    }
  } else if (config->command_env[0] != '\0') {
    f2e_audit_add(audit, 0, "parse.command_env is set but no [commands.*] tables are declared");
  }

  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    char label[F2E_MAX_VALUE];
    if (!f2e_command_path_label(config, (int)i, label, sizeof(label))) {
      f2e_strlcpy(label, command->name, sizeof(label));
    }
    if (!f2e_option_name_is_valid(command->name)) {
      f2e_audit_add(audit, 1, "commands.%s has an unsafe command name", label);
    }
    if (f2e_command_depth(config, (int)i) > F2E_MAX_COMMAND_DEPTH) {
      f2e_audit_add(audit, 1, "commands.%s is nested deeper than %d levels", label, F2E_MAX_COMMAND_DEPTH);
    }
    if (command->env[0] != '\0' && !f2e_env_name_is_valid(command->env)) {
      f2e_audit_add(audit, 1, "commands.%s env \"%s\" is not a valid env var name", label, command->env);
    }
    for (size_t j = 0; j < command->alias_count; j++) {
      const char *alias = command->aliases[j];
      if (alias[0] == '\0' || alias[0] == '-' || !f2e_option_name_is_valid(alias)) {
        f2e_audit_add(audit, 1, "commands.%s alias \"%s\" contains unsafe characters", label, alias);
      } else if (f2e_streq(alias, command->name)) {
        f2e_audit_add(audit, 1, "commands.%s alias \"%s\" duplicates its canonical name", label, alias);
      }
    }
    if (command->env[0] != '\0') {
      for (size_t j = 0; j < config->flag_count; j++) {
        if (f2e_streq(config->flags[j].env, command->env)) {
          f2e_audit_add(audit, 1, "commands.%s env \"%s\" collides with flags.%s env",
                        label,
                        command->env,
                        f2e_audit_flag_name(&config->flags[j]));
        }
      }
      if (config->command_env[0] != '\0' && f2e_streq(command->env, config->command_env)) {
        f2e_audit_add(audit, 1, "commands.%s env \"%s\" collides with parse.command_env", label, command->env);
      }
    }
    for (size_t j = i + 1; j < config->command_count; j++) {
      const F2ECommand *sibling = &config->commands[j];
      if (sibling->parent != command->parent) {
        continue;
      }
      int clash = f2e_streq(command->name, sibling->name);
      for (size_t a = 0; !clash && a < command->alias_count; a++) {
        clash = f2e_streq(command->aliases[a], sibling->name);
        for (size_t b = 0; !clash && b < sibling->alias_count; b++) {
          clash = f2e_streq(command->aliases[a], sibling->aliases[b]);
        }
      }
      for (size_t b = 0; !clash && b < sibling->alias_count; b++) {
        clash = f2e_streq(command->name, sibling->aliases[b]);
      }
      if (clash) {
        char sibling_label[F2E_MAX_VALUE];
        if (!f2e_command_path_label(config, (int)j, sibling_label, sizeof(sibling_label))) {
          f2e_strlcpy(sibling_label, sibling->name, sizeof(sibling_label));
        }
        f2e_audit_add(audit, 1, "commands.%s and commands.%s share a name or alias", label, sibling_label);
      }
    }
  }

  if (config->command_env[0] != '\0' && config->command_count > 0) {
    for (size_t i = 0; i < config->flag_count; i++) {
      if (f2e_streq(config->flags[i].env, config->command_env)) {
        f2e_audit_add(audit, 1, "parse.command_env collides with flags.%s env \"%s\"",
                      f2e_audit_flag_name(&config->flags[i]),
                      config->command_env);
      }
    }
  }
}

static void f2e_audit_config_semantics(const F2EConfig *config, F2EAudit *audit) {
  if (config->flag_count == 0 && config->command_count == 0) {
    f2e_audit_add(audit, 1, "no [flags.*] or [commands.*] tables declared");
    return;
  }

  f2e_audit_command_semantics(config, audit);

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (flag->name[0] == '\0') {
      f2e_audit_add(audit, 1, "flags.%lu has empty name", (unsigned long)i);
    }
    if (flag->env[0] == '\0') {
      f2e_audit_add(audit, 1, "flags.%s is missing env", f2e_audit_flag_name(flag));
    } else if (!f2e_env_name_is_valid(flag->env)) {
      f2e_audit_add(audit, 1, "flags.%s env \"%s\" is not a valid env var name", f2e_audit_flag_name(flag), flag->env);
    }
    if (flag->alias_count == 0) {
      f2e_audit_add(audit, 1, "flags.%s has no long aliases", f2e_audit_flag_name(flag));
    }
    for (size_t j = 0; j < flag->alias_count; j++) {
      const char *alias = flag->aliases[j];
      if (alias[0] == '\0') {
        f2e_audit_add(audit, 1, "flags.%s contains an empty alias", f2e_audit_flag_name(flag));
      } else if (alias[0] == '-') {
        f2e_audit_add(audit, 1, "flags.%s alias \"%s\" should not include leading dashes", f2e_audit_flag_name(flag), alias);
      } else if (!f2e_option_name_is_valid(alias)) {
        f2e_audit_add(audit, 1, "flags.%s alias \"%s\" contains unsafe option characters", f2e_audit_flag_name(flag), alias);
      }
    }
    if (flag->short_name != '\0' && !isalnum((unsigned char)flag->short_name)) {
      f2e_audit_add(audit, 1, "flags.%s has invalid short flag \"%c\"", f2e_audit_flag_name(flag), flag->short_name);
    }
    if (flag->invalid_type) {
      f2e_audit_add(audit, 1, "flags.%s type \"%s\" is not supported",
                    f2e_audit_flag_name(flag),
                    flag->type_value);
    }
    f2e_audit_bool_value_aliases(flag, audit);

    if (config->positionals_env[0] != '\0' && f2e_streq(config->positionals_env, flag->env)) {
      f2e_audit_add(audit, 1, "parse.positionals_env collides with flags.%s env \"%s\"",
                    f2e_audit_flag_name(flag),
                    config->positionals_env);
    }
    if (config->unknown_options_env[0] != '\0' && f2e_streq(config->unknown_options_env, flag->env)) {
      f2e_audit_add(audit, 1, "parse.unknown_options_env collides with flags.%s env \"%s\"",
                    f2e_audit_flag_name(flag),
                    config->unknown_options_env);
    }
    if (config->errors_env[0] != '\0' && f2e_streq(config->errors_env, flag->env)) {
      f2e_audit_add(audit, 1, "parse.errors_env collides with flags.%s env \"%s\"",
                    f2e_audit_flag_name(flag),
                    config->errors_env);
    }
  }

  if (config->positionals_env[0] != '\0' &&
      !f2e_env_name_is_valid(config->positionals_env)) {
    f2e_audit_add(audit, 1, "parse.positionals_env \"%s\" is not a valid env var name",
                  config->positionals_env);
  }
  if (config->unknown_options_env[0] != '\0' &&
      !f2e_env_name_is_valid(config->unknown_options_env)) {
    f2e_audit_add(audit, 1, "parse.unknown_options_env \"%s\" is not a valid env var name",
                  config->unknown_options_env);
  }
  if (config->errors_env[0] != '\0' &&
      !f2e_env_name_is_valid(config->errors_env)) {
    f2e_audit_add(audit, 1, "parse.errors_env \"%s\" is not a valid env var name",
                  config->errors_env);
  }
  if (config->invalid_help_columns) {
    f2e_audit_add(audit, 1, "help.columns must be a list of supported table column names");
  }
  if (config->invalid_help_exclude_columns) {
    f2e_audit_add(audit, 1, "help.exclude must be a list of supported table column names");
  }
  if (config->invalid_env_audit_ignore) {
    f2e_audit_add(audit, 1, "env.ignore must be a list of env var names");
  }
  for (size_t i = 0; i < config->env_audit_ignored_count; i++) {
    if (!f2e_env_name_is_valid(config->env_audit_ignored_keys[i])) {
      f2e_audit_add(audit, 1, "env.ignore contains invalid env var name \"%s\"",
                    config->env_audit_ignored_keys[i]);
    }
  }

  if (config->positionals_env[0] != '\0' &&
      config->unknown_options_env[0] != '\0' &&
      f2e_streq(config->positionals_env, config->unknown_options_env)) {
    f2e_audit_add(audit, 1, "parse.positionals_env and parse.unknown_options_env both use env \"%s\"",
                  config->positionals_env);
  }
  if (config->positionals_env[0] != '\0' &&
      config->errors_env[0] != '\0' &&
      f2e_streq(config->positionals_env, config->errors_env)) {
    f2e_audit_add(audit, 1, "parse.positionals_env and parse.errors_env both use env \"%s\"",
                  config->positionals_env);
  }
  if (config->unknown_options_env[0] != '\0' &&
      config->errors_env[0] != '\0' &&
      f2e_streq(config->unknown_options_env, config->errors_env)) {
    f2e_audit_add(audit, 1, "parse.unknown_options_env and parse.errors_env both use env \"%s\"",
                  config->unknown_options_env);
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *left = &config->flags[i];

    if (left->type == F2E_TYPE_BOOL) {
      for (size_t alias_index = 0; alias_index < left->alias_count; alias_index++) {
        char negated_alias[F2E_MAX_NAME + 3];
        snprintf(negated_alias, sizeof(negated_alias), "no-%s", left->aliases[alias_index]);
        const F2EFlag *clash = f2e_find_flag_by_alias_const(config, left->command, negated_alias);
        if (clash) {
          f2e_audit_add(audit, 1, "alias \"%s\" clashes with negated bool flag flags.%s",
                        negated_alias,
                        f2e_audit_flag_name(left));
        }
      }
    }

    for (size_t j = i + 1; j < config->flag_count; j++) {
      const F2EFlag *right = &config->flags[j];
      /* the same alias, short flag, or env may be reused by a different
         command scope; duplicates are only errors within one scope */
      if (left->command != right->command) {
        continue;
      }
      if (left->env[0] != '\0' && right->env[0] != '\0' && f2e_streq(left->env, right->env)) {
        f2e_audit_add(audit, 1, "flags.%s and flags.%s both map to env \"%s\"",
                      f2e_audit_flag_name(left),
                      f2e_audit_flag_name(right),
                      left->env);
      }
      if (left->short_name != '\0' && right->short_name != '\0' && left->short_name == right->short_name) {
        f2e_audit_add(audit, 1, "flags.%s and flags.%s both use short flag \"%c\"",
                      f2e_audit_flag_name(left),
                      f2e_audit_flag_name(right),
                      left->short_name);
      }
      for (size_t left_alias = 0; left_alias < left->alias_count; left_alias++) {
        for (size_t right_alias = 0; right_alias < right->alias_count; right_alias++) {
          if (f2e_streq(left->aliases[left_alias], right->aliases[right_alias])) {
            f2e_audit_add(audit, 1, "flags.%s and flags.%s both use alias \"%s\"",
                          f2e_audit_flag_name(left),
                          f2e_audit_flag_name(right),
                          left->aliases[left_alias]);
          }
        }
      }
    }
  }
}

static int f2e_config_has_audit_errors(const F2EConfig *config) {
  F2EAudit audit;
  if (!f2e_audit_init(&audit)) {
    return 1;
  }
  f2e_audit_config_semantics(config, &audit);
  int has_errors = audit.failed || audit.error_count > 0;
  f2e_audit_discard(&audit);
  return has_errors;
}

static char *f2e_audit_error_report(const char *message, int *status_out) {
  F2EAudit audit;
  if (!f2e_audit_init(&audit)) {
    if (status_out) {
      *status_out = 1;
    }
    return f2e_empty_json_object();
  }
  f2e_audit_add(&audit, 1, "%s", message);
  return f2e_audit_report(&audit, status_out);
}

static char *f2e_audit_config_from_file_impl(const char *config_path, int *status_out) {
  F2EAudit audit;
  if (!f2e_audit_init(&audit)) {
    if (status_out) {
      *status_out = 1;
    }
    return f2e_empty_json_object();
  }

  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    f2e_audit_add(&audit, 1, "audit allocation failed");
    return f2e_audit_report(&audit, status_out);
  }

  if (!config_path || config_path[0] == '\0') {
    f2e_audit_add(&audit, 1, "config path is empty");
  } else if (!f2e_load_config(config_path, config)) {
    f2e_audit_add(&audit, 1, "could not read config \"%s\"", config_path);
  } else {
    f2e_audit_config_semantics(config, &audit);
  }

  free(config);
  return f2e_audit_report(&audit, status_out);
}

const char *f2e_version(void) {
  return F2E_VERSION;
}

char *f2e_audit_config_from_file(const char *config_path) {
  return f2e_audit_config_from_file_impl(config_path, NULL);
}

char *f2e_audit_config(void) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_audit_error_report("no usable .cli-flags.toml found before HOME", NULL);
  }
  char *result = f2e_audit_config_from_file(path);
  free(path);
  return result;
}

int f2e_audit_config_status_from_file(const char *config_path) {
  int status = 1;
  char *report = f2e_audit_config_from_file_impl(config_path, &status);
  free(report);
  return status;
}

int f2e_audit_config_status(void) {
  int status = 1;
  char *path = f2e_default_config_path();
  if (!path) {
    return 1;
  }
  char *report = f2e_audit_config_from_file_impl(path, &status);
  free(report);
  free(path);
  return status;
}

typedef struct {
  char keys[F2E_MAX_ENV_FILE_KEYS][F2E_MAX_ENV];
  size_t count;
} F2EEnvKeySet;

static char *f2e_sibling_path(const char *path, const char *file_name) {
  if (!path || !file_name || file_name[0] == '\0') {
    return NULL;
  }

  const char *slash = strrchr(path, '/');
#if defined(_WIN32)
  const char *backslash = strrchr(path, '\\');
  if (!slash || (backslash && backslash > slash)) {
    slash = backslash;
  }
#endif

  if (!slash) {
    return f2e_strdup(file_name);
  }

  size_t dir_len = (size_t)(slash - path);
  char separator = *slash;
  if (dir_len == 0) {
    dir_len = 1;
  }

  size_t file_len = strlen(file_name);
  size_t needs_separator = path[dir_len - 1] == '/' || path[dir_len - 1] == '\\' ? 0 : 1;
  if (dir_len > SIZE_MAX - needs_separator - file_len - 1) {
    return NULL;
  }

  char *joined = (char *)malloc(dir_len + needs_separator + file_len + 1);
  if (!joined) {
    return NULL;
  }
  memcpy(joined, path, dir_len);
  size_t offset = dir_len;
  if (needs_separator) {
    joined[offset++] = separator;
  }
  memcpy(joined + offset, file_name, file_len);
  joined[offset + file_len] = '\0';
  return joined;
}

static int f2e_env_key_is_valid(const char *key) {
  return f2e_env_name_is_valid(key);
}

static int f2e_env_keyset_contains(const F2EEnvKeySet *set, const char *key) {
  if (!set || !key) {
    return 0;
  }
  for (size_t i = 0; i < set->count; i++) {
    if (f2e_streq(set->keys[i], key)) {
      return 1;
    }
  }
  return 0;
}

static int f2e_env_keyset_add(F2EEnvKeySet *set, const char *key, int *duplicate_out) {
  if (duplicate_out) {
    *duplicate_out = 0;
  }
  if (!set || !key || key[0] == '\0') {
    return 1;
  }
  if (f2e_env_keyset_contains(set, key)) {
    if (duplicate_out) {
      *duplicate_out = 1;
    }
    return 1;
  }
  if (set->count >= F2E_MAX_ENV_FILE_KEYS) {
    return 0;
  }
  f2e_strlcpy(set->keys[set->count++], key, sizeof(set->keys[0]));
  return 1;
}

static int f2e_config_ignores_env_key(const F2EConfig *config, const char *key) {
  if (!config || !key) {
    return 0;
  }
  for (size_t i = 0; i < config->env_audit_ignored_count; i++) {
    if (f2e_streq(config->env_audit_ignored_keys[i], key)) {
      return 1;
    }
  }
  /* command markers are derived at parse time, so .env files may set or omit
     them freely without tripping the audit */
  if (config->command_count > 0 && config->command_env[0] != '\0' && f2e_streq(config->command_env, key)) {
    return 1;
  }
  for (size_t i = 0; i < config->command_count; i++) {
    if (config->commands[i].env[0] != '\0' && f2e_streq(config->commands[i].env, key)) {
      return 1;
    }
  }
  return 0;
}

static void f2e_collect_config_env_keys(const F2EConfig *config, F2EEnvKeySet *declared, int include_ignored) {
  memset(declared, 0, sizeof(*declared));
  if (!config) {
    return;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    int duplicate = 0;
    if (include_ignored || !f2e_config_ignores_env_key(config, config->flags[i].env)) {
      f2e_env_keyset_add(declared, config->flags[i].env, &duplicate);
    }
  }
  int duplicate = 0;
  if (include_ignored || !f2e_config_ignores_env_key(config, config->positionals_env)) {
    f2e_env_keyset_add(declared, config->positionals_env, &duplicate);
  }
  if (include_ignored || !f2e_config_ignores_env_key(config, config->unknown_options_env)) {
    f2e_env_keyset_add(declared, config->unknown_options_env, &duplicate);
  }
  if (include_ignored || !f2e_config_ignores_env_key(config, config->errors_env)) {
    f2e_env_keyset_add(declared, config->errors_env, &duplicate);
  }
}

static void f2e_audit_env_file_semantics(const F2EConfig *config, const char *env_path, F2EAudit *audit) {
  F2EEnvKeySet declared;
  F2EEnvKeySet declared_all;
  F2EEnvKeySet seen;
  f2e_collect_config_env_keys(config, &declared, 0);
  f2e_collect_config_env_keys(config, &declared_all, 1);
  memset(&seen, 0, sizeof(seen));

  if (declared_all.count == 0) {
    f2e_audit_add(audit, 1, ".cli-flags.toml declares no env keys");
    return;
  }

  FILE *file = fopen(env_path, "r");
  if (!file) {
    f2e_audit_add(audit, 1, "could not read env file \"%s\"", env_path ? env_path : "");
    return;
  }

  char line[F2E_MAX_LINE];
  unsigned long line_no = 0;
  while (fgets(line, sizeof(line), file)) {
    line_no++;
    char *trimmed = f2e_trim(line);
    if (trimmed[0] == '\0' || trimmed[0] == '#') {
      continue;
    }
    if (strncmp(trimmed, "export", 6) == 0 && isspace((unsigned char)trimmed[6])) {
      trimmed = f2e_trim_left(trimmed + 6);
    }

    char *eq = strchr(trimmed, '=');
    if (!eq) {
      f2e_audit_add(audit, 0, ".env line %lu is not KEY=value", line_no);
      continue;
    }
    *eq = '\0';
    char *key = f2e_trim(trimmed);
    if (!f2e_env_key_is_valid(key)) {
      f2e_audit_add(audit, 0, ".env line %lu has invalid key \"%s\"", line_no, key);
      continue;
    }
    if (f2e_config_ignores_env_key(config, key)) {
      continue;
    }

    int duplicate = 0;
    if (!f2e_env_keyset_add(&seen, key, &duplicate)) {
      f2e_audit_add(audit, 1, ".env declares too many keys");
      break;
    }
    if (duplicate) {
      f2e_audit_add(audit, 0, ".env key \"%s\" appears more than once", key);
      continue;
    }
    if (!f2e_env_keyset_contains(&declared, key)) {
      f2e_audit_add(audit, 1, ".env key \"%s\" is not declared by .cli-flags.toml", key);
    }
  }

  fclose(file);

  for (size_t i = 0; i < declared.count; i++) {
    if (!f2e_env_keyset_contains(&seen, declared.keys[i])) {
      f2e_audit_add(audit, 0, ".cli-flags.toml env \"%s\" is not present in .env", declared.keys[i]);
    }
  }
}

static char *f2e_audit_env_file_from_file_impl(const char *config_path, const char *env_path, int *status_out) {
  F2EAudit audit;
  if (!f2e_audit_init(&audit)) {
    if (status_out) {
      *status_out = 1;
    }
    return f2e_empty_json_object();
  }

  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    f2e_audit_add(&audit, 1, "audit allocation failed");
    return f2e_audit_report(&audit, status_out);
  }

  char *resolved_env_path = NULL;
  if (!config_path || config_path[0] == '\0') {
    f2e_audit_add(&audit, 1, "config path is empty");
  } else if (!f2e_load_config(config_path, config)) {
    f2e_audit_add(&audit, 1, "could not read config \"%s\"", config_path);
  } else {
    size_t config_error_count = audit.error_count;
    f2e_audit_config_semantics(config, &audit);
    if (!audit.failed && audit.error_count == config_error_count) {
      resolved_env_path = env_path && env_path[0] != '\0' ? f2e_strdup(env_path) : f2e_sibling_path(config_path, ".env");
      if (!resolved_env_path) {
        f2e_audit_add(&audit, 1, "env path allocation failed");
      } else {
        f2e_audit_env_file_semantics(config, resolved_env_path, &audit);
      }
    }
  }

  free(resolved_env_path);
  free(config);
  return f2e_audit_report(&audit, status_out);
}

char *f2e_audit_env_file_from_file(const char *config_path, const char *env_path) {
  return f2e_audit_env_file_from_file_impl(config_path, env_path, NULL);
}

char *f2e_audit_env_file(void) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_audit_error_report("no usable .cli-flags.toml found before HOME", NULL);
  }
  char *result = f2e_audit_env_file_from_file(path, NULL);
  free(path);
  return result;
}

int f2e_audit_env_file_status_from_file(const char *config_path, const char *env_path) {
  int status = 1;
  char *report = f2e_audit_env_file_from_file_impl(config_path, env_path, &status);
  free(report);
  return status;
}

int f2e_audit_env_file_status(void) {
  int status = 1;
  char *path = f2e_default_config_path();
  if (!path) {
    return 1;
  }
  char *report = f2e_audit_env_file_from_file_impl(path, NULL, &status);
  free(report);
  free(path);
  return status;
}

static int f2e_completion_append_word(F2EBuffer *words, const char *word) {
  if (!word || word[0] == '\0') {
    return 1;
  }
  if (words->len > 0 && !f2e_buffer_append_char(words, ' ')) {
    return 0;
  }
  return f2e_buffer_append(words, word);
}

static int f2e_completion_command_name(const char *command_name, char *out, size_t out_size) {
  return f2e_path_basename_copy(command_name, out, out_size);
}

static void f2e_completion_function_name(const char *command_name, char *out, size_t out_size) {
  const char *command = command_name && command_name[0] != '\0' ? command_name : "flags2env";
  const char prefix[] = "_flags2env_complete_";
  if (out_size == 0) {
    return;
  }
  f2e_strlcpy(out, prefix, out_size);
  size_t len = strlen(out);
  for (const unsigned char *cursor = (const unsigned char *)command; *cursor && len + 1 < out_size; cursor++) {
    out[len++] = isalnum(*cursor) ? (char)*cursor : '_';
  }
  out[len] = '\0';
  if (len == sizeof(prefix) - 1 && len + 7 < out_size) {
    f2e_strlcpy(out + len, "command", out_size - len);
  }
}

static int f2e_completion_add_option_word(F2EBuffer *all_options, const char *prefix, const char *name, const char *suffix) {
  if (!f2e_option_name_is_valid(name)) {
    return 0;
  }
  char option[F2E_MAX_NAME + 8];
  snprintf(option, sizeof(option), "%s%s%s", prefix, name, suffix ? suffix : "");
  return f2e_completion_append_word(all_options, option);
}

static int f2e_completion_add_bool_values(F2EBuffer *bool_values, const F2EFlag *flag) {
  if (!f2e_completion_append_word(bool_values, "true") ||
      !f2e_completion_append_word(bool_values, "false")) {
    return 0;
  }
  for (size_t i = 0; i < flag->true_alias_count; i++) {
    if (!f2e_shell_word_is_valid(flag->true_aliases[i])) {
      return 0;
    }
    if (!f2e_completion_append_word(bool_values, flag->true_aliases[i])) {
      return 0;
    }
  }
  for (size_t i = 0; i < flag->false_alias_count; i++) {
    if (!f2e_shell_word_is_valid(flag->false_aliases[i])) {
      return 0;
    }
    if (!f2e_completion_append_word(bool_values, flag->false_aliases[i])) {
      return 0;
    }
  }
  return 1;
}

static int f2e_completion_collect_commands(const F2EConfig *config, F2EBuffer *command_words) {
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->parent != F2E_SCOPE_ROOT) {
      continue;
    }
    if (!f2e_option_name_is_valid(command->name) ||
        !f2e_completion_append_word(command_words, command->name)) {
      return 0;
    }
    for (size_t j = 0; j < command->alias_count; j++) {
      if (!f2e_option_name_is_valid(command->aliases[j]) ||
          !f2e_completion_append_word(command_words, command->aliases[j])) {
        return 0;
      }
    }
  }
  return 1;
}

static int f2e_completion_collect_bash_words(const F2EConfig *config,
                                             F2EBuffer *all_options,
                                             F2EBuffer *value_options,
                                             F2EBuffer *bool_value_options,
                                             F2EBuffer *bool_values) {
  if (!f2e_buffer_init(all_options) ||
      !f2e_buffer_init(value_options) ||
      !f2e_buffer_init(bool_value_options) ||
      !f2e_buffer_init(bool_values)) {
    return 0;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    /* completion scripts are static; only global options and top-level
       command names are offered */
    if (flag->env[0] == '\0' || flag->command != F2E_SCOPE_ROOT) {
      continue;
    }
    for (size_t j = 0; j < flag->alias_count; j++) {
      if (!f2e_option_name_is_valid(flag->aliases[j])) {
        return 0;
      }
      char option[F2E_MAX_NAME + 4];
      snprintf(option, sizeof(option), "--%s", flag->aliases[j]);
      if (!f2e_completion_append_word(all_options, option)) {
        return 0;
      }
      if (flag->type == F2E_TYPE_BOOL) {
        if (!f2e_completion_append_word(bool_value_options, option) ||
            !f2e_completion_add_option_word(all_options, "--no-", flag->aliases[j], NULL)) {
          return 0;
        }
      } else {
        if (!f2e_completion_append_word(value_options, option) ||
            !f2e_completion_add_option_word(all_options, "--", flag->aliases[j], "=")) {
          return 0;
        }
      }
    }

    if (flag->short_name != '\0') {
      if (!isalnum((unsigned char)flag->short_name)) {
        return 0;
      }
      char short_option[4] = {'-', flag->short_name, '\0', '\0'};
      if (!f2e_completion_append_word(all_options, short_option)) {
        return 0;
      }
      if (flag->type == F2E_TYPE_BOOL) {
        if (!f2e_completion_append_word(bool_value_options, short_option)) {
          return 0;
        }
      } else if (!f2e_completion_append_word(value_options, short_option)) {
        return 0;
      }
    }

    if (flag->type == F2E_TYPE_BOOL && !f2e_completion_add_bool_values(bool_values, flag)) {
      return 0;
    }
  }

  return 1;
}

static void f2e_completion_free_words(F2EBuffer *a, F2EBuffer *b, F2EBuffer *c, F2EBuffer *d) {
  free(a->data);
  free(b->data);
  free(c->data);
  free(d->data);
}

static int f2e_completion_scope_key(const F2EConfig *config, int scope, char *out, size_t out_size) {
  if (scope < 0) {
    if (out_size == 0) {
      return 0;
    }
    out[0] = '\0';
    return 1;
  }
  return f2e_command_path_label(config, scope, out, out_size);
}

/* Effective option words for one command scope: own flags plus inherited,
   unshadowed ancestors, mirroring how the parser resolves them. */
static int f2e_completion_scope_flag_words(const F2EConfig *config,
                                           int scope,
                                           F2EBuffer *all_options,
                                           F2EBuffer *value_options,
                                           F2EBuffer *bool_value_options) {
  size_t indexes[F2E_MAX_FLAGS];
  size_t count = f2e_help_collect_scope_flags(config, scope, indexes);
  for (size_t k = 0; k < count; k++) {
    const F2EFlag *flag = &config->flags[indexes[k]];
    if (flag->env[0] == '\0') {
      continue;
    }
    for (size_t j = 0; j < flag->alias_count; j++) {
      if (!f2e_option_name_is_valid(flag->aliases[j])) {
        return 0;
      }
      char option[F2E_MAX_NAME + 4];
      snprintf(option, sizeof(option), "--%s", flag->aliases[j]);
      if (!f2e_completion_append_word(all_options, option)) {
        return 0;
      }
      if (flag->type == F2E_TYPE_BOOL) {
        if (!f2e_completion_append_word(bool_value_options, option) ||
            !f2e_completion_add_option_word(all_options, "--no-", flag->aliases[j], NULL)) {
          return 0;
        }
      } else {
        if (!f2e_completion_append_word(value_options, option) ||
            !f2e_completion_add_option_word(all_options, "--", flag->aliases[j], "=")) {
          return 0;
        }
      }
    }
    if (flag->short_name != '\0') {
      if (!isalnum((unsigned char)flag->short_name)) {
        return 0;
      }
      if (f2e_find_flag_by_short((F2EConfig *)config, scope, flag->short_name) == flag) {
        char short_option[3] = {'-', flag->short_name, '\0'};
        if (!f2e_completion_append_word(all_options, short_option)) {
          return 0;
        }
        if (flag->type == F2E_TYPE_BOOL) {
          if (!f2e_completion_append_word(bool_value_options, short_option)) {
            return 0;
          }
        } else if (!f2e_completion_append_word(value_options, short_option)) {
          return 0;
        }
      }
    }
  }
  return 1;
}

static int f2e_completion_scope_child_commands(const F2EConfig *config, int scope, F2EBuffer *words) {
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->parent != scope) {
      continue;
    }
    if (!f2e_option_name_is_valid(command->name) ||
        !f2e_completion_append_word(words, command->name)) {
      return 0;
    }
    for (size_t j = 0; j < command->alias_count; j++) {
      if (!f2e_option_name_is_valid(command->aliases[j]) ||
          !f2e_completion_append_word(words, command->aliases[j])) {
        return 0;
      }
    }
  }
  return 1;
}

static int f2e_completion_emit_case_entry(F2EBuffer *script, const char *pattern, const char *value) {
  return f2e_buffer_append(script, "    '") &&
         f2e_buffer_append(script, pattern) &&
         f2e_buffer_append(script, "') printf '%s' '") &&
         f2e_buffer_append(script, value ? value : "") &&
         f2e_buffer_append(script, "' ;;\n");
}

/*
 * Emits the shared scope-lookup helper functions (POSIX case statements, so
 * they work in both bash 3.2 and zsh):
 *   <fn>_opts <scope>            effective option words for the scope
 *   <fn>_value_opts <scope>      options that consume a separate value
 *   <fn>_bool_value_opts <scope> bool options that may consume a value
 *   <fn>_cmds <scope>            child command words for the scope
 *   <fn>_child <scope> <word>    resolved child scope key, or empty
 *   <fn>_consumes_value <scope> <previous> <word>
 *                                whether word is an option value, not a command
 */
static int f2e_completion_emit_scope_helpers(const F2EConfig *config, F2EBuffer *script, const char *function_name, const char *bool_values) {
  int scopes[F2E_MAX_COMMANDS + 1];
  size_t scope_count = 0;
  scopes[scope_count++] = F2E_SCOPE_ROOT;
  for (size_t i = 0; i < config->command_count; i++) {
    scopes[scope_count++] = (int)i;
  }

  const char *suffixes[] = {"_opts", "_value_opts", "_bool_value_opts"};
  for (size_t which = 0; which < 3; which++) {
    if (!f2e_buffer_append(script, function_name) ||
        !f2e_buffer_append(script, suffixes[which]) ||
        !f2e_buffer_append(script, "() {\n  case \"$1\" in\n")) {
      return 0;
    }
    for (size_t s = 0; s < scope_count; s++) {
      char key[F2E_MAX_VALUE];
      if (!f2e_completion_scope_key(config, scopes[s], key, sizeof(key))) {
        return 0;
      }
      F2EBuffer all_options = {0};
      F2EBuffer value_options = {0};
      F2EBuffer bool_value_options = {0};
      if (!f2e_buffer_init(&all_options) || !f2e_buffer_init(&value_options) || !f2e_buffer_init(&bool_value_options)) {
        free(all_options.data);
        free(value_options.data);
        free(bool_value_options.data);
        return 0;
      }
      int collected = f2e_completion_scope_flag_words(config, scopes[s], &all_options, &value_options, &bool_value_options);
      const F2EBuffer *chosen = which == 0 ? &all_options : which == 1 ? &value_options : &bool_value_options;
      int ok = collected && f2e_completion_emit_case_entry(script, key, chosen->data);
      free(all_options.data);
      free(value_options.data);
      free(bool_value_options.data);
      if (!ok) {
        return 0;
      }
    }
    if (!f2e_buffer_append(script, "    *) printf '%s' '' ;;\n  esac\n}\n")) {
      return 0;
    }
  }

  if (!f2e_buffer_append(script, function_name) ||
      !f2e_buffer_append(script, "_cmds() {\n  case \"$1\" in\n")) {
    return 0;
  }
  for (size_t s = 0; s < scope_count; s++) {
    char key[F2E_MAX_VALUE];
    if (!f2e_completion_scope_key(config, scopes[s], key, sizeof(key))) {
      return 0;
    }
    F2EBuffer words = {0};
    if (!f2e_buffer_init(&words)) {
      return 0;
    }
    int ok = f2e_completion_scope_child_commands(config, scopes[s], &words) &&
             f2e_completion_emit_case_entry(script, key, words.data);
    free(words.data);
    if (!ok) {
      return 0;
    }
  }
  if (!f2e_buffer_append(script, "    *) printf '%s' '' ;;\n  esac\n}\n")) {
    return 0;
  }

  if (!f2e_buffer_append(script, function_name) ||
      !f2e_buffer_append(script, "_child() {\n  case \"$1|$2\" in\n")) {
    return 0;
  }
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    char parent_key[F2E_MAX_VALUE];
    char child_key[F2E_MAX_VALUE];
    if (!f2e_completion_scope_key(config, command->parent, parent_key, sizeof(parent_key)) ||
        !f2e_completion_scope_key(config, (int)i, child_key, sizeof(child_key))) {
      return 0;
    }
    char pattern[F2E_MAX_VALUE * 2];
    snprintf(pattern, sizeof(pattern), "%s|%s", parent_key, command->name);
    if (!f2e_completion_emit_case_entry(script, pattern, child_key)) {
      return 0;
    }
    for (size_t j = 0; j < command->alias_count; j++) {
      snprintf(pattern, sizeof(pattern), "%s|%s", parent_key, command->aliases[j]);
      if (!f2e_completion_emit_case_entry(script, pattern, child_key)) {
        return 0;
      }
    }
  }
  if (!f2e_buffer_append(script, "    *) printf '%s' '' ;;\n  esac\n}\n") ||
      !f2e_buffer_append(script, function_name) ||
      !f2e_buffer_append(script, "_consumes_value() {\n"
                                 "  local value_opts bool_value_opts\n"
                                 "  value_opts=\"$(") ||
      !f2e_buffer_append(script, function_name) ||
      !f2e_buffer_append(script, "_value_opts \"$1\")\"\n"
                                 "  case \" $value_opts \" in\n"
                                 "    *\" $2 \"*) return 0 ;;\n"
                                 "  esac\n"
                                 "  bool_value_opts=\"$(") ||
      !f2e_buffer_append(script, function_name) ||
      !f2e_buffer_append(script, "_bool_value_opts \"$1\")\"\n"
                                 "  case \" $bool_value_opts \" in\n"
                                 "    *\" $2 \"*)\n"
                                 "      case \" ") ||
      !f2e_buffer_append(script, bool_values ? bool_values : "") ||
      !f2e_buffer_append(script, " \" in\n"
                                 "        *\" $3 \"*) return 0 ;;\n"
                                 "      esac\n"
                                 "      ;;\n"
                                 "  esac\n"
                                 "  return 1\n"
                                 "}\n")) {
    return 0;
  }
  return 1;
}

/*
 * Scope-aware bash completion for configs with [commands.*]: the generated
 * function walks the words typed so far to find the active command scope,
 * then offers that scope's options and child commands. Still fully static —
 * neither flags2env nor the TOML file is consulted at completion time.
 */
static char *f2e_completion_script_bash_commands(const F2EConfig *config, const char *command_name) {
  F2EBuffer bool_values = {0};
  memset(&bool_values, 0, sizeof(bool_values));
  if (!f2e_buffer_init(&bool_values)) {
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (flag->env[0] != '\0' && flag->type == F2E_TYPE_BOOL &&
        !f2e_completion_add_bool_values(&bool_values, flag)) {
      free(bool_values.data);
      return NULL;
    }
  }

  char command[F2E_MAX_NAME];
  if (!f2e_completion_command_name(command_name, command, sizeof(command))) {
    free(bool_values.data);
    return NULL;
  }
  char function_name[F2E_MAX_NAME * 2];
  f2e_completion_function_name(command, function_name, sizeof(function_name));

  F2EBuffer script = {0};
  if (!f2e_buffer_init(&script)) {
    free(bool_values.data);
    return NULL;
  }

  int ok = f2e_buffer_append(&script, "# flags2env bash completion (subcommand-aware)\n") &&
           f2e_completion_emit_scope_helpers(config, &script, function_name, bool_values.data) &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "() {\n"
                                      "  local cur prev opt opts value_opts bool_value_opts bool_values cmds scope child w i matched stopped\n"
                                      "  COMPREPLY=()\n"
                                      "  cur=\"${COMP_WORDS[COMP_CWORD]}\"\n"
                                      "  prev=\"${COMP_WORDS[COMP_CWORD-1]}\"\n"
                                      "  scope=''\n"
                                      "  matched=0\n"
                                      "  stopped=0\n"
                                      "  for ((i=1; i<COMP_CWORD; i++)); do\n"
                                      "    w=\"${COMP_WORDS[i]}\"\n"
                                      "    if [ \"$w\" = \"--\" ]; then\n"
                                      "      return 0\n"
                                      "    fi\n") &&
           f2e_buffer_append(&script, "    if ") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_consumes_value \"$scope\" \"${COMP_WORDS[i-1]}\" \"$w\"; then\n"
                                      "      continue\n"
                                      "    fi\n"
                                      "    case \"$w\" in\n"
                                      "      -*) continue ;;\n"
                                      "    esac\n"
                                      "    if [ \"$stopped\" = 1 ]; then\n"
                                      "      continue\n"
                                      "    fi\n"
                                      "    child=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_child \"$scope\" \"$w\")\"\n"
                                      "    if [ -n \"$child\" ]; then\n"
                                      "      scope=\"$child\"\n"
                                      "      matched=1\n"
                                      "    elif [ \"$matched\" = 1 ]; then\n"
                                      "      stopped=1\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_opts \"$scope\")\"\n  value_opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_value_opts \"$scope\")\"\n  bool_value_opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_bool_value_opts \"$scope\")\"\n  bool_values=") &&
           f2e_buffer_append_shell_single_quoted(&script, bool_values.data) &&
           f2e_buffer_append(&script, "\n  cmds=''\n"
                                      "  if [ \"$stopped\" = 0 ]; then\n"
                                      "    cmds=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_cmds \"$scope\")\"\n"
                                      "  fi\n"
                                      "  for opt in $bool_value_opts; do\n"
                                      "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                      "      COMPREPLY=( $(compgen -W \"$bool_values\" -- \"$cur\") )\n"
                                      "      return 0\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  for opt in $value_opts; do\n"
                                      "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                      "      return 0\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  case \"$cur\" in\n"
                                      "    -*) COMPREPLY=( $(compgen -W \"$opts\" -- \"$cur\") ) ;;\n"
                                      "    *)\n"
                                      "      if [ -n \"$cmds\" ]; then\n"
                                      "        COMPREPLY=( $(compgen -W \"$cmds\" -- \"$cur\") )\n"
                                      "      fi\n"
                                      "      ;;\n"
                                      "  esac\n"
                                      "  return 0\n"
                                      "}\n"
                                      "complete -o default -F ") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, " -- ") &&
           f2e_buffer_append_shell_single_quoted(&script, command) &&
           f2e_buffer_append_char(&script, '\n');

  free(bool_values.data);
  if (!ok) {
    free(script.data);
    return NULL;
  }
  return script.data;
}

static char *f2e_completion_script_bash(const F2EConfig *config, const char *command_name) {
  if (config->command_count > 0) {
    return f2e_completion_script_bash_commands(config, command_name);
  }
  F2EBuffer options = {0};
  F2EBuffer value_options = {0};
  F2EBuffer bool_value_options = {0};
  F2EBuffer bool_values = {0};
  F2EBuffer command_words = {0};
  memset(&options, 0, sizeof(options));
  memset(&value_options, 0, sizeof(value_options));
  memset(&bool_value_options, 0, sizeof(bool_value_options));
  memset(&bool_values, 0, sizeof(bool_values));
  memset(&command_words, 0, sizeof(command_words));
  if (!f2e_completion_collect_bash_words(config, &options, &value_options, &bool_value_options, &bool_values) ||
      !f2e_buffer_init(&command_words) ||
      !f2e_completion_collect_commands(config, &command_words)) {
    f2e_completion_free_words(&options, &value_options, &bool_value_options, &bool_values);
    free(command_words.data);
    return NULL;
  }

  F2EBuffer script = {0};
  if (!f2e_buffer_init(&script)) {
    f2e_completion_free_words(&options, &value_options, &bool_value_options, &bool_values);
    free(command_words.data);
    return NULL;
  }

  char command[F2E_MAX_NAME];
  if (!f2e_completion_command_name(command_name, command, sizeof(command))) {
    f2e_completion_free_words(&options, &value_options, &bool_value_options, &bool_values);
    free(command_words.data);
    free(script.data);
    return NULL;
  }
  char function_name[F2E_MAX_NAME * 2];
  f2e_completion_function_name(command, function_name, sizeof(function_name));

  if (!f2e_buffer_append(&script, "# flags2env bash completion\n") ||
      !f2e_buffer_append(&script, function_name) ||
      !f2e_buffer_append(&script, "() {\n"
                                  "  local cur prev opt opts value_opts bool_value_opts bool_values cmds\n"
                                  "  COMPREPLY=()\n"
                                  "  cur=\"${COMP_WORDS[COMP_CWORD]}\"\n"
                                  "  prev=\"${COMP_WORDS[COMP_CWORD-1]}\"\n"
                                  "  opts=") ||
      !f2e_buffer_append_shell_single_quoted(&script, options.data) ||
      !f2e_buffer_append(&script, "\n  value_opts=") ||
      !f2e_buffer_append_shell_single_quoted(&script, value_options.data) ||
      !f2e_buffer_append(&script, "\n  bool_value_opts=") ||
      !f2e_buffer_append_shell_single_quoted(&script, bool_value_options.data) ||
      !f2e_buffer_append(&script, "\n  bool_values=") ||
      !f2e_buffer_append_shell_single_quoted(&script, bool_values.data) ||
      !f2e_buffer_append(&script, "\n  cmds=") ||
      !f2e_buffer_append_shell_single_quoted(&script, command_words.data) ||
      !f2e_buffer_append(&script, "\n"
                                  "  for opt in $bool_value_opts; do\n"
                                  "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                  "      COMPREPLY=( $(compgen -W \"$bool_values\" -- \"$cur\") )\n"
                                  "      return 0\n"
                                  "    fi\n"
                                  "  done\n"
                                  "  for opt in $value_opts; do\n"
                                  "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                  "      return 0\n"
                                  "    fi\n"
                                  "  done\n"
                                  "  case \"$cur\" in\n"
                                  "    -*) COMPREPLY=( $(compgen -W \"$opts\" -- \"$cur\") ) ;;\n"
                                  "    *)\n"
                                  "      if [ -n \"$cmds\" ]; then\n"
                                  "        COMPREPLY=( $(compgen -W \"$cmds\" -- \"$cur\") )\n"
                                  "      fi\n"
                                  "      ;;\n"
                                  "  esac\n"
                                  "  return 0\n"
                                  "}\n"
                                  "complete -o default -F ") ||
      !f2e_buffer_append(&script, function_name) ||
      !f2e_buffer_append(&script, " -- ") ||
      !f2e_buffer_append_shell_single_quoted(&script, command) ||
      !f2e_buffer_append_char(&script, '\n')) {
    free(script.data);
    f2e_completion_free_words(&options, &value_options, &bool_value_options, &bool_values);
    free(command_words.data);
    return NULL;
  }

  f2e_completion_free_words(&options, &value_options, &bool_value_options, &bool_values);
  free(command_words.data);
  return script.data;
}

static int f2e_completion_zsh_append_spec(F2EBuffer *script, const F2EBuffer *spec) {
  return f2e_buffer_append(script, "    ") &&
         f2e_buffer_append_shell_single_quoted(script, spec->data) &&
         f2e_buffer_append(script, " \\\n");
}

static int f2e_completion_zsh_option_spec(F2EBuffer *script, const char *option, const F2EFlag *flag, int bool_negated) {
  F2EBuffer spec = {0};
  F2EBuffer values = {0};
  if (!f2e_buffer_init(&spec)) {
    return 0;
  }
  memset(&values, 0, sizeof(values));
  if (flag->env[0] != '\0' && !f2e_env_name_is_valid(flag->env)) {
    free(spec.data);
    return 0;
  }
  if (!f2e_buffer_append(&spec, option) ||
      !f2e_buffer_append_char(&spec, '[') ||
      !f2e_buffer_append(&spec, flag->env[0] != '\0' ? flag->env : f2e_audit_flag_name(flag)) ||
      !f2e_buffer_append_char(&spec, ']')) {
    free(spec.data);
    return 0;
  }

  if (flag->type == F2E_TYPE_BOOL && !bool_negated) {
    if (!f2e_buffer_init(&values) ||
        !f2e_completion_add_bool_values(&values, flag) ||
        !f2e_buffer_append(&spec, "::value:(") ||
        !f2e_buffer_append(&spec, values.data) ||
        !f2e_buffer_append_char(&spec, ')')) {
      free(values.data);
      free(spec.data);
      return 0;
    }
    free(values.data);
  } else if (flag->type != F2E_TYPE_BOOL) {
    if (!f2e_buffer_append(&spec, ":value:")) {
      free(spec.data);
      return 0;
    }
  }

  int ok = f2e_completion_zsh_append_spec(script, &spec);
  free(spec.data);
  return ok;
}

/*
 * Scope-aware zsh completion for configs with [commands.*]: shares the same
 * generated scope-lookup helpers as the bash script (plain POSIX case
 * statements) and offers scope options, child commands, and bool values.
 */
static char *f2e_completion_script_zsh_commands(const F2EConfig *config, const char *command_name) {
  F2EBuffer bool_values = {0};
  memset(&bool_values, 0, sizeof(bool_values));
  if (!f2e_buffer_init(&bool_values)) {
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (flag->env[0] != '\0' && flag->type == F2E_TYPE_BOOL &&
        !f2e_completion_add_bool_values(&bool_values, flag)) {
      free(bool_values.data);
      return NULL;
    }
  }

  char command[F2E_MAX_NAME];
  if (!f2e_completion_command_name(command_name, command, sizeof(command))) {
    free(bool_values.data);
    return NULL;
  }
  char function_name[F2E_MAX_NAME * 2];
  f2e_completion_function_name(command, function_name, sizeof(function_name));

  F2EBuffer script = {0};
  if (!f2e_buffer_init(&script)) {
    free(bool_values.data);
    return NULL;
  }

  int ok = f2e_buffer_append(&script, "#compdef ") &&
           f2e_buffer_append(&script, command) &&
           f2e_buffer_append(&script, "\n# flags2env zsh completion (subcommand-aware)\n") &&
           f2e_completion_emit_scope_helpers(config, &script, function_name, bool_values.data) &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "() {\n"
                                      "  local cur prev opt opts value_opts bool_value_opts bool_values cmds scope child w i matched stopped\n"
                                      "  cur=\"${words[CURRENT]}\"\n"
                                      "  prev=\"${words[CURRENT-1]}\"\n"
                                      "  scope=''\n"
                                      "  matched=0\n"
                                      "  stopped=0\n"
                                      "  for ((i=2; i<CURRENT; i++)); do\n"
                                      "    w=\"${words[i]}\"\n"
                                      "    if [ \"$w\" = \"--\" ]; then\n"
                                      "      _files\n"
                                      "      return\n"
                                      "    fi\n") &&
           f2e_buffer_append(&script, "    if ") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_consumes_value \"$scope\" \"${words[i-1]}\" \"$w\"; then\n"
                                      "      continue\n"
                                      "    fi\n"
                                      "    case \"$w\" in\n"
                                      "      -*) continue ;;\n"
                                      "    esac\n"
                                      "    if [ \"$stopped\" = 1 ]; then\n"
                                      "      continue\n"
                                      "    fi\n"
                                      "    child=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_child \"$scope\" \"$w\")\"\n"
                                      "    if [ -n \"$child\" ]; then\n"
                                      "      scope=\"$child\"\n"
                                      "      matched=1\n"
                                      "    elif [ \"$matched\" = 1 ]; then\n"
                                      "      stopped=1\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_opts \"$scope\")\"\n  value_opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_value_opts \"$scope\")\"\n  bool_value_opts=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_bool_value_opts \"$scope\")\"\n  bool_values=") &&
           f2e_buffer_append_shell_single_quoted(&script, bool_values.data) &&
           f2e_buffer_append(&script, "\n  cmds=''\n"
                                      "  if [ \"$stopped\" = 0 ]; then\n"
                                      "    cmds=\"$(") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, "_cmds \"$scope\")\"\n"
                                      "  fi\n"
                                      "  for opt in ${=bool_value_opts}; do\n"
                                      "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                      "      compadd -- ${=bool_values}\n"
                                      "      return\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  for opt in ${=value_opts}; do\n"
                                      "    if [ \"$prev\" = \"$opt\" ]; then\n"
                                      "      _files\n"
                                      "      return\n"
                                      "    fi\n"
                                      "  done\n"
                                      "  case \"$cur\" in\n"
                                      "    -*) compadd -- ${=opts} ;;\n"
                                      "    *)\n"
                                      "      if [ -n \"$cmds\" ]; then\n"
                                      "        compadd -- ${=cmds}\n"
                                      "      else\n"
                                      "        _files\n"
                                      "      fi\n"
                                      "      ;;\n"
                                      "  esac\n"
                                      "}\n") &&
           f2e_buffer_append(&script, function_name) &&
           f2e_buffer_append(&script, " \"$@\"\n");

  free(bool_values.data);
  if (!ok) {
    free(script.data);
    return NULL;
  }
  return script.data;
}

static char *f2e_completion_script_zsh(const F2EConfig *config, const char *command_name) {
  if (config->command_count > 0) {
    return f2e_completion_script_zsh_commands(config, command_name);
  }
  F2EBuffer script = {0};
  if (!f2e_buffer_init(&script)) {
    return NULL;
  }

  char command[F2E_MAX_NAME];
  if (!f2e_completion_command_name(command_name, command, sizeof(command))) {
    free(script.data);
    return NULL;
  }
  char function_name[F2E_MAX_NAME * 2];
  f2e_completion_function_name(command, function_name, sizeof(function_name));

  if (!f2e_buffer_append(&script, "#compdef ") ||
      !f2e_buffer_append(&script, command) ||
      !f2e_buffer_append_char(&script, '\n') ||
      !f2e_buffer_append(&script, function_name) ||
      !f2e_buffer_append(&script, "() {\n  _arguments -s \\\n")) {
    free(script.data);
    return NULL;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (flag->env[0] == '\0' || flag->command != F2E_SCOPE_ROOT) {
      continue;
    }
    for (size_t j = 0; j < flag->alias_count; j++) {
      if (!f2e_option_name_is_valid(flag->aliases[j])) {
        free(script.data);
        return NULL;
      }
      char option[F2E_MAX_NAME + 8];
      snprintf(option, sizeof(option), "--%s", flag->aliases[j]);
      if (!f2e_completion_zsh_option_spec(&script, option, flag, 0)) {
        free(script.data);
        return NULL;
      }
      if (flag->type == F2E_TYPE_BOOL) {
        snprintf(option, sizeof(option), "--no-%s", flag->aliases[j]);
        if (!f2e_completion_zsh_option_spec(&script, option, flag, 1)) {
          free(script.data);
          return NULL;
        }
      }
    }
    if (flag->short_name != '\0') {
      if (!isalnum((unsigned char)flag->short_name)) {
        free(script.data);
        return NULL;
      }
      char option[4] = {'-', flag->short_name, '\0', '\0'};
      if (!f2e_completion_zsh_option_spec(&script, option, flag, 0)) {
        free(script.data);
        return NULL;
      }
    }
  }

  if (f2e_command_has_children(config, F2E_SCOPE_ROOT)) {
    F2EBuffer command_words = {0};
    if (!f2e_buffer_init(&command_words) ||
        !f2e_completion_collect_commands(config, &command_words)) {
      free(command_words.data);
      free(script.data);
      return NULL;
    }
    F2EBuffer spec = {0};
    int ok = f2e_buffer_init(&spec) &&
             f2e_buffer_append(&spec, "1:command:(") &&
             f2e_buffer_append(&spec, command_words.data) &&
             f2e_buffer_append_char(&spec, ')') &&
             f2e_completion_zsh_append_spec(&script, &spec);
    free(spec.data);
    free(command_words.data);
    if (!ok) {
      free(script.data);
      return NULL;
    }
  }

  if (!f2e_buffer_append(&script, "    '*::arg:->args'\n}\n") ||
      !f2e_buffer_append(&script, function_name) ||
      !f2e_buffer_append(&script, " \"$@\"\n")) {
    free(script.data);
    return NULL;
  }

  return script.data;
}

static char *f2e_completion_script_from_config(const F2EConfig *config, const char *shell, const char *command_name) {
  if (!config || !shell || shell[0] == '\0') {
    return NULL;
  }
  if (f2e_streq(shell, "bash")) {
    return f2e_completion_script_bash(config, command_name);
  }
  if (f2e_streq(shell, "zsh")) {
    return f2e_completion_script_zsh(config, command_name);
  }
  return NULL;
}

char *f2e_completion_script_from_file(const char *config_path, const char *shell, const char *command_name) {
  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return NULL;
  }
  if (f2e_config_has_audit_errors(config)) {
    free(config);
    return NULL;
  }
  char *script = f2e_completion_script_from_config(config, shell, command_name);
  free(config);
  return script;
}

char *f2e_completion_script(const char *shell, const char *command_name) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *script = f2e_completion_script_from_file(path, shell, command_name);
  free(path);
  return script;
}

static int f2e_codegen_identifier_is_valid(const char *value) {
  if (!value || !(isalpha((unsigned char)value[0]) || value[0] == '_')) {
    return 0;
  }
  for (const char *cursor = value + 1; *cursor; cursor++) {
    if (!(isalnum((unsigned char)*cursor) || *cursor == '_')) {
      return 0;
    }
  }
  return 1;
}

static const char *f2e_codegen_typescript_type(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "boolean";
    case F2E_TYPE_INT:
    case F2E_TYPE_FLOAT:
      return "number";
    case F2E_TYPE_JSON:
      return "unknown";
    case F2E_TYPE_ARRAY:
      return "unknown[]";
    case F2E_TYPE_MAP:
      return "Record<string, unknown>";
    case F2E_TYPE_STRING:
    default:
      return "string";
  }
}

static const char *f2e_codegen_python_type(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "bool";
    case F2E_TYPE_INT:
      return "int";
    case F2E_TYPE_FLOAT:
      return "float";
    case F2E_TYPE_JSON:
      return "Any";
    case F2E_TYPE_ARRAY:
      return "List[Any]";
    case F2E_TYPE_MAP:
      return "Dict[str, Any]";
    case F2E_TYPE_STRING:
    default:
      return "str";
  }
}

static const char *f2e_codegen_go_type(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "bool";
    case F2E_TYPE_INT:
      return "int64";
    case F2E_TYPE_FLOAT:
      return "float64";
    case F2E_TYPE_JSON:
      return "any";
    case F2E_TYPE_ARRAY:
      return "[]any";
    case F2E_TYPE_MAP:
      return "map[string]any";
    case F2E_TYPE_STRING:
    default:
      return "string";
  }
}

static const char *f2e_codegen_rust_type(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "bool";
    case F2E_TYPE_INT:
      return "i64";
    case F2E_TYPE_FLOAT:
      return "f64";
    case F2E_TYPE_JSON:
      return "serde_json::Value";
    case F2E_TYPE_ARRAY:
      return "Vec<serde_json::Value>";
    case F2E_TYPE_MAP:
      return "std::collections::HashMap<String, serde_json::Value>";
    case F2E_TYPE_STRING:
    default:
      return "String";
  }
}

static const char *f2e_codegen_java_type(F2EValueType type, int optional) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return optional ? "Boolean" : "boolean";
    case F2E_TYPE_INT:
      return optional ? "Long" : "long";
    case F2E_TYPE_FLOAT:
      return optional ? "Double" : "double";
    case F2E_TYPE_JSON:
      return "Object";
    case F2E_TYPE_ARRAY:
      return "List<Object>";
    case F2E_TYPE_MAP:
      return "Map<String, Object>";
    case F2E_TYPE_STRING:
    default:
      return "String";
  }
}

static const char *f2e_codegen_csharp_type(F2EValueType type) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return "bool";
    case F2E_TYPE_INT:
      return "long";
    case F2E_TYPE_FLOAT:
      return "double";
    case F2E_TYPE_JSON:
      return "JsonElement";
    case F2E_TYPE_ARRAY:
      return "IReadOnlyList<JsonElement>";
    case F2E_TYPE_MAP:
      return "IReadOnlyDictionary<string, JsonElement>";
    case F2E_TYPE_STRING:
    default:
      return "string";
  }
}

static const char *f2e_codegen_dart_type(F2EValueType type, int optional) {
  switch (type) {
    case F2E_TYPE_BOOL:
      return optional ? "bool?" : "bool";
    case F2E_TYPE_INT:
      return optional ? "int?" : "int";
    case F2E_TYPE_FLOAT:
      return optional ? "double?" : "double";
    case F2E_TYPE_JSON:
      return "Object?";
    case F2E_TYPE_ARRAY:
      return optional ? "List<Object?>?" : "List<Object?>";
    case F2E_TYPE_MAP:
      return optional ? "Map<String, Object?>?" : "Map<String, Object?>";
    case F2E_TYPE_STRING:
    default:
      return optional ? "String?" : "String";
  }
}

static char *f2e_codegen_typescript(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "/* Generated by flags2env from .cli-flags.toml. Do not edit. */\n\nexport interface ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, " {\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "  ") ||
        !f2e_buffer_append(&source, flag->env) ||
        (!flag->has_default && !f2e_buffer_append_char(&source, '?')) ||
        !f2e_buffer_append(&source, ": ") ||
        !f2e_buffer_append(&source, f2e_codegen_typescript_type(flag->type)) ||
        !f2e_buffer_append(&source, ";\n")) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_codegen_python(const F2EConfig *config, const char *type_name) {
  size_t optional_count = 0;
  for (size_t i = 0; i < config->flag_count; i++) {
    optional_count += config->flags[i].has_default ? 0u : 1u;
  }

  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "# Generated by flags2env from .cli-flags.toml. Do not edit.\n\nfrom typing import Any, Dict, List, TypedDict\n\n")) {
    free(source.data);
    return NULL;
  }

  if (optional_count > 0) {
    if (!f2e_buffer_append(&source, "class _") ||
        !f2e_buffer_append(&source, type_name) ||
        !f2e_buffer_append(&source, "Optional(TypedDict, total=False):\n")) {
      free(source.data);
      return NULL;
    }
    for (size_t i = 0; i < config->flag_count; i++) {
      const F2EFlag *flag = &config->flags[i];
      if (flag->has_default) {
        continue;
      }
      if (!f2e_buffer_append(&source, "    ") ||
          !f2e_buffer_append(&source, flag->env) ||
          !f2e_buffer_append(&source, ": ") ||
          !f2e_buffer_append(&source, f2e_codegen_python_type(flag->type)) ||
          !f2e_buffer_append_char(&source, '\n')) {
        free(source.data);
        return NULL;
      }
    }
    if (!f2e_buffer_append_char(&source, '\n') ||
        !f2e_buffer_append(&source, "class ") ||
        !f2e_buffer_append(&source, type_name) ||
        !f2e_buffer_append(&source, "(_") ||
        !f2e_buffer_append(&source, type_name) ||
        !f2e_buffer_append(&source, "Optional):\n")) {
      free(source.data);
      return NULL;
    }
  } else if (!f2e_buffer_append(&source, "class ") ||
             !f2e_buffer_append(&source, type_name) ||
             !f2e_buffer_append(&source, "(TypedDict):\n")) {
    free(source.data);
    return NULL;
  }

  size_t required_count = 0;
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!flag->has_default) {
      continue;
    }
    required_count++;
    if (!f2e_buffer_append(&source, "    ") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, ": ") ||
        !f2e_buffer_append(&source, f2e_codegen_python_type(flag->type)) ||
        !f2e_buffer_append_char(&source, '\n')) {
      free(source.data);
      return NULL;
    }
  }
  if (required_count == 0 && !f2e_buffer_append(&source, "    pass\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_codegen_go(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "// Code generated by flags2env from .cli-flags.toml. DO NOT EDIT.\n\npackage generated\n\ntype ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, " struct {\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "\t") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append_char(&source, ' ') ||
        (!flag->has_default && !f2e_buffer_append_char(&source, '*')) ||
        !f2e_buffer_append(&source, f2e_codegen_go_type(flag->type)) ||
        !f2e_buffer_append(&source, " `json:\"") ||
        !f2e_buffer_append(&source, flag->env) ||
        (!flag->has_default && !f2e_buffer_append(&source, ",omitempty")) ||
        !f2e_buffer_append(&source, "\"`\n")) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_codegen_rust(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "// Generated by flags2env from .cli-flags.toml. Do not edit.\n\n#[allow(non_snake_case)]\n#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]\npub struct ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, " {\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!flag->has_default && !f2e_buffer_append(&source, "    #[serde(default, skip_serializing_if = \"Option::is_none\")]\n")) {
      free(source.data);
      return NULL;
    }
    if (!f2e_buffer_append(&source, "    pub ") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, ": ") ||
        (!flag->has_default && !f2e_buffer_append(&source, "Option<")) ||
        !f2e_buffer_append(&source, f2e_codegen_rust_type(flag->type)) ||
        (!flag->has_default && !f2e_buffer_append_char(&source, '>')) ||
        !f2e_buffer_append(&source, ",\n")) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_codegen_java(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "// Generated by flags2env from .cli-flags.toml. Do not edit.\npackage generated;\n\nimport java.util.List;\nimport java.util.Map;\n\npublic record ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, "(\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "    ") ||
        !f2e_buffer_append(&source, f2e_codegen_java_type(flag->type, !flag->has_default)) ||
        !f2e_buffer_append_char(&source, ' ') ||
        !f2e_buffer_append(&source, flag->env) ||
        (i + 1 < config->flag_count && !f2e_buffer_append_char(&source, ',')) ||
        !f2e_buffer_append_char(&source, '\n')) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, ") {}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_codegen_csharp(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "// Generated by flags2env from .cli-flags.toml. Do not edit.\nusing System.Collections.Generic;\nusing System.Text.Json;\n\nnamespace Generated;\n\npublic sealed record ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, "(\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "    ") ||
        !f2e_buffer_append(&source, f2e_codegen_csharp_type(flag->type)) ||
        (!flag->has_default && !f2e_buffer_append_char(&source, '?')) ||
        !f2e_buffer_append_char(&source, ' ') ||
        !f2e_buffer_append(&source, flag->env) ||
        (i + 1 < config->flag_count && !f2e_buffer_append_char(&source, ',')) ||
        !f2e_buffer_append_char(&source, '\n')) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, ");\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static int f2e_codegen_dart_append_json_read(F2EBuffer *source, const F2EFlag *flag) {
  const int optional = !flag->has_default;
  if (!f2e_buffer_append(source, "      ") ||
      !f2e_buffer_append(source, flag->env) ||
      !f2e_buffer_append(source, ": ")) {
    return 0;
  }

  switch (flag->type) {
    case F2E_TYPE_BOOL:
      if (!f2e_buffer_append(source, "json['") ||
          !f2e_buffer_append(source, flag->env) ||
          !f2e_buffer_append(source, optional ? "'] as bool?" : "'] as bool")) {
        return 0;
      }
      break;
    case F2E_TYPE_INT:
      if (!f2e_buffer_append(source, "json['") ||
          !f2e_buffer_append(source, flag->env) ||
          !f2e_buffer_append(source, optional ? "'] as int?" : "'] as int")) {
        return 0;
      }
      break;
    case F2E_TYPE_FLOAT:
      if (optional) {
        if (!f2e_buffer_append(source, "json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] == null ? null : (json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] as num).toDouble()")) {
          return 0;
        }
      } else if (!f2e_buffer_append(source, "(json['") ||
                 !f2e_buffer_append(source, flag->env) ||
                 !f2e_buffer_append(source, "'] as num).toDouble()")) {
        return 0;
      }
      break;
    case F2E_TYPE_ARRAY:
      if (optional) {
        if (!f2e_buffer_append(source, "json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] == null ? null : (json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] as List<dynamic>).cast<Object?>()")) {
          return 0;
        }
      } else if (!f2e_buffer_append(source, "(json['") ||
                 !f2e_buffer_append(source, flag->env) ||
                 !f2e_buffer_append(source, "'] as List<dynamic>).cast<Object?>()")) {
        return 0;
      }
      break;
    case F2E_TYPE_MAP:
      if (optional) {
        if (!f2e_buffer_append(source, "json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] == null ? null : (json['") ||
            !f2e_buffer_append(source, flag->env) ||
            !f2e_buffer_append(source, "'] as Map<dynamic, dynamic>).cast<String, Object?>()")) {
          return 0;
        }
      } else if (!f2e_buffer_append(source, "(json['") ||
                 !f2e_buffer_append(source, flag->env) ||
                 !f2e_buffer_append(source, "'] as Map<dynamic, dynamic>).cast<String, Object?>()")) {
        return 0;
      }
      break;
    case F2E_TYPE_JSON:
      if (!f2e_buffer_append(source, "json['") ||
          !f2e_buffer_append(source, flag->env) ||
          !f2e_buffer_append(source, "']")) {
        return 0;
      }
      break;
    case F2E_TYPE_STRING:
    default:
      if (!f2e_buffer_append(source, "json['") ||
          !f2e_buffer_append(source, flag->env) ||
          !f2e_buffer_append(source, optional ? "'] as String?" : "'] as String")) {
        return 0;
      }
      break;
  }
  return f2e_buffer_append(source, ",\n");
}

static char *f2e_codegen_dart(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "// Generated by flags2env from .cli-flags.toml. Do not edit.\n\nfinal class ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, " {\n  const ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, "({\n")) {
    free(source.data);
    return NULL;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, flag->has_default ? "    required this." : "    this.") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, ",\n")) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "  });\n\n")) {
    free(source.data);
    return NULL;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "  final ") ||
        !f2e_buffer_append(&source, f2e_codegen_dart_type(flag->type, !flag->has_default)) ||
        !f2e_buffer_append_char(&source, ' ') ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, ";\n")) {
      free(source.data);
      return NULL;
    }
  }

  if (!f2e_buffer_append(&source, "\n  factory ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, ".fromJson(Map<String, dynamic> json) {\n    return ") ||
      !f2e_buffer_append(&source, type_name) ||
      !f2e_buffer_append(&source, "(\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    if (!f2e_codegen_dart_append_json_read(&source, &config->flags[i])) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "    );\n  }\n\n  Map<String, Object?> toJson() => <String, Object?>{\n")) {
    free(source.data);
    return NULL;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, "        '") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, "': ") ||
        !f2e_buffer_append(&source, flag->env) ||
        !f2e_buffer_append(&source, ",\n")) {
      free(source.data);
      return NULL;
    }
  }
  if (!f2e_buffer_append(&source, "      };\n}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static int f2e_codegen_append_default_json(F2EBuffer *buffer, const F2EFlag *flag) {
  if (!buffer || !flag || !flag->has_default) {
    return 0;
  }
  switch (flag->type) {
    case F2E_TYPE_STRING:
      return f2e_buffer_append_json_string(buffer, flag->default_value);
    case F2E_TYPE_BOOL: {
      const char *canonical = NULL;
      return f2e_bool_value_alias(flag, flag->default_value, &canonical) && f2e_buffer_append(buffer, canonical);
    }
    case F2E_TYPE_INT: {
      char canonical[64];
      snprintf(canonical, sizeof(canonical), "%lld", strtoll(flag->default_value, NULL, 10));
      return f2e_buffer_append(buffer, canonical);
    }
    case F2E_TYPE_FLOAT: {
      char canonical[128];
      snprintf(canonical, sizeof(canonical), "%.17g", strtod(flag->default_value, NULL));
      return f2e_buffer_append(buffer, canonical);
    }
    case F2E_TYPE_JSON:
    case F2E_TYPE_ARRAY:
    case F2E_TYPE_MAP:
      return f2e_buffer_append(buffer, flag->default_value);
    default:
      return 0;
  }
}

static int f2e_codegen_append_json_schema_type(F2EBuffer *source, F2EValueType type, int *wrote) {
  const char *schema = NULL;
  switch (type) {
    case F2E_TYPE_STRING:
      schema = "\"type\":\"string\"";
      break;
    case F2E_TYPE_BOOL:
      schema = "\"type\":\"boolean\"";
      break;
    case F2E_TYPE_INT:
      schema = "\"type\":\"integer\"";
      break;
    case F2E_TYPE_FLOAT:
      schema = "\"type\":\"number\"";
      break;
    case F2E_TYPE_ARRAY:
      schema = "\"type\":\"array\",\"items\":{}";
      break;
    case F2E_TYPE_MAP:
      schema = "\"type\":\"object\",\"additionalProperties\":true";
      break;
    case F2E_TYPE_JSON:
    default:
      break;
  }
  if (!schema) {
    return 1;
  }
  if ((*wrote && !f2e_buffer_append_char(source, ',')) || !f2e_buffer_append(source, schema)) {
    return 0;
  }
  *wrote = 1;
  return 1;
}

static char *f2e_codegen_json_schema(const F2EConfig *config, const char *type_name) {
  F2EBuffer source = {0};
  if (!f2e_buffer_init(&source) ||
      !f2e_buffer_append(&source, "{\n  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n  \"title\": ") ||
      !f2e_buffer_append_json_string(&source, type_name) ||
      !f2e_buffer_append(&source,
                         ",\n"
                         "  \"description\": \"Post-coercion flags2env config generated from .cli-flags.toml.\",\n"
                         "  \"x-flags2env-stage\": \"coerced\",\n"
                         "  \"x-flags2env-source\": \".cli-flags.toml\",\n"
                         "  \"type\": \"object\",\n"
                         "  \"additionalProperties\": false,\n"
                         "  \"properties\": {")) {
    free(source.data);
    return NULL;
  }

  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!f2e_buffer_append(&source, i == 0 ? "\n    " : ",\n    ") ||
        !f2e_buffer_append_json_string(&source, flag->env) ||
        !f2e_buffer_append(&source, ": {")) {
      free(source.data);
      return NULL;
    }
    int wrote = 0;
    if (!f2e_codegen_append_json_schema_type(&source, flag->type, &wrote)) {
      free(source.data);
      return NULL;
    }
    if (flag->help[0] != '\0') {
      if ((wrote && !f2e_buffer_append_char(&source, ',')) ||
          !f2e_buffer_append(&source, "\"description\":") ||
          !f2e_buffer_append_json_string(&source, flag->help)) {
        free(source.data);
        return NULL;
      }
      wrote = 1;
    }
    if (flag->has_default) {
      if ((wrote && !f2e_buffer_append_char(&source, ',')) ||
          !f2e_buffer_append(&source, "\"default\":") ||
          !f2e_codegen_append_default_json(&source, flag)) {
        free(source.data);
        return NULL;
      }
      wrote = 1;
    }
    if ((wrote && !f2e_buffer_append_char(&source, ',')) ||
        !f2e_buffer_append(&source, "\"x-flags2env-type\":") ||
        !f2e_buffer_append_json_string(&source, f2e_value_type_name(flag->type))) {
      free(source.data);
      return NULL;
    }
    wrote = 1;
    if (!f2e_buffer_append_char(&source, ',') ||
        !f2e_buffer_append(&source, "\"x-flags2env-flag\":") ||
        !f2e_buffer_append_json_string(&source, f2e_audit_flag_name(flag)) ||
        !f2e_buffer_append_char(&source, '}')) {
      free(source.data);
      return NULL;
    }
  }
  if (config->flag_count > 0 && !f2e_buffer_append_char(&source, '\n')) {
    free(source.data);
    return NULL;
  }
  if (!f2e_buffer_append(&source, "  },\n  \"required\": [")) {
    free(source.data);
    return NULL;
  }
  int wrote_required = 0;
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    if (!flag->has_default) {
      continue;
    }
    if ((wrote_required && !f2e_buffer_append(&source, ", ")) ||
        !f2e_buffer_append_json_string(&source, flag->env)) {
      free(source.data);
      return NULL;
    }
    wrote_required = 1;
  }
  if (!f2e_buffer_append(&source, "]\n}\n")) {
    free(source.data);
    return NULL;
  }
  return source.data;
}

static char *f2e_generate_types_from_config(const F2EConfig *config, const char *language, const char *type_name) {
  const char *name = type_name && type_name[0] ? type_name : "CliConfig";
  if (!config || !language || !f2e_codegen_identifier_is_valid(name)) {
    return NULL;
  }
  if (f2e_streq(language, "typescript") || f2e_streq(language, "ts")) {
    return f2e_codegen_typescript(config, name);
  }
  if (f2e_streq(language, "python") || f2e_streq(language, "py")) {
    return f2e_codegen_python(config, name);
  }
  if (f2e_streq(language, "go") || f2e_streq(language, "golang")) {
    return f2e_codegen_go(config, name);
  }
  if (f2e_streq(language, "rust") || f2e_streq(language, "rs")) {
    return f2e_codegen_rust(config, name);
  }
  if (f2e_streq(language, "java")) {
    return f2e_codegen_java(config, name);
  }
  if (f2e_streq(language, "csharp") || f2e_streq(language, "c#") ||
      f2e_streq(language, "cs") || f2e_streq(language, "dotnet")) {
    return f2e_codegen_csharp(config, name);
  }
  if (f2e_streq(language, "dart")) {
    return f2e_codegen_dart(config, name);
  }
  if (f2e_streq(language, "json-schema") || f2e_streq(language, "jsonschema") ||
      f2e_streq(language, "schema")) {
    return f2e_codegen_json_schema(config, name);
  }
  return NULL;
}

static int f2e_codegen_env_is_declared(const F2EConfig *config, const char *env) {
  for (size_t i = 0; i < config->flag_count; i++) {
    if (f2e_streq(config->flags[i].env, env)) {
      return 1;
    }
  }
  return 0;
}

/*
 * Generated types describe every env key the parser may emit, so command
 * marker envs and parse.command_env are appended as synthetic optional
 * fields (bool markers, string command path) before rendering.
 */
static void f2e_codegen_append_command_envs(F2EConfig *config) {
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->env[0] == '\0' || f2e_codegen_env_is_declared(config, command->env)) {
      continue;
    }
    F2EFlag *flag = f2e_add_flag(config, command->name);
    if (!flag) {
      return;
    }
    f2e_strlcpy(flag->env, command->env, sizeof(flag->env));
    flag->type = F2E_TYPE_BOOL;
    f2e_strlcpy(flag->help, command->help, sizeof(flag->help));
  }
  if (config->command_count > 0 && config->command_env[0] != '\0' &&
      !f2e_codegen_env_is_declared(config, config->command_env)) {
    F2EFlag *flag = f2e_add_flag(config, "command");
    if (flag) {
      f2e_strlcpy(flag->env, config->command_env, sizeof(flag->env));
      flag->type = F2E_TYPE_STRING;
      f2e_strlcpy(flag->help, "Space-joined subcommand path selected by argv.", sizeof(flag->help));
    }
  }
}

char *f2e_generate_types_from_file(const char *config_path, const char *language, const char *type_name) {
  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config) || f2e_config_has_audit_errors(config)) {
    free(config);
    return NULL;
  }
  f2e_codegen_append_command_envs(config);
  for (size_t i = 0; i < config->flag_count; i++) {
    /* a command-scoped default is only emitted when its command runs, so the
       generated field must be optional */
    if (config->flags[i].command != F2E_SCOPE_ROOT) {
      config->flags[i].has_default = 0;
    }
  }
  char *source = f2e_generate_types_from_config(config, language, type_name);
  free(config);
  return source;
}

char *f2e_generate_types(const char *language, const char *type_name) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *source = f2e_generate_types_from_file(path, language, type_name);
  free(path);
  return source;
}

typedef struct {
  char **items;
  size_t count;
} F2EHelpLines;

static size_t f2e_size_min(size_t a, size_t b) {
  return a < b ? a : b;
}

static size_t f2e_size_max(size_t a, size_t b) {
  return a > b ? a : b;
}

static int f2e_help_terminal_columns(void) {
  const char *env_columns = getenv("COLUMNS");
  if (env_columns && env_columns[0] != '\0') {
    char *end = NULL;
    errno = 0;
    long parsed = strtol(env_columns, &end, 10);
    if (errno == 0 && end && *end == '\0' && parsed > 0 && parsed <= 1000) {
      return (int)parsed;
    }
  }

#if defined(_WIN32)
  CONSOLE_SCREEN_BUFFER_INFO info;
  HANDLE out = GetStdHandle(STD_OUTPUT_HANDLE);
  if (out != INVALID_HANDLE_VALUE && GetConsoleScreenBufferInfo(out, &info)) {
    int columns = (int)(info.srWindow.Right - info.srWindow.Left + 1);
    if (columns > 0) {
      return columns;
    }
  }
#elif defined(TIOCGWINSZ) && (defined(__unix__) || defined(__APPLE__))
  struct winsize size;
  if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &size) == 0 && size.ws_col > 0) {
    return (int)size.ws_col;
  }
#endif

  return 80;
}

static size_t f2e_help_resolve_columns(int terminal_columns) {
  int columns = terminal_columns > 0 ? terminal_columns : f2e_help_terminal_columns();
  if (columns < 40) {
    columns = 40;
  }
  if (columns > 160) {
    columns = 160;
  }
  return (size_t)columns;
}

static int f2e_help_append_repeat(F2EBuffer *buffer, char ch, size_t count) {
  for (size_t i = 0; i < count; i++) {
    if (!f2e_buffer_append_char(buffer, ch)) {
      return 0;
    }
  }
  return 1;
}

static int f2e_help_append_padded(F2EBuffer *buffer, const char *value, size_t width) {
  size_t len = value ? strlen(value) : 0;
  size_t used = f2e_size_min(len, width);
  if (used > 0) {
    if (!f2e_buffer_reserve(buffer, used)) {
      return 0;
    }
    memcpy(buffer->data + buffer->len, value, used);
    buffer->len += used;
    buffer->data[buffer->len] = '\0';
  }
  return f2e_help_append_repeat(buffer, ' ', width - used);
}

static int f2e_help_lines_push(F2EHelpLines *lines, const char *value, size_t len) {
  char *copy = (char *)malloc(len + 1);
  if (!copy) {
    return 0;
  }
  for (size_t i = 0; i < len; i++) {
    unsigned char ch = value ? (unsigned char)value[i] : '\0';
    if (ch == '\t') {
      copy[i] = ' ';
    } else if (ch < 0x20 || ch == 0x7f) {
      copy[i] = '?';
    } else {
      copy[i] = (char)ch;
    }
  }
  copy[len] = '\0';

  char **grown = (char **)realloc(lines->items, sizeof(char *) * (lines->count + 1));
  if (!grown) {
    free(copy);
    return 0;
  }
  lines->items = grown;
  lines->items[lines->count++] = copy;
  return 1;
}

static void f2e_help_lines_free(F2EHelpLines *lines) {
  if (!lines) {
    return;
  }
  for (size_t i = 0; i < lines->count; i++) {
    free(lines->items[i]);
  }
  free(lines->items);
  lines->items = NULL;
  lines->count = 0;
}

static int f2e_help_wrap_lines(const char *value, size_t width, F2EHelpLines *out) {
  memset(out, 0, sizeof(*out));
  if (width == 0) {
    width = 1;
  }

  const char *cursor = value ? value : "";
  if (*cursor == '\0') {
    return f2e_help_lines_push(out, "", 0);
  }

  while (*cursor) {
    while (*cursor == ' ' || *cursor == '\t') {
      cursor++;
    }
    if (*cursor == '\n' || *cursor == '\r') {
      if (!f2e_help_lines_push(out, "", 0)) {
        f2e_help_lines_free(out);
        return 0;
      }
      while (*cursor == '\n' || *cursor == '\r') {
        cursor++;
      }
      continue;
    }
    if (*cursor == '\0') {
      break;
    }

    size_t available = 0;
    while (cursor[available] && cursor[available] != '\n' && cursor[available] != '\r') {
      available++;
    }

    size_t take = f2e_size_min(available, width);
    if (available > width) {
      size_t break_at = 0;
      for (size_t i = 1; i < width; i++) {
        if (isspace((unsigned char)cursor[i])) {
          break_at = i;
        }
      }
      if (break_at > 0) {
        take = break_at;
      }
    }

    if (!f2e_help_lines_push(out, cursor, take)) {
      f2e_help_lines_free(out);
      return 0;
    }
    cursor += take;
    while (*cursor == ' ' || *cursor == '\t') {
      cursor++;
    }
    if (*cursor == '\n' || *cursor == '\r') {
      while (*cursor == '\n' || *cursor == '\r') {
        cursor++;
      }
    }
  }

  if (out->count == 0) {
    return f2e_help_lines_push(out, "", 0);
  }
  return 1;
}

static int f2e_help_append_border(F2EBuffer *buffer, const size_t *widths, size_t count) {
  if (!f2e_buffer_append_char(buffer, '+')) {
    return 0;
  }
  for (size_t i = 0; i < count; i++) {
    if (!f2e_help_append_repeat(buffer, '-', widths[i] + 2) ||
        !f2e_buffer_append_char(buffer, '+')) {
      return 0;
    }
  }
  return f2e_buffer_append_char(buffer, '\n');
}

static size_t f2e_help_table_width(const size_t *widths, size_t count) {
  size_t width = 1;
  for (size_t i = 0; i < count; i++) {
    width += widths[i] + 3;
  }
  return width;
}

static int f2e_help_append_spanning_row(F2EBuffer *buffer, const char *value, size_t table_width) {
  size_t inner_width = table_width > 4 ? table_width - 4 : 1;
  F2EHelpLines lines;
  if (!f2e_help_wrap_lines(value, inner_width, &lines)) {
    return 0;
  }
  int ok = 1;
  for (size_t i = 0; i < lines.count; i++) {
    if (!f2e_buffer_append(buffer, "| ") ||
        !f2e_help_append_padded(buffer, lines.items[i], inner_width) ||
        !f2e_buffer_append(buffer, " |\n")) {
      ok = 0;
      break;
    }
  }
  f2e_help_lines_free(&lines);
  return ok;
}

static int f2e_help_append_row(F2EBuffer *buffer, const char *const *cells, const size_t *widths, size_t count) {
  F2EHelpLines wrapped[5];
  if (count > 5) {
    return 0;
  }
  memset(wrapped, 0, sizeof(wrapped));

  size_t max_lines = 1;
  for (size_t i = 0; i < count; i++) {
    if (!f2e_help_wrap_lines(cells[i] ? cells[i] : "", widths[i], &wrapped[i])) {
      for (size_t j = 0; j <= i && j < count; j++) {
        f2e_help_lines_free(&wrapped[j]);
      }
      return 0;
    }
    max_lines = f2e_size_max(max_lines, wrapped[i].count);
  }

  int ok = 1;
  for (size_t line = 0; line < max_lines; line++) {
    if (!f2e_buffer_append_char(buffer, '|')) {
      ok = 0;
      break;
    }
    for (size_t col = 0; col < count; col++) {
      const char *cell = line < wrapped[col].count ? wrapped[col].items[line] : "";
      if (!f2e_buffer_append_char(buffer, ' ') ||
          !f2e_help_append_padded(buffer, cell, widths[col]) ||
          !f2e_buffer_append(buffer, " |")) {
        ok = 0;
        break;
      }
    }
    if (!ok || !f2e_buffer_append_char(buffer, '\n')) {
      ok = 0;
      break;
    }
  }

  for (size_t i = 0; i < count; i++) {
    f2e_help_lines_free(&wrapped[i]);
  }
  return ok;
}

static char *f2e_help_flag_names(const F2EFlag *flag, int show_short) {
  F2EBuffer names = {0};
  if (!f2e_buffer_init(&names)) {
    return NULL;
  }

  if (flag->short_name != '\0' && show_short) {
    char short_name[3] = {'-', flag->short_name, '\0'};
    if (!f2e_buffer_append(&names, short_name)) {
      free(names.data);
      return NULL;
    }
  }

  for (size_t i = 0; i < flag->alias_count; i++) {
    if (names.len > 0 && !f2e_buffer_append(&names, ", ")) {
      free(names.data);
      return NULL;
    }
    if (!f2e_buffer_append(&names, "--") || !f2e_buffer_append(&names, flag->aliases[i])) {
      free(names.data);
      return NULL;
    }
  }

  if (names.len == 0 && flag->name[0] != '\0') {
    if (!f2e_buffer_append(&names, "--") || !f2e_buffer_append(&names, flag->name)) {
      free(names.data);
      return NULL;
    }
  }
  return names.data;
}

static int f2e_help_append_bool_values(F2EBuffer *buffer, const F2EFlag *flag) {
  if (!f2e_buffer_append(buffer, "true, false")) {
    return 0;
  }
  for (size_t i = 0; i < flag->true_alias_count; i++) {
    if (!f2e_buffer_append(buffer, ", ") || !f2e_buffer_append(buffer, flag->true_aliases[i])) {
      return 0;
    }
  }
  for (size_t i = 0; i < flag->false_alias_count; i++) {
    if (!f2e_buffer_append(buffer, ", ") || !f2e_buffer_append(buffer, flag->false_aliases[i])) {
      return 0;
    }
  }
  return 1;
}

static char *f2e_help_flag_description(const F2EFlag *flag) {
  F2EBuffer description = {0};
  if (!f2e_buffer_init(&description)) {
    return NULL;
  }

  if (flag->help[0] != '\0' && !f2e_buffer_append(&description, flag->help)) {
    free(description.data);
    return NULL;
  }

  if (flag->type == F2E_TYPE_BOOL) {
    if (description.len > 0 && !f2e_buffer_append_char(&description, ' ')) {
      free(description.data);
      return NULL;
    }
    if (!f2e_buffer_append(&description, "Values: ") ||
        !f2e_help_append_bool_values(&description, flag) ||
        !f2e_buffer_append_char(&description, '.')) {
      free(description.data);
      return NULL;
    }
    if (flag->alias_count > 0) {
      if (!f2e_buffer_append(&description, " Negate with --no-") ||
          !f2e_buffer_append(&description, flag->aliases[0]) ||
          !f2e_buffer_append_char(&description, '.')) {
        free(description.data);
        return NULL;
      }
    }
  }

  if (description.len == 0 && !f2e_buffer_append_char(&description, '-')) {
    free(description.data);
    return NULL;
  }
  return description.data;
}

static int f2e_help_details_sep(F2EBuffer *details, int *wrote) {
  if (*wrote && !f2e_buffer_append(details, "; ")) {
    return 0;
  }
  *wrote = 1;
  return 1;
}

static char *f2e_help_flag_details_for_columns(const F2EFlag *flag, unsigned columns) {
  F2EBuffer details = {0};
  if (!f2e_buffer_init(&details)) {
    return NULL;
  }

  int wrote = 0;
  if (columns & F2E_HELP_COL_ENV) {
    if (!f2e_help_details_sep(&details, &wrote) ||
        !f2e_buffer_append(&details, "env=") ||
        !f2e_buffer_append(&details, flag->env[0] != '\0' ? flag->env : "-")) {
      free(details.data);
      return NULL;
    }
  }

  if (columns & F2E_HELP_COL_TYPE) {
    if (!f2e_help_details_sep(&details, &wrote) ||
        !f2e_buffer_append(&details, "type=") ||
        !f2e_buffer_append(&details, f2e_value_type_name(flag->type))) {
      free(details.data);
      return NULL;
    }
  }

  if ((columns & F2E_HELP_COL_DEFAULT) && flag->has_default) {
    if (!f2e_help_details_sep(&details, &wrote) ||
        !f2e_buffer_append(&details, "default=") ||
        !f2e_buffer_append(&details, flag->default_value)) {
      free(details.data);
      return NULL;
    }
  }
  if ((columns & F2E_HELP_COL_DESCRIPTION) && flag->help[0] != '\0') {
    if (!f2e_help_details_sep(&details, &wrote) ||
        !f2e_buffer_append(&details, flag->help)) {
      free(details.data);
      return NULL;
    }
  }
  if ((columns & F2E_HELP_COL_DESCRIPTION) && flag->type == F2E_TYPE_BOOL) {
    if (!f2e_help_details_sep(&details, &wrote) ||
        !f2e_buffer_append(&details, "values=") ||
        !f2e_help_append_bool_values(&details, flag)) {
      free(details.data);
      return NULL;
    }
    if (flag->alias_count > 0) {
      if (!f2e_help_details_sep(&details, &wrote) ||
          !f2e_buffer_append(&details, "negates=--no-") ||
          !f2e_buffer_append(&details, flag->aliases[0])) {
        free(details.data);
        return NULL;
      }
    }
  }
  if (!wrote && !f2e_buffer_append_char(&details, '-')) {
    free(details.data);
    return NULL;
  }
  return details.data;
}

static void f2e_help_wide_widths(size_t terminal_columns, size_t widths[5]) {
  size_t columns = terminal_columns >= 110 ? terminal_columns : 110;
  size_t inner = columns - 16;
  widths[0] = columns >= 132 ? 32 : 27;
  widths[1] = columns >= 132 ? 20 : 16;
  widths[2] = columns >= 132 ? 10 : 9;
  widths[3] = columns >= 132 ? 14 : 12;
  size_t used = widths[0] + widths[1] + widths[2] + widths[3];
  widths[4] = inner > used ? inner - used : 24;
}

static void f2e_help_narrow_widths(size_t terminal_columns, size_t widths[2]) {
  size_t columns = terminal_columns >= 40 ? terminal_columns : 40;
  size_t inner = columns - 7;
  widths[0] = inner >= 64 ? 28 : inner >= 50 ? 22 : inner / 2;
  if (widths[0] < 14) {
    widths[0] = 14;
  }
  if (widths[0] + 16 > inner) {
    widths[0] = inner > 24 ? inner - 20 : inner / 2;
  }
  widths[1] = inner - widths[0];
}

static int f2e_help_command_name(const char *command_name, char *out, size_t out_size) {
  if (f2e_path_basename_copy(command_name, out, out_size)) {
    return 1;
  }
  return f2e_path_basename_copy("flags2env", out, out_size);
}

static unsigned f2e_help_selected_columns(const F2EConfig *config) {
  unsigned columns = config && config->help_columns_configured ? config->help_columns : F2E_HELP_COL_DEFAULTS;
  if (config) {
    columns &= ~config->help_exclude_columns;
  }
  columns &= F2E_HELP_COL_DEFAULTS;
  columns |= F2E_HELP_COL_OPTIONS;
  return columns;
}

static size_t f2e_help_collect_columns(unsigned columns, unsigned out[5]) {
  size_t count = 0;
  if (columns & F2E_HELP_COL_OPTIONS) {
    out[count++] = F2E_HELP_COL_OPTIONS;
  }
  if (columns & F2E_HELP_COL_ENV) {
    out[count++] = F2E_HELP_COL_ENV;
  }
  if (columns & F2E_HELP_COL_TYPE) {
    out[count++] = F2E_HELP_COL_TYPE;
  }
  if (columns & F2E_HELP_COL_DEFAULT) {
    out[count++] = F2E_HELP_COL_DEFAULT;
  }
  if (columns & F2E_HELP_COL_DESCRIPTION) {
    out[count++] = F2E_HELP_COL_DESCRIPTION;
  }
  return count;
}

static const char *f2e_help_column_header(unsigned column) {
  switch (column) {
    case F2E_HELP_COL_ENV:
      return "Env";
    case F2E_HELP_COL_TYPE:
      return "Type";
    case F2E_HELP_COL_DEFAULT:
      return "Default";
    case F2E_HELP_COL_DESCRIPTION:
      return "Description";
    case F2E_HELP_COL_OPTIONS:
    default:
      return "Option(s)";
  }
}

static int f2e_help_uses_default_wide_columns(const unsigned *columns, size_t column_count) {
  return column_count == 5 &&
         columns[0] == F2E_HELP_COL_OPTIONS &&
         columns[1] == F2E_HELP_COL_ENV &&
         columns[2] == F2E_HELP_COL_TYPE &&
         columns[3] == F2E_HELP_COL_DEFAULT &&
         columns[4] == F2E_HELP_COL_DESCRIPTION;
}

static size_t f2e_help_min_width_for_column(unsigned column) {
  switch (column) {
    case F2E_HELP_COL_ENV:
      return 10;
    case F2E_HELP_COL_TYPE:
      return 8;
    case F2E_HELP_COL_DEFAULT:
      return 10;
    case F2E_HELP_COL_DESCRIPTION:
      return 18;
    case F2E_HELP_COL_OPTIONS:
    default:
      return 18;
  }
}

static void f2e_help_custom_wide_widths(size_t terminal_columns,
                                        const unsigned *columns,
                                        size_t column_count,
                                        size_t widths[5]) {
  if (f2e_help_uses_default_wide_columns(columns, column_count)) {
    f2e_help_wide_widths(terminal_columns, widths);
    return;
  }

  size_t table_columns = terminal_columns >= 40 ? terminal_columns : 40;
  size_t separators = column_count * 3 + 1;
  size_t inner = table_columns > separators ? table_columns - separators : column_count * 12;
  size_t used = 0;
  size_t flexible_index = column_count > 0 ? column_count - 1 : 0;

  for (size_t i = 0; i < column_count; i++) {
    widths[i] = f2e_help_min_width_for_column(columns[i]);
    used += widths[i];
    if (columns[i] == F2E_HELP_COL_DESCRIPTION) {
      flexible_index = i;
    }
  }

  if (used < inner && column_count > 0) {
    widths[flexible_index] += inner - used;
  }
}

static const char *f2e_help_column_value(unsigned column,
                                         const F2EFlag *flag,
                                         char *names,
                                         char **description_out) {
  switch (column) {
    case F2E_HELP_COL_ENV:
      return flag->env[0] != '\0' ? flag->env : "-";
    case F2E_HELP_COL_TYPE:
      return f2e_value_type_name(flag->type);
    case F2E_HELP_COL_DEFAULT:
      return flag->has_default ? flag->default_value : "-";
    case F2E_HELP_COL_DESCRIPTION:
      if (!*description_out) {
        *description_out = f2e_help_flag_description(flag);
      }
      return *description_out;
    case F2E_HELP_COL_OPTIONS:
    default:
      return names;
  }
}

/*
 * Collects the flags reachable from a command scope for the help table:
 * the scope's own flags first, then ancestor/global flags that are not
 * shadowed by a nearer definition.
 */
static size_t f2e_help_collect_scope_flags(const F2EConfig *config, int scope, size_t out[F2E_MAX_FLAGS]) {
  size_t count = 0;
  int level = scope;
  for (;;) {
    for (size_t i = 0; i < config->flag_count && count < F2E_MAX_FLAGS; i++) {
      const F2EFlag *flag = &config->flags[i];
      if (flag->command != level) {
        continue;
      }
      int reachable = level == scope;
      for (size_t j = 0; !reachable && j < flag->alias_count; j++) {
        reachable = f2e_find_flag_by_alias_const(config, scope, flag->aliases[j]) == flag;
      }
      if (!reachable && flag->short_name != '\0') {
        reachable = f2e_find_flag_by_short((F2EConfig *)config, scope, flag->short_name) == flag;
      }
      if (reachable) {
        out[count++] = i;
      }
    }
    if (level < 0) {
      break;
    }
    level = f2e_scope_parent(config, level);
  }
  return count;
}

/*
 * Renders a command's name cell relative to the help scope, so nested
 * commands read as their invocation path (e.g. "remote add"), with any
 * aliases appended ("commit, ci").
 */
static char *f2e_help_command_row_name(const F2EConfig *config, int index, int scope) {
  F2EBuffer names = {0};
  if (!f2e_buffer_init(&names)) {
    return NULL;
  }
  int chain[F2E_MAX_COMMAND_DEPTH];
  size_t depth = 0;
  for (int cursor = index; cursor >= 0 && cursor != scope && depth < F2E_MAX_COMMAND_DEPTH;
       cursor = config->commands[cursor].parent) {
    chain[depth++] = cursor;
  }
  for (size_t i = depth; i > 0; i--) {
    if ((i < depth && !f2e_buffer_append_char(&names, ' ')) ||
        !f2e_buffer_append(&names, config->commands[chain[i - 1]].name)) {
      free(names.data);
      return NULL;
    }
  }
  const F2ECommand *command = &config->commands[index];
  for (size_t i = 0; i < command->alias_count; i++) {
    if (!f2e_buffer_append(&names, ", ") || !f2e_buffer_append(&names, command->aliases[i])) {
      free(names.data);
      return NULL;
    }
  }
  return names.data;
}

static size_t f2e_help_commands_name_width(const F2EConfig *config, int help_scope, int parent, size_t depth) {
  size_t width = 0;
  if (depth >= F2E_MAX_COMMAND_DEPTH) {
    return width;
  }
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->parent != parent) {
      continue;
    }
    char *names = f2e_help_command_row_name(config, (int)i, help_scope);
    if (names) {
      width = f2e_size_max(width, strlen(names));
      free(names);
    }
    width = f2e_size_max(width, f2e_help_commands_name_width(config, help_scope, (int)i, depth + 1));
  }
  return width;
}

static int f2e_help_append_command_rows(const F2EConfig *config,
                                        int help_scope,
                                        int parent,
                                        size_t depth,
                                        F2EBuffer *table,
                                        const size_t *widths) {
  if (depth >= F2E_MAX_COMMAND_DEPTH) {
    return 1;
  }
  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECommand *command = &config->commands[i];
    if (command->parent != parent) {
      continue;
    }
    char *names = f2e_help_command_row_name(config, (int)i, help_scope);
    if (!names) {
      return 0;
    }
    const char *row[] = {names, command->help[0] != '\0' ? command->help : "-"};
    int ok = f2e_help_append_row(table, row, widths, 2);
    free(names);
    if (!ok || !f2e_help_append_command_rows(config, help_scope, (int)i, depth + 1, table, widths)) {
      return 0;
    }
  }
  return 1;
}

static char *f2e_help_table_scoped(const F2EConfig *config, const char *command_name, int terminal_columns, int scope) {
  if (!config) {
    return NULL;
  }

  size_t columns = f2e_help_resolve_columns(terminal_columns);
  int wide = columns >= 110;
  unsigned selected_columns = f2e_help_selected_columns(config);
  unsigned wide_columns[5] = {0};
  size_t widths[5];
  size_t column_count = 0;
  if (wide) {
    column_count = f2e_help_collect_columns(selected_columns, wide_columns);
    f2e_help_custom_wide_widths(columns, wide_columns, column_count, widths);
  } else if ((selected_columns & ~F2E_HELP_COL_OPTIONS) == 0) {
    widths[0] = columns > 4 ? columns - 4 : 36;
    column_count = 1;
  } else {
    f2e_help_narrow_widths(columns, widths);
    column_count = 2;
  }

  F2EBuffer table = {0};
  if (!f2e_buffer_init(&table)) {
    return NULL;
  }

  char command[F2E_MAX_NAME];
  if (!f2e_help_command_name(command_name, command, sizeof(command))) {
    free(table.data);
    return NULL;
  }

  char path_label[F2E_MAX_VALUE];
  path_label[0] = '\0';
  if (scope >= 0 && !f2e_command_path_label(config, scope, path_label, sizeof(path_label))) {
    path_label[0] = '\0';
  }
  int has_children = f2e_command_has_children(config, scope);

  size_t table_width = f2e_help_table_width(widths, column_count);
  F2EBuffer title = {0};
  int title_ok = f2e_buffer_init(&title) &&
                 f2e_buffer_append(&title, "Command: ") &&
                 f2e_buffer_append(&title, command) &&
                 (path_label[0] == '\0' ||
                  (f2e_buffer_append_char(&title, ' ') &&
                   f2e_buffer_append(&title, path_label))) &&
                 (!has_children || f2e_buffer_append(&title, " [COMMAND]")) &&
                 f2e_buffer_append(&title, " [OPTIONS]");
  if (!title_ok ||
      !f2e_help_append_border(&table, widths, column_count) ||
      !f2e_help_append_spanning_row(&table, title.data, table_width) ||
      !f2e_help_append_border(&table, widths, column_count)) {
    free(title.data);
    free(table.data);
    return NULL;
  }
  free(title.data);

  if (scope >= 0 && (size_t)scope < config->command_count && config->commands[scope].help[0] != '\0') {
    if (!f2e_help_append_spanning_row(&table, config->commands[scope].help, table_width) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  }

  size_t scope_flags[F2E_MAX_FLAGS];
  size_t scope_flag_count = f2e_help_collect_scope_flags(config, scope, scope_flags);

  if (scope_flag_count == 0) {
    /* no reachable flags at this scope; skip the options table entirely */
  } else if (wide) {
    const char *header[5] = {0};
    for (size_t i = 0; i < column_count; i++) {
      header[i] = f2e_help_column_header(wide_columns[i]);
    }
    if (!f2e_help_append_row(&table, header, widths, column_count) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  } else if (column_count == 1) {
    const char *header[] = {"Option(s)"};
    if (!f2e_help_append_row(&table, header, widths, column_count) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  } else {
    const char *header[] = {"Option(s)", "Details"};
    if (!f2e_help_append_row(&table, header, widths, column_count) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  }

  for (size_t k = 0; k < scope_flag_count; k++) {
    const F2EFlag *flag = &config->flags[scope_flags[k]];
    /* hide a short flag that a nearer scope reuses for a different option */
    int show_short = flag->short_name == '\0' ||
                     f2e_find_flag_by_short((F2EConfig *)config, scope, flag->short_name) == flag;
    char *names = f2e_help_flag_names(flag, show_short);
    if (!names) {
      free(table.data);
      return NULL;
    }

    int ok = 0;
    if (wide) {
      const char *row[5] = {0};
      char *description = NULL;
      for (size_t j = 0; j < column_count; j++) {
        row[j] = f2e_help_column_value(wide_columns[j], flag, names, &description);
      }
      if (!row[column_count - 1]) {
        free(description);
        free(names);
        free(table.data);
        return NULL;
      }
      ok = f2e_help_append_row(&table, row, widths, column_count);
      free(description);
    } else if (column_count == 1) {
      const char *row[] = {names};
      ok = f2e_help_append_row(&table, row, widths, column_count);
    } else {
      char *details = f2e_help_flag_details_for_columns(flag, selected_columns & ~F2E_HELP_COL_OPTIONS);
      if (!details) {
        free(names);
        free(table.data);
        return NULL;
      }
      const char *row[] = {names, details};
      ok = f2e_help_append_row(&table, row, widths, column_count);
      free(details);
    }
    free(names);

    if (!ok || !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  }

  if (has_children) {
    size_t name_width = f2e_help_commands_name_width(config, scope, scope, 0);
    name_width = f2e_size_max(name_width, strlen("Command"));
    size_t max_name_width = table_width > 7 + 18 ? table_width - 7 - 18 : name_width;
    name_width = f2e_size_min(name_width, f2e_size_min(max_name_width, 40));
    size_t command_widths[2] = {name_width, table_width - name_width - 7};
    const char *command_header[] = {"Command", "Description"};
    if (!f2e_help_append_spanning_row(&table, "Commands:", table_width) ||
        !f2e_help_append_border(&table, command_widths, 2) ||
        !f2e_help_append_row(&table, command_header, command_widths, 2) ||
        !f2e_help_append_border(&table, command_widths, 2) ||
        !f2e_help_append_command_rows(config, scope, scope, 0, &table, command_widths) ||
        !f2e_help_append_border(&table, command_widths, 2)) {
      free(table.data);
      return NULL;
    }
    F2EBuffer hint = {0};
    int hint_ok = f2e_buffer_init(&hint) &&
                  f2e_buffer_append(&hint, "Run '") &&
                  f2e_buffer_append(&hint, command) &&
                  (path_label[0] == '\0' ||
                   (f2e_buffer_append_char(&hint, ' ') &&
                    f2e_buffer_append(&hint, path_label))) &&
                  f2e_buffer_append(&hint, " <command> --help' for command-specific options.");
    if (!hint_ok ||
        !f2e_help_append_spanning_row(&table, hint.data, table_width) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(hint.data);
      free(table.data);
      return NULL;
    }
    free(hint.data);
  }

  if (config->help_url[0] != '\0') {
    char help_url[F2E_MAX_VALUE + 16];
    snprintf(help_url, sizeof(help_url), "More help: %s", config->help_url);
    if (!f2e_help_append_spanning_row(&table, help_url, table_width) ||
        !f2e_help_append_border(&table, widths, column_count)) {
      free(table.data);
      return NULL;
    }
  }

  return table.data;
}

static char *f2e_help_table_from_config(const F2EConfig *config, const char *command_name, int terminal_columns) {
  return f2e_help_table_scoped(config, command_name, terminal_columns, F2E_SCOPE_ROOT);
}

static int f2e_print_stream_locked(FILE *stream, const char *value) {
  if (!stream || !value) {
    return 0;
  }
#if !defined(_WIN32)
  flockfile(stream);
#endif
  int ok = fputs(value, stream) != EOF;
  if (fflush(stream) == EOF) {
    ok = 0;
  }
#if !defined(_WIN32)
  funlockfile(stream);
#endif
  return ok;
}

int f2e_is_help_requested(int argc, const char *const argv[]) {
  if (argc < 0 || !argv) {
    return 0;
  }
  for (int i = 0; i < argc; i++) {
    if (argv[i] && f2e_streq(argv[i], "--")) {
      return 0;
    }
    if (argv[i] && f2e_streq(argv[i], "--help")) {
      return 1;
    }
  }
  return 0;
}

char *f2e_help_table_from_file(const char *config_path, const char *command_name, int terminal_columns) {
  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return NULL;
  }
  char *table = f2e_help_table_from_config(config, command_name, terminal_columns);
  free(config);
  return table;
}

char *f2e_help_table(const char *command_name, int terminal_columns) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *table = f2e_help_table_from_file(path, command_name, terminal_columns);
  free(path);
  return table;
}

static int f2e_help_scope_from_argv(F2EConfig *config, int argc, const char *const argv[]) {
  if (argc < 0 || !argv || config->command_count == 0) {
    return F2E_SCOPE_ROOT;
  }
  F2ECommandPath path;
  memset(&path, 0, sizeof(path));
  f2e_resolve_command_path(config, argc, argv, &path);
  return path.depth > 0 ? path.commands[path.depth - 1] : F2E_SCOPE_ROOT;
}

char *f2e_help_table_for_argv_from_file(const char *config_path,
                                        const char *command_name,
                                        int argc,
                                        const char *const argv[],
                                        int terminal_columns) {
  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return NULL;
  }
  int scope = f2e_help_scope_from_argv(config, argc, argv);
  char *table = f2e_help_table_scoped(config, command_name, terminal_columns, scope);
  free(config);
  return table;
}

char *f2e_help_table_for_argv(const char *command_name, int argc, const char *const argv[], int terminal_columns) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *table = f2e_help_table_for_argv_from_file(path, command_name, argc, argv, terminal_columns);
  free(path);
  return table;
}

int f2e_print_table_for_argv_from_file(const char *config_path,
                                       const char *command_name,
                                       int argc,
                                       const char *const argv[],
                                       int terminal_columns) {
  char *table = f2e_help_table_for_argv_from_file(config_path, command_name, argc, argv, terminal_columns);
  if (!table) {
    return 1;
  }
  int ok = f2e_print_stream_locked(stdout, table);
  f2e_free(table);
  return ok ? 0 : 1;
}

int f2e_print_table_for_argv(const char *command_name, int argc, const char *const argv[], int terminal_columns) {
  char *table = f2e_help_table_for_argv(command_name, argc, argv, terminal_columns);
  if (!table) {
    return 1;
  }
  int ok = f2e_print_stream_locked(stdout, table);
  f2e_free(table);
  return ok ? 0 : 1;
}

int f2e_print_table_from_file(const char *config_path, const char *command_name, int terminal_columns) {
  char *table = f2e_help_table_from_file(config_path, command_name, terminal_columns);
  if (!table) {
    return 1;
  }
  int ok = f2e_print_stream_locked(stdout, table);
  f2e_free(table);
  return ok ? 0 : 1;
}

int f2e_print_table(const char *command_name, int terminal_columns) {
  char *table = f2e_help_table(command_name, terminal_columns);
  if (!table) {
    return 1;
  }
  int ok = f2e_print_stream_locked(stdout, table);
  f2e_free(table);
  return ok ? 0 : 1;
}

/*
 * Walks argv once: matches subcommand tokens, resolves flags in the active
 * command scope, and (when pairs is non-NULL) records the parsed values.
 * A NULL pairs pointer makes this a dry run used to discover the command
 * path before defaults are applied; both passes consume tokens identically.
 *
 * Command matching skips leading positionals (program or wrapper names) until
 * the first token matches a top-level command; after that, each positional
 * must match a subcommand of the current scope or matching stops for good.
 */
static void f2e_scan_argv(F2EConfig *config,
                          F2EPair *pairs,
                          size_t pair_count,
                          int argc,
                          const char *const argv[],
                          F2EJsonList *positionals,
                          F2EJsonList *unknown_options,
                          F2EJsonList *errors,
                          F2EJsonList *extras,
                          int extras_after_match,
                          int allow_unknown,
                          int allow_unknown_forced,
                          int lenient,
                          F2ECommandPath *path_out) {
  int scope = lenient ? F2E_SCOPE_LENIENT : F2E_SCOPE_ROOT;
  /* lenient mode keeps the command-mode positional handling (leading tokens
     are skipped, never triggering stop_at_first_positional) but resolves no
     commands, mirroring the dry-run pass that found none */
  int matching = config->command_count > 0;
  int matched_any = 0;

  for (int i = 0; i < argc; i++) {
    const char *token = argv[i];
    if (!token || token[0] != '-' || token[1] == '\0') {
      if (matching && token && token[0] != '\0') {
        int next = lenient ? -1 : f2e_find_command_by_token(config, scope, token);
        if (next >= 0) {
          scope = next;
          matched_any = 1;
          if (!allow_unknown_forced && config->commands[next].allow_unknown_set) {
            allow_unknown = config->commands[next].allow_unknown;
          }
          if (path_out && path_out->depth < F2E_MAX_COMMAND_DEPTH) {
            path_out->commands[path_out->depth++] = next;
          }
          continue;
        }
        if (!matched_any) {
          if (positionals) {
            f2e_json_list_append(positionals, token);
          }
          if (extras && !extras_after_match && i > 0) {
            f2e_json_list_append(extras, token);
          }
          continue;
        }
        matching = 0;
      }
      if (extras) {
        if (config->stop_at_first_positional) {
          for (int j = i; j < argc; j++) {
            if (extras_after_match ? matched_any : j > 0) {
              f2e_json_list_append(extras, argv[j]);
            }
          }
        } else if (extras_after_match ? matched_any : i > 0) {
          f2e_json_list_append(extras, token);
        }
      }
      if (positionals) {
        if (config->stop_at_first_positional) {
          for (int j = i; j < argc; j++) {
            f2e_json_list_append(positionals, argv[j]);
          }
          break;
        }
        f2e_json_list_append(positionals, token);
      } else if (config->stop_at_first_positional) {
        break;
      }
      continue;
    }
    if (strcmp(token, "--") == 0) {
      if (extras && (extras_after_match ? matched_any : 1)) {
        for (int j = i + 1; j < argc; j++) {
          f2e_json_list_append(extras, argv[j]);
        }
      }
      if (positionals) {
        for (int j = i + 1; j < argc; j++) {
          f2e_json_list_append(positionals, argv[j]);
        }
      }
      break;
    }
    if (!f2e_token_looks_like_known_option(config, scope, token)) {
      int parsed_allow_unknown = 0;
      if (f2e_token_sets_allow_unknown(token, &parsed_allow_unknown)) {
        allow_unknown = parsed_allow_unknown;
        allow_unknown_forced = 1;
      } else if (!allow_unknown && unknown_options) {
        f2e_json_list_append(unknown_options, token);
      }
      continue;
    }
    if (token[1] == '-') {
      f2e_apply_long_arg(config, scope, pairs, pair_count, token, &i, argc, argv, errors);
    } else {
      f2e_apply_short_arg(config, scope, pairs, pair_count, token, &i, argc, argv, errors);
    }
  }
}

/* Dry-run scan that only resolves the command path selected by argv. */
static void f2e_resolve_command_path(F2EConfig *config, int argc, const char *const argv[], F2ECommandPath *path_out) {
  f2e_scan_argv(config, NULL, 0, argc, argv, NULL, NULL, NULL, NULL, 0, 1, 1, 0, path_out);
}

char *f2e_parse_from_file(const char *config_path, int argc, const char *const argv[]) {
  if (argc < 0 || !argv) {
    argc = 0;
  }

  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return f2e_empty_json_object();
  }

  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return f2e_empty_json_object();
  }

  F2EPair *pairs = (F2EPair *)calloc(F2E_MAX_PAIRS, sizeof(F2EPair));
  if (!pairs) {
    free(config);
    return f2e_empty_json_object();
  }

  F2EJsonList positionals = {0};
  F2EJsonList unknown_options = {0};
  F2EJsonList errors = {0};
  int track_positionals = config->positionals_env[0] != '\0' && f2e_json_list_init(&positionals);
  int track_unknown_options = config->unknown_options_env[0] != '\0' && f2e_json_list_init(&unknown_options);
  int track_errors = config->errors_env[0] != '\0' && f2e_json_list_init(&errors);
  int allow_unknown_forced = 0;
  int allow_unknown = f2e_resolve_allow_unknown(config, argc, argv, &allow_unknown_forced);

  int lenient = 0;
  if (config->command_count > 0) {
    F2ECommandPath path;
    memset(&path, 0, sizeof(path));
    f2e_resolve_command_path(config, argc, argv, &path);
    /* a wrapper script may have consumed the subcommand before argv reached
       this parser; when nothing matched, fall back to lenient global
       resolution instead of treating scoped flags as unknown */
    lenient = path.depth == 0;
    char joined[F2E_MAX_VALUE];
    int tail = path.depth > 0 ? path.commands[path.depth - 1] : F2E_SCOPE_ROOT;
    if (f2e_command_path_label(config, tail, joined, sizeof(joined))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->command_env, joined);
    }
    for (size_t i = 0; i < path.depth; i++) {
      const F2ECommand *command = &config->commands[path.commands[i]];
      if (command->env[0] != '\0') {
        f2e_set_pair(pairs, F2E_MAX_PAIRS, command->env, "true");
      }
    }
    f2e_apply_defaults_for_path(config, pairs, F2E_MAX_PAIRS, &path);
  } else {
    f2e_apply_defaults(config, pairs, F2E_MAX_PAIRS);
  }

  f2e_scan_argv(config,
                pairs,
                F2E_MAX_PAIRS,
                argc,
                argv,
                track_positionals ? &positionals : NULL,
                track_unknown_options ? &unknown_options : NULL,
                track_errors ? &errors : NULL,
                NULL,
                0,
                allow_unknown,
                allow_unknown_forced,
                lenient,
                NULL);

  if (track_positionals && positionals.count > 0) {
    char value[F2E_MAX_VALUE];
    if (f2e_json_list_finish(&positionals, value, sizeof(value))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->positionals_env, value);
    }
  }
  if (track_unknown_options && unknown_options.count > 0) {
    char value[F2E_MAX_VALUE];
    if (f2e_json_list_finish(&unknown_options, value, sizeof(value))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->unknown_options_env, value);
    }
  }
  if (track_errors && errors.count > 0) {
    char value[F2E_MAX_VALUE];
    if (f2e_json_list_finish(&errors, value, sizeof(value))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->errors_env, value);
    }
  }

  f2e_json_list_discard(&positionals);
  f2e_json_list_discard(&unknown_options);
  f2e_json_list_discard(&errors);

  char *json = f2e_pairs_to_json(pairs, F2E_MAX_PAIRS);
  free(pairs);
  free(config);
  if (!json) {
    json = f2e_empty_json_object();
  }
  return json;
}

char *f2e_parse(int argc, const char *const argv[]) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_empty_json_object();
  }
  char *result = f2e_parse_from_file(path, argc, argv);
  free(path);
  return result;
}

static int f2e_json_list_close(F2EJsonList *list) {
  if (!list || list->failed || !list->initialized) {
    return 0;
  }
  if (!f2e_buffer_append_char(&list->buffer, ']')) {
    list->failed = 1;
    return 0;
  }
  return 1;
}

/*
 * Structured parse: returns every channel separately instead of packing them
 * into env keys, so nothing can be shadowed by real environment variables:
 *   {"flags":{...},               the same env map f2e_parse returns
 *    "providedFlags":{...},       argv-derived values without schema defaults
 *    "command":"remote add",      space-joined resolved command path
 *    "subcommands":["remote","add"],
 *    "extras":["abc","efg"],      operands: positionals after the last matched
 *                                 command (including tokens after --); with no
 *                                 command matched, everything but argv[0]
 *    "unknownOptions":[...],      collected regardless of unknown_options_env
 *    "errors":[...]}              collected regardless of errors_env
 */
char *f2e_parse_structured_from_file(const char *config_path, int argc, const char *const argv[]) {
  if (argc < 0 || !argv) {
    argc = 0;
  }

  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return NULL;
  }

  F2EPair *pairs = (F2EPair *)calloc(F2E_MAX_PAIRS, sizeof(F2EPair));
  if (!pairs) {
    free(config);
    return NULL;
  }
  F2EPair *provided_pairs = (F2EPair *)calloc(F2E_MAX_PAIRS, sizeof(F2EPair));
  if (!provided_pairs) {
    free(pairs);
    free(config);
    return NULL;
  }

  F2EJsonList positionals = {0};
  F2EJsonList unknown_options = {0};
  F2EJsonList errors = {0};
  F2EJsonList extras = {0};
  F2EJsonList subcommands = {0};
  int track_positionals = config->positionals_env[0] != '\0' && f2e_json_list_init(&positionals);
  int lists_ok = f2e_json_list_init(&unknown_options) &&
                 f2e_json_list_init(&errors) &&
                 f2e_json_list_init(&extras) &&
                 f2e_json_list_init(&subcommands);
  int allow_unknown_forced = 0;
  int allow_unknown = f2e_resolve_allow_unknown(config, argc, argv, &allow_unknown_forced);

  F2ECommandPath path;
  memset(&path, 0, sizeof(path));
  int lenient = 0;
  if (config->command_count > 0) {
    f2e_resolve_command_path(config, argc, argv, &path);
    lenient = path.depth == 0;
    char joined[F2E_MAX_VALUE];
    int tail = path.depth > 0 ? path.commands[path.depth - 1] : F2E_SCOPE_ROOT;
    if (f2e_command_path_label(config, tail, joined, sizeof(joined))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->command_env, joined);
      f2e_set_pair(provided_pairs, F2E_MAX_PAIRS, config->command_env, joined);
    }
    for (size_t i = 0; i < path.depth; i++) {
      const F2ECommand *command = &config->commands[path.commands[i]];
      f2e_json_list_append(&subcommands, command->name);
      if (command->env[0] != '\0') {
        f2e_set_pair(pairs, F2E_MAX_PAIRS, command->env, "true");
        f2e_set_pair(provided_pairs, F2E_MAX_PAIRS, command->env, "true");
      }
    }
    f2e_apply_defaults_for_path(config, pairs, F2E_MAX_PAIRS, &path);
  } else {
    f2e_apply_defaults(config, pairs, F2E_MAX_PAIRS);
  }

  f2e_scan_argv(config,
                pairs,
                F2E_MAX_PAIRS,
                argc,
                argv,
                track_positionals ? &positionals : NULL,
                lists_ok ? &unknown_options : NULL,
                lists_ok ? &errors : NULL,
                lists_ok ? &extras : NULL,
                path.depth > 0,
                allow_unknown,
                allow_unknown_forced,
                lenient,
                NULL);

  /*
   * Scan the same argv without seeded defaults. Diagnostics and operands were
   * already collected above; this pass only captures normalized CLI
   * overrides for callers that merge them over a real environment before
   * coercion.
   */
  f2e_scan_argv(config,
                provided_pairs,
                F2E_MAX_PAIRS,
                argc,
                argv,
                NULL,
                NULL,
                NULL,
                NULL,
                0,
                allow_unknown,
                allow_unknown_forced,
                lenient,
                NULL);

  if (track_positionals && positionals.count > 0) {
    char value[F2E_MAX_VALUE];
    if (f2e_json_list_finish(&positionals, value, sizeof(value))) {
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->positionals_env, value);
      f2e_set_pair(provided_pairs, F2E_MAX_PAIRS, config->positionals_env, value);
    }
  }
  if (config->unknown_options_env[0] != '\0' && unknown_options.count > 0) {
    char value[F2E_MAX_VALUE];
    F2EBuffer copy = unknown_options.buffer;
    if (copy.len + 2 <= sizeof(value)) {
      memcpy(value, copy.data, copy.len);
      value[copy.len] = ']';
      value[copy.len + 1] = '\0';
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->unknown_options_env, value);
      f2e_set_pair(provided_pairs, F2E_MAX_PAIRS, config->unknown_options_env, value);
    }
  }
  if (config->errors_env[0] != '\0' && errors.count > 0) {
    char value[F2E_MAX_VALUE];
    F2EBuffer copy = errors.buffer;
    if (copy.len + 2 <= sizeof(value)) {
      memcpy(value, copy.data, copy.len);
      value[copy.len] = ']';
      value[copy.len + 1] = '\0';
      f2e_set_pair(pairs, F2E_MAX_PAIRS, config->errors_env, value);
      f2e_set_pair(provided_pairs, F2E_MAX_PAIRS, config->errors_env, value);
    }
  }

  char *flags_json = f2e_pairs_to_json(pairs, F2E_MAX_PAIRS);
  char *provided_flags_json = f2e_pairs_to_json(provided_pairs, F2E_MAX_PAIRS);
  char label[F2E_MAX_VALUE];
  label[0] = '\0';
  if (path.depth > 0 &&
      !f2e_command_path_label(config, path.commands[path.depth - 1], label, sizeof(label))) {
    label[0] = '\0';
  }

  char *result = NULL;
  if (flags_json && provided_flags_json && lists_ok &&
      f2e_json_list_close(&subcommands) &&
      f2e_json_list_close(&extras) &&
      f2e_json_list_close(&unknown_options) &&
      f2e_json_list_close(&errors)) {
    F2EBuffer out = {0};
    if (f2e_buffer_init(&out)) {
      int ok = f2e_buffer_append(&out, "{\"flags\":") &&
               f2e_buffer_append(&out, flags_json) &&
               f2e_buffer_append(&out, ",\"providedFlags\":") &&
               f2e_buffer_append(&out, provided_flags_json) &&
               f2e_buffer_append(&out, ",\"command\":") &&
               f2e_buffer_append_json_string(&out, label) &&
               f2e_buffer_append(&out, ",\"subcommands\":") &&
               f2e_buffer_append(&out, subcommands.buffer.data) &&
               f2e_buffer_append(&out, ",\"extras\":") &&
               f2e_buffer_append(&out, extras.buffer.data) &&
               f2e_buffer_append(&out, ",\"unknownOptions\":") &&
               f2e_buffer_append(&out, unknown_options.buffer.data) &&
               f2e_buffer_append(&out, ",\"errors\":") &&
               f2e_buffer_append(&out, errors.buffer.data) &&
               f2e_buffer_append_char(&out, '}');
      if (ok) {
        result = out.data;
      } else {
        free(out.data);
      }
    }
  }

  f2e_free(flags_json);
  f2e_free(provided_flags_json);
  f2e_json_list_discard(&positionals);
  f2e_json_list_discard(&unknown_options);
  f2e_json_list_discard(&errors);
  f2e_json_list_discard(&extras);
  f2e_json_list_discard(&subcommands);
  free(pairs);
  free(provided_pairs);
  free(config);
  return result;
}

char *f2e_parse_structured(int argc, const char *const argv[]) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *result = f2e_parse_structured_from_file(path, argc, argv);
  free(path);
  return result;
}

/*
 * Returns the resolved command path as its own JSON report, independent of
 * the env map (whose keys can be shadowed by real environment variables):
 *   {"path":["remote","add"],"label":"remote add"}
 * An empty path means argv selected no command (or none are declared).
 */
char *f2e_resolve_commands_from_file(const char *config_path, int argc, const char *const argv[]) {
  if (argc < 0 || !argv) {
    argc = 0;
  }

  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return NULL;
  }

  F2ECommandPath path;
  memset(&path, 0, sizeof(path));
  if (config->command_count > 0) {
    f2e_resolve_command_path(config, argc, argv, &path);
  }

  F2EBuffer out = {0};
  if (!f2e_buffer_init(&out)) {
    free(config);
    return NULL;
  }

  int ok = f2e_buffer_append(&out, "{\"path\":[");
  for (size_t i = 0; ok && i < path.depth; i++) {
    if (i > 0) {
      ok = f2e_buffer_append_char(&out, ',');
    }
    ok = ok && f2e_buffer_append_json_string(&out, config->commands[path.commands[i]].name);
  }
  char label[F2E_MAX_VALUE];
  label[0] = '\0';
  if (path.depth > 0) {
    if (!f2e_command_path_label(config, path.commands[path.depth - 1], label, sizeof(label))) {
      label[0] = '\0';
    }
  }
  ok = ok &&
       f2e_buffer_append(&out, "],\"label\":") &&
       f2e_buffer_append_json_string(&out, label) &&
       f2e_buffer_append_char(&out, '}');

  free(config);
  if (!ok) {
    free(out.data);
    return NULL;
  }
  return out.data;
}

char *f2e_resolve_commands(int argc, const char *const argv[]) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *result = f2e_resolve_commands_from_file(path, argc, argv);
  free(path);
  return result;
}

static int f2e_json_array_append(char ***items, int *count, int *cap, const char *value) {
  if (*count >= *cap) {
    int next = *cap == 0 ? 8 : *cap * 2;
    char **grown = (char **)realloc(*items, sizeof(char *) * (size_t)next);
    if (!grown) {
      return 0;
    }
    *items = grown;
    *cap = next;
  }
  char *copy = (char *)malloc(strlen(value) + 1);
  if (!copy) {
    return 0;
  }
  strcpy(copy, value);
  (*items)[(*count)++] = copy;
  return 1;
}

static int f2e_parse_json_string_token(const char **cursor_ref, char *out, size_t out_size) {
  const char *cursor = f2e_trim_left((char *)*cursor_ref);
  size_t len = 0;
  int overflow = 0;
  if (*cursor != '"') {
    return 0;
  }
  cursor++;
  while (*cursor && *cursor != '"') {
    char ch = *cursor++;
    if (ch == '\\' && *cursor) {
      char escaped = *cursor++;
      switch (escaped) {
        case 'b':
          ch = '\b';
          break;
        case 'f':
          ch = '\f';
          break;
        case 'n':
          ch = '\n';
          break;
        case 'r':
          ch = '\r';
          break;
        case 't':
          ch = '\t';
          break;
        case '"':
        case '\\':
        case '/':
          ch = escaped;
          break;
        case 'u':
          if (cursor[0] && cursor[1] && cursor[2] && cursor[3]) {
            ch = '?';
            cursor += 4;
          }
          break;
        default:
          ch = escaped;
          break;
      }
    }
    if (len + 1 < out_size) {
      out[len++] = ch;
    } else {
      overflow = 1;
    }
  }
  if (*cursor != '"') {
    return 0;
  }
  cursor++;
  if (out_size > 0) {
    out[len] = '\0';
  }
  *cursor_ref = cursor;
  return !overflow;
}

typedef enum {
  F2E_JSON_INPUT_STRING = 0,
  F2E_JSON_INPUT_NUMBER = 1,
  F2E_JSON_INPUT_BOOL = 2,
  F2E_JSON_INPUT_ARRAY = 3,
  F2E_JSON_INPUT_OBJECT = 4,
  F2E_JSON_INPUT_NULL = 5
} F2EJsonInputKind;

typedef struct {
  int present;
  F2EJsonInputKind kind;
  char text[F2E_MAX_VALUE];
  char raw[F2E_MAX_LINE];
} F2ECoerceValue;

static size_t f2e_find_flag_index_by_env(const F2EConfig *config, const char *env) {
  if (!config || !env) {
    return SIZE_MAX;
  }
  for (size_t i = 0; i < config->flag_count; i++) {
    if (f2e_streq(config->flags[i].env, env)) {
      return i;
    }
  }
  return SIZE_MAX;
}

/*
 * Coercion slots: [0, flag_count) are flags, [flag_count, flag_count +
 * command_count) are per-command marker envs, and the final slot is
 * parse.command_env.
 */
static size_t f2e_coerce_slot_count_values(size_t flag_count, size_t command_count) {
  return flag_count + command_count + 1;
}

static size_t f2e_coerce_slot_count(const F2EConfig *config) {
  return f2e_coerce_slot_count_values(config->flag_count, config->command_count);
}

static size_t f2e_coerce_slot_for_key(const F2EConfig *config, const char *key) {
  size_t index = f2e_find_flag_index_by_env(config, key);
  if (index != SIZE_MAX) {
    return index;
  }
  for (size_t i = 0; i < config->command_count; i++) {
    if (config->commands[i].env[0] != '\0' && f2e_streq(config->commands[i].env, key)) {
      return config->flag_count + i;
    }
  }
  if (config->command_count > 0 && config->command_env[0] != '\0' && f2e_streq(config->command_env, key)) {
    return config->flag_count + config->command_count;
  }
  return SIZE_MAX;
}

static int f2e_coerce_value_is_true(const F2ECoerceValue *value) {
  if (!value || !value->present) {
    return 0;
  }
  if (value->kind == F2E_JSON_INPUT_BOOL) {
    return f2e_streq(value->raw, "true");
  }
  return value->kind == F2E_JSON_INPUT_STRING && f2e_streq(value->text, "true");
}

static int f2e_command_is_same_or_descendant(const F2EConfig *config,
                                             int candidate,
                                             int ancestor) {
  while (candidate >= 0 && (size_t)candidate < config->command_count) {
    if (candidate == ancestor) {
      return 1;
    }
    candidate = config->commands[candidate].parent;
  }
  return 0;
}

/*
 * Command-scoped defaults are only materialized when the input identifies an
 * active command. Parsed maps always carry parse.command_env; command marker
 * envs are also accepted for callers that construct a map directly.
 */
static int f2e_coerce_command_is_active(const F2EConfig *config,
                                        const F2ECoerceValue *values,
                                        int command) {
  if (!config || !values || command < 0 || (size_t)command >= config->command_count) {
    return 0;
  }

  const F2ECoerceValue *path =
      &values[config->flag_count + config->command_count];
  if (path->present && path->kind == F2E_JSON_INPUT_STRING) {
    for (size_t i = 0; i < config->command_count; i++) {
      char label[F2E_MAX_VALUE];
      if (f2e_command_path_label(config, (int)i, label, sizeof(label)) &&
          f2e_streq(label, path->text) &&
          f2e_command_is_same_or_descendant(config, (int)i, command)) {
        return 1;
      }
    }
  }

  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECoerceValue *marker = &values[config->flag_count + i];
    if (f2e_coerce_value_is_true(marker) &&
        f2e_command_is_same_or_descendant(config, (int)i, command)) {
      return 1;
    }
  }
  return 0;
}

static int f2e_copy_json_span(char *out, size_t out_size, const char *start, const char *end) {
  if (!out || out_size == 0 || !start || !end || end < start) {
    return 0;
  }
  size_t len = (size_t)(end - start);
  if (len >= out_size) {
    return 0;
  }
  memcpy(out, start, len);
  out[len] = '\0';
  return 1;
}

static int f2e_parse_coerce_input(const F2EConfig *config,
                                  const char *values_json,
                                  F2ECoerceValue *values,
                                  F2ECoerceValue *parse_errors) {
  if (!config || !values_json || !values || !parse_errors) {
    return 0;
  }
  const char *cursor = values_json;
  f2e_json_skip_ws(&cursor);
  if (*cursor != '{') {
    return 0;
  }
  cursor++;
  f2e_json_skip_ws(&cursor);
  if (*cursor == '}') {
    cursor++;
    f2e_json_skip_ws(&cursor);
    return *cursor == '\0';
  }

  while (*cursor) {
    char key[F2E_MAX_LINE];
    if (!f2e_parse_json_string_token(&cursor, key, sizeof(key))) {
      return 0;
    }
    size_t index = f2e_coerce_slot_for_key(config, key);
    int is_parse_errors = config->errors_env[0] != '\0' && f2e_streq(config->errors_env, key);
    f2e_json_skip_ws(&cursor);
    if (*cursor != ':') {
      return 0;
    }
    cursor++;
    f2e_json_skip_ws(&cursor);

    const char *start = cursor;
    if (index == SIZE_MAX && !is_parse_errors) {
      if (!f2e_json_parse_value(&cursor, 0)) {
        return 0;
      }
    }
    F2EJsonInputKind kind;
    char text[F2E_MAX_VALUE] = "";
    if (index == SIZE_MAX && !is_parse_errors) {
      kind = F2E_JSON_INPUT_NULL;
    } else if (*cursor == '"') {
      kind = F2E_JSON_INPUT_STRING;
      if (!f2e_parse_json_string_token(&cursor, text, sizeof(text))) {
        return 0;
      }
    } else {
      switch (*cursor) {
        case '{':
          kind = F2E_JSON_INPUT_OBJECT;
          break;
        case '[':
          kind = F2E_JSON_INPUT_ARRAY;
          break;
        case 't':
        case 'f':
          kind = F2E_JSON_INPUT_BOOL;
          break;
        case 'n':
          kind = F2E_JSON_INPUT_NULL;
          break;
        default:
          kind = F2E_JSON_INPUT_NUMBER;
          break;
      }
      if (!f2e_json_parse_value(&cursor, 0)) {
        return 0;
      }
    }

    if (index != SIZE_MAX) {
      F2ECoerceValue *value = &values[index];
      value->present = 1;
      value->kind = kind;
      f2e_strlcpy(value->text, text, sizeof(value->text));
      if (!f2e_copy_json_span(value->raw, sizeof(value->raw), start, cursor)) {
        return 0;
      }
    } else if (is_parse_errors) {
      parse_errors->present = 1;
      parse_errors->kind = kind;
      f2e_strlcpy(parse_errors->text, text, sizeof(parse_errors->text));
      if (!f2e_copy_json_span(parse_errors->raw, sizeof(parse_errors->raw), start, cursor)) {
        return 0;
      }
    }

    f2e_json_skip_ws(&cursor);
    if (*cursor == ',') {
      cursor++;
      f2e_json_skip_ws(&cursor);
      if (*cursor == '}') {
        return 0;
      }
      continue;
    }
    if (*cursor == '}') {
      cursor++;
      f2e_json_skip_ws(&cursor);
      return *cursor == '\0';
    }
    return 0;
  }
  return 0;
}

static int f2e_coerce_append_parse_errors(F2EJsonList *errors, const char *json) {
  if (!errors || !json || !f2e_json_container_is_valid(json, '[')) {
    return 0;
  }
  const char *cursor = json;
  f2e_json_skip_ws(&cursor);
  cursor++;
  f2e_json_skip_ws(&cursor);
  if (*cursor == ']') {
    return 1;
  }
  while (*cursor) {
    char message[F2E_MAX_VALUE];
    if (!f2e_parse_json_string_token(&cursor, message, sizeof(message)) ||
        !f2e_json_list_append(errors, message)) {
      return 0;
    }
    f2e_json_skip_ws(&cursor);
    if (*cursor == ',') {
      cursor++;
      f2e_json_skip_ws(&cursor);
      continue;
    }
    if (*cursor == ']') {
      cursor++;
      f2e_json_skip_ws(&cursor);
      return *cursor == '\0';
    }
    return 0;
  }
  return 0;
}

static const char *f2e_coerce_input_kind_name(F2EJsonInputKind kind) {
  switch (kind) {
    case F2E_JSON_INPUT_STRING:
      return "a string";
    case F2E_JSON_INPUT_NUMBER:
      return "a number";
    case F2E_JSON_INPUT_BOOL:
      return "a boolean";
    case F2E_JSON_INPUT_ARRAY:
      return "a JSON array";
    case F2E_JSON_INPUT_OBJECT:
      return "a JSON object";
    case F2E_JSON_INPUT_NULL:
      return "null";
    default:
      return "an unsupported value";
  }
}

static void f2e_coerce_add_error(F2EJsonList *errors,
                                 const F2EFlag *flag,
                                 const char *expected,
                                 F2EJsonInputKind received_kind) {
  char message[F2E_MAX_VALUE];
  const char *env = flag && flag->env[0] ? flag->env : "<unknown>";
  const char *name = flag && flag->name[0] ? flag->name : "<unknown>";
  const char *declared_type = flag ? f2e_value_type_name(flag->type) : "unknown";
  snprintf(message,
           sizeof(message),
           "env %s (flags.%s) must be %s because .cli-flags.toml declares type = \"%s\"; received %s. Set %s to %s or update flags.%s.type",
           env,
           name,
           expected,
           declared_type,
           f2e_coerce_input_kind_name(received_kind),
           env,
           expected,
           name);
  f2e_json_list_append(errors, message);
}

static int f2e_coerce_value_to_json(const F2EFlag *flag,
                                    const F2ECoerceValue *input,
                                    const char *default_value,
                                    F2EBuffer *out,
                                    F2EJsonList *errors) {
  if (!flag || !out || !errors || !f2e_buffer_init(out)) {
    if (errors) {
      errors->failed = 1;
    }
    return 0;
  }

  F2EJsonInputKind kind = input ? input->kind : F2E_JSON_INPUT_STRING;
  const char *text = input ? input->text : default_value;
  const char *raw = input ? input->raw : NULL;
  int valid = 0;

  switch (flag->type) {
    case F2E_TYPE_STRING:
      valid = kind == F2E_JSON_INPUT_STRING && f2e_buffer_append_json_string(out, text ? text : "");
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "a string", kind);
      }
      break;

    case F2E_TYPE_BOOL:
      if (kind == F2E_JSON_INPUT_BOOL && raw) {
        valid = f2e_buffer_append(out, raw);
      } else if (kind == F2E_JSON_INPUT_STRING) {
        const char *canonical = NULL;
        if (f2e_bool_value_alias(flag, text, &canonical)) {
          valid = f2e_buffer_append(out, canonical);
        }
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "a boolean", kind);
      }
      break;

    case F2E_TYPE_INT:
      if ((kind == F2E_JSON_INPUT_STRING || kind == F2E_JSON_INPUT_NUMBER) &&
          f2e_int_value_is_valid(kind == F2E_JSON_INPUT_STRING ? text : raw)) {
        char canonical[64];
        long long parsed = strtoll(kind == F2E_JSON_INPUT_STRING ? text : raw, NULL, 10);
        snprintf(canonical, sizeof(canonical), "%lld", parsed);
        valid = f2e_buffer_append(out, canonical);
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "an integer", kind);
      }
      break;

    case F2E_TYPE_FLOAT:
      if ((kind == F2E_JSON_INPUT_STRING || kind == F2E_JSON_INPUT_NUMBER) &&
          f2e_float_value_is_valid(kind == F2E_JSON_INPUT_STRING ? text : raw)) {
        char canonical[128];
        double parsed = strtod(kind == F2E_JSON_INPUT_STRING ? text : raw, NULL);
        snprintf(canonical, sizeof(canonical), "%.17g", parsed);
        valid = f2e_json_value_is_valid(canonical) && f2e_buffer_append(out, canonical);
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "a finite number", kind);
      }
      break;

    case F2E_TYPE_JSON: {
      const char *json = kind == F2E_JSON_INPUT_STRING ? text : raw;
      if (json && f2e_json_value_is_valid(json)) {
        valid = f2e_buffer_append(out, json);
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "valid JSON", kind);
      }
      break;
    }

    case F2E_TYPE_ARRAY: {
      const char *json = kind == F2E_JSON_INPUT_STRING ? text : raw;
      if ((kind == F2E_JSON_INPUT_STRING || kind == F2E_JSON_INPUT_ARRAY) &&
          json && f2e_json_container_is_valid(json, '[')) {
        valid = f2e_buffer_append(out, json);
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "a JSON array", kind);
      }
      break;
    }

    case F2E_TYPE_MAP: {
      const char *json = kind == F2E_JSON_INPUT_STRING ? text : raw;
      if ((kind == F2E_JSON_INPUT_STRING || kind == F2E_JSON_INPUT_OBJECT) &&
          json && f2e_json_container_is_valid(json, '{')) {
        valid = f2e_buffer_append(out, json);
      }
      if (!valid) {
        f2e_coerce_add_error(errors, flag, "a JSON object", kind);
      }
      break;
    }

    default:
      f2e_coerce_add_error(errors, flag, "a supported value", kind);
      break;
  }

  if (!valid) {
    free(out->data);
    memset(out, 0, sizeof(*out));
  }
  return valid;
}

static char *f2e_coerce_error_report(const char *message) {
  F2EBuffer report = {0};
  if (!f2e_buffer_init(&report)) {
    return NULL;
  }
  if (!f2e_buffer_append(&report, "{\"ok\":false,\"errors\":[") ||
      !f2e_buffer_append_json_string(&report, message ? message : "coercion failed") ||
      !f2e_buffer_append(&report, "]}")) {
    free(report.data);
    return NULL;
  }
  return report.data;
}

static char *f2e_coerce_report_from_config(const F2EConfig *config, const char *values_json) {
  if (!config || !values_json) {
    return f2e_coerce_error_report("values must be a JSON object");
  }

  F2ECoerceValue *values = (F2ECoerceValue *)calloc(f2e_coerce_slot_count(config), sizeof(F2ECoerceValue));
  if (!values) {
    return NULL;
  }
  F2ECoerceValue parse_errors = {0};
  if (!f2e_parse_coerce_input(config, values_json, values, &parse_errors)) {
    free(values);
    return f2e_coerce_error_report("values must be a valid JSON object with supported value sizes");
  }

  F2EJsonList errors = {0};
  F2EBuffer output = {0};
  if (!f2e_json_list_init(&errors) || !f2e_buffer_init(&output) || !f2e_buffer_append_char(&output, '{')) {
    free(values);
    f2e_json_list_discard(&errors);
    free(output.data);
    return NULL;
  }

  if (parse_errors.present) {
    const char *json = parse_errors.kind == F2E_JSON_INPUT_STRING ? parse_errors.text : parse_errors.raw;
    if ((parse_errors.kind != F2E_JSON_INPUT_STRING && parse_errors.kind != F2E_JSON_INPUT_ARRAY) ||
        !f2e_coerce_append_parse_errors(&errors, json)) {
      char message[512];
      snprintf(message, sizeof(message), "env %s must be a JSON array of error strings", config->errors_env);
      f2e_json_list_append(&errors, message);
    }
  }

  int wrote = 0;
  for (size_t i = 0; i < config->flag_count; i++) {
    const F2EFlag *flag = &config->flags[i];
    const F2ECoerceValue *input = values[i].present ? &values[i] : NULL;
    if (!input && !flag->has_default) {
      continue;
    }
    if (!input &&
        flag->command != F2E_SCOPE_ROOT &&
        !f2e_coerce_command_is_active(config, values, flag->command)) {
      continue;
    }

    F2EBuffer coerced = {0};
    if (!f2e_coerce_value_to_json(flag, input, flag->default_value, &coerced, &errors)) {
      continue;
    }
    if ((wrote && !f2e_buffer_append_char(&output, ',')) ||
        !f2e_buffer_append_json_string(&output, flag->env) ||
        !f2e_buffer_append_char(&output, ':') ||
        !f2e_buffer_append(&output, coerced.data)) {
      errors.failed = 1;
    } else {
      wrote = 1;
    }
    free(coerced.data);
  }

  for (size_t i = 0; i < config->command_count; i++) {
    const F2ECoerceValue *input = &values[config->flag_count + i];
    const char *env = config->commands[i].env;
    if (!input->present || env[0] == '\0') {
      continue;
    }
    const char *emitted = NULL;
    if (input->kind == F2E_JSON_INPUT_BOOL) {
      emitted = input->raw;
    } else if (input->kind == F2E_JSON_INPUT_STRING &&
               (f2e_streq(input->text, "true") || f2e_streq(input->text, "false"))) {
      emitted = f2e_streq(input->text, "true") ? "true" : "false";
    }
    if (!emitted) {
      char message[F2E_MAX_VALUE];
      snprintf(message, sizeof(message),
               "env %s (commands.%s) must be a boolean command marker; received %s",
               env,
               config->commands[i].name,
               f2e_coerce_input_kind_name(input->kind));
      f2e_json_list_append(&errors, message);
      continue;
    }
    if ((wrote && !f2e_buffer_append_char(&output, ',')) ||
        !f2e_buffer_append_json_string(&output, env) ||
        !f2e_buffer_append_char(&output, ':') ||
        !f2e_buffer_append(&output, emitted)) {
      errors.failed = 1;
    } else {
      wrote = 1;
    }
  }

  {
    const F2ECoerceValue *input = &values[config->flag_count + config->command_count];
    if (input->present && config->command_env[0] != '\0') {
      if (input->kind != F2E_JSON_INPUT_STRING) {
        char message[F2E_MAX_VALUE];
        snprintf(message, sizeof(message),
                 "env %s (parse.command_env) must be a string command path; received %s",
                 config->command_env,
                 f2e_coerce_input_kind_name(input->kind));
        f2e_json_list_append(&errors, message);
      } else if ((wrote && !f2e_buffer_append_char(&output, ',')) ||
                 !f2e_buffer_append_json_string(&output, config->command_env) ||
                 !f2e_buffer_append_char(&output, ':') ||
                 !f2e_buffer_append_json_string(&output, input->text)) {
        errors.failed = 1;
      } else {
        wrote = 1;
      }
    }
  }
  free(values);

  if (!f2e_buffer_append_char(&output, '}') || errors.failed) {
    free(output.data);
    f2e_json_list_discard(&errors);
    return NULL;
  }

  F2EBuffer report = {0};
  if (!f2e_buffer_init(&report)) {
    free(output.data);
    f2e_json_list_discard(&errors);
    return NULL;
  }
  int ok = 1;
  if (errors.count > 0) {
    ok = f2e_buffer_append_char(&errors.buffer, ']') &&
         f2e_buffer_append(&report, "{\"ok\":false,\"errors\":") &&
         f2e_buffer_append(&report, errors.buffer.data) &&
         f2e_buffer_append_char(&report, '}');
  } else {
    ok = f2e_buffer_append(&report, "{\"ok\":true,\"value\":") &&
         f2e_buffer_append(&report, output.data) &&
         f2e_buffer_append_char(&report, '}');
  }
  free(output.data);
  f2e_json_list_discard(&errors);
  if (!ok) {
    free(report.data);
    return NULL;
  }
  return report.data;
}

char *f2e_coerce_json_from_file(const char *config_path, const char *values_json) {
  F2EConfig *config = (F2EConfig *)malloc(sizeof(F2EConfig));
  if (!config) {
    return NULL;
  }
  if (!config_path || !f2e_load_config(config_path, config)) {
    free(config);
    return f2e_coerce_error_report(
        "could not read .cli-flags.toml; verify configPath and file permissions");
  }
  if (f2e_config_has_audit_errors(config)) {
    free(config);
    return f2e_coerce_error_report(
        ".cli-flags.toml failed audit; run \"f2e audit <path>\" and fix the reported errors");
  }
  char *report = f2e_coerce_report_from_config(config, values_json);
  free(config);
  return report;
}

char *f2e_coerce_json(const char *values_json) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_coerce_error_report(
        "no usable .cli-flags.toml found before HOME; add one to the project or pass an explicit configPath");
  }
  char *report = f2e_coerce_json_from_file(path, values_json);
  free(path);
  return report;
}

static void f2e_free_json_items(char **items, int count);

static int f2e_parse_json_argv_items(const char *argv_json, char ***items, int *count) {
  const char *cursor = f2e_trim_left((char *)argv_json);
  int cap = 0;
  int expecting_value = 1;
  int saw_value = 0;
  *items = NULL;
  *count = 0;

  if (!argv_json || *cursor != '[') {
    return 0;
  }
  cursor++;

  while (*cursor) {
    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ']') {
      return !saw_value || !expecting_value;
    }

    char value[F2E_MAX_VALUE];
    if (!f2e_parse_json_string_token(&cursor, value, sizeof(value))) {
      return 0;
    }
    if (!f2e_json_array_append(items, count, &cap, value)) {
      return 0;
    }
    saw_value = 1;
    expecting_value = 0;

    cursor = f2e_trim_left((char *)cursor);
    if (*cursor == ',') {
      cursor++;
      expecting_value = 1;
      continue;
    }
    if (*cursor == ']') {
      return 1;
    }
    return 0;
  }
  return 0;
}

int f2e_is_help_requested_json_argv(const char *argv_json) {
  char **items = NULL;
  int count = 0;
  if (!argv_json || !f2e_parse_json_argv_items(argv_json, &items, &count)) {
    f2e_free_json_items(items, count);
    return 0;
  }
  int requested = f2e_is_help_requested(count, (const char *const *)items);
  f2e_free_json_items(items, count);
  return requested;
}

char *f2e_help_table_for_json_argv_from_file(const char *config_path,
                                             const char *command_name,
                                             const char *argv_json,
                                             int terminal_columns) {
  char **items = NULL;
  int count = 0;
  if (!argv_json || !f2e_parse_json_argv_items(argv_json, &items, &count)) {
    f2e_free_json_items(items, count);
    return f2e_help_table_from_file(config_path, command_name, terminal_columns);
  }
  char *table = f2e_help_table_for_argv_from_file(config_path,
                                                  command_name,
                                                  count,
                                                  (const char *const *)items,
                                                  terminal_columns);
  f2e_free_json_items(items, count);
  return table;
}

char *f2e_help_table_for_json_argv(const char *command_name, const char *argv_json, int terminal_columns) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *table = f2e_help_table_for_json_argv_from_file(path, command_name, argv_json, terminal_columns);
  free(path);
  return table;
}

static void f2e_free_json_items(char **items, int count) {
  if (!items) {
    return;
  }
  for (int i = 0; i < count; i++) {
    free(items[i]);
  }
  free(items);
}

static char *f2e_empty_json_object(void) {
  char *empty = (char *)malloc(3);
  if (empty) {
    f2e_strlcpy(empty, "{}", 3);
  }
  return empty;
}

#if defined(__linux__)
static int f2e_read_process_argv(char ***items, int *count) {
  FILE *file = fopen("/proc/self/cmdline", "rb");
  if (!file) {
    return 0;
  }

  size_t len = 0;
  size_t cap = 256;
  char *data = (char *)malloc(cap);
  if (!data) {
    fclose(file);
    return 0;
  }

  int ch;
  while ((ch = fgetc(file)) != EOF) {
    if (len + 1 >= cap) {
      cap *= 2;
      char *grown = (char *)realloc(data, cap);
      if (!grown) {
        free(data);
        fclose(file);
        return 0;
      }
      data = grown;
    }
    data[len++] = (char)ch;
  }
  fclose(file);

  int argv_cap = 0;
  *items = NULL;
  *count = 0;
  size_t start = 0;
  for (size_t i = 0; i <= len; i++) {
    if (i == len || data[i] == '\0') {
      if (i > start && !f2e_json_array_append(items, count, &argv_cap, data + start)) {
        f2e_free_json_items(*items, *count);
        *items = NULL;
        *count = 0;
        free(data);
        return 0;
      }
      start = i + 1;
    }
  }

  free(data);
  return *count > 0;
}
#elif defined(__APPLE__)
static int f2e_read_process_argv(char ***items, int *count) {
  int mib[3] = {CTL_KERN, KERN_PROCARGS2, getpid()};
  size_t size = 0;
  if (sysctl(mib, 3, NULL, &size, NULL, 0) != 0 || size == 0) {
    return 0;
  }

  char *data = (char *)malloc(size);
  if (!data) {
    return 0;
  }
  if (sysctl(mib, 3, data, &size, NULL, 0) != 0) {
    free(data);
    return 0;
  }

  int argc = 0;
  memcpy(&argc, data, sizeof(argc));
  if (argc <= 0) {
    free(data);
    return 0;
  }

  char *cursor = data + sizeof(argc);
  char *end = data + size;
  while (cursor < end && *cursor != '\0') {
    cursor++;
  }
  while (cursor < end && *cursor == '\0') {
    cursor++;
  }

  int argv_cap = 0;
  *items = NULL;
  *count = 0;
  for (int i = 0; i < argc && cursor < end; i++) {
    if (!f2e_json_array_append(items, count, &argv_cap, cursor)) {
      f2e_free_json_items(*items, *count);
      *items = NULL;
      *count = 0;
      free(data);
      return 0;
    }
    while (cursor < end && *cursor != '\0') {
      cursor++;
    }
    while (cursor < end && *cursor == '\0') {
      cursor++;
    }
  }

  free(data);
  return *count > 0;
}
#elif defined(_WIN32)
static int f2e_read_process_argv(char ***items, int *count) {
  int argc = 0;
  LPWSTR *wide_argv = CommandLineToArgvW(GetCommandLineW(), &argc);
  if (!wide_argv || argc <= 0) {
    return 0;
  }

  int argv_cap = 0;
  *items = NULL;
  *count = 0;
  for (int i = 0; i < argc; i++) {
    int utf8_len = WideCharToMultiByte(CP_UTF8, 0, wide_argv[i], -1, NULL, 0, NULL, NULL);
    if (utf8_len <= 0) {
      continue;
    }
    char *value = (char *)malloc((size_t)utf8_len);
    if (!value) {
      f2e_free_json_items(*items, *count);
      LocalFree(wide_argv);
      return 0;
    }
    WideCharToMultiByte(CP_UTF8, 0, wide_argv[i], -1, value, utf8_len, NULL, NULL);
    if (!f2e_json_array_append(items, count, &argv_cap, value)) {
      free(value);
      f2e_free_json_items(*items, *count);
      LocalFree(wide_argv);
      return 0;
    }
    free(value);
  }

  LocalFree(wide_argv);
  return *count > 0;
}
#else
static int f2e_read_process_argv(char ***items, int *count) {
  *items = NULL;
  *count = 0;
  return 0;
}
#endif

char *f2e_parse_process_from_file(const char *config_path) {
  char **items = NULL;
  int count = 0;
  if (!f2e_read_process_argv(&items, &count)) {
    return f2e_parse_from_file(config_path, 0, NULL);
  }

  char *result = f2e_parse_from_file(config_path, count, (const char *const *)items);
  f2e_free_json_items(items, count);
  return result;
}

char *f2e_parse_process(void) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_empty_json_object();
  }
  char *result = f2e_parse_process_from_file(path);
  free(path);
  return result;
}

char *f2e_parse_json_argv_from_file(const char *config_path, const char *argv_json) {
  char **items = NULL;
  int count = 0;
  if (!argv_json || !f2e_parse_json_argv_items(argv_json, &items, &count)) {
    f2e_free_json_items(items, count);
    return f2e_empty_json_object();
  }

  char *result = f2e_parse_from_file(config_path, count, (const char *const *)items);
  f2e_free_json_items(items, count);
  return result;
}

char *f2e_parse_structured_json_argv_from_file(const char *config_path, const char *argv_json) {
  char **items = NULL;
  int count = 0;
  if (!argv_json || !f2e_parse_json_argv_items(argv_json, &items, &count)) {
    f2e_free_json_items(items, count);
    return f2e_parse_structured_from_file(config_path, 0, NULL);
  }
  char *result = f2e_parse_structured_from_file(config_path, count, (const char *const *)items);
  f2e_free_json_items(items, count);
  return result;
}

char *f2e_parse_structured_json_argv(const char *argv_json) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *result = f2e_parse_structured_json_argv_from_file(path, argv_json);
  free(path);
  return result;
}

char *f2e_resolve_commands_json_argv_from_file(const char *config_path, const char *argv_json) {
  char **items = NULL;
  int count = 0;
  if (!argv_json || !f2e_parse_json_argv_items(argv_json, &items, &count)) {
    f2e_free_json_items(items, count);
    return f2e_resolve_commands_from_file(config_path, 0, NULL);
  }
  char *result = f2e_resolve_commands_from_file(config_path, count, (const char *const *)items);
  f2e_free_json_items(items, count);
  return result;
}

char *f2e_resolve_commands_json_argv(const char *argv_json) {
  char *path = f2e_default_config_path();
  if (!path) {
    return NULL;
  }
  char *result = f2e_resolve_commands_json_argv_from_file(path, argv_json);
  free(path);
  return result;
}

char *f2e_parse_json_argv(const char *argv_json) {
  char *path = f2e_default_config_path();
  if (!path) {
    return f2e_empty_json_object();
  }
  char *result = f2e_parse_json_argv_from_file(path, argv_json);
  free(path);
  return result;
}

void f2e_free(char *value) {
  free(value);
}
