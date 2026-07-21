/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.sql.app;

import de.ii.xtraplatform.features.domain.SchemaBase;
import java.math.BigDecimal;
import java.util.Locale;

/**
 * Renders a typed value as an inline SQL literal for the mutation path.
 *
 * <p>The mutation SQL is assembled by string concatenation rather than JDBC bind parameters, so
 * every value inlined here must be provably safe: string/date literals are single-quoted with
 * quote-doubling, numeric literals are re-rendered from a parsed {@link BigDecimal} (never the raw
 * input text), and booleans are normalized to the {@code TRUE}/{@code FALSE} keywords. A value that
 * does not match its column type is rejected with {@link IllegalArgumentException} instead of being
 * inlined verbatim.
 */
final class SqlLiterals {

  private SqlLiterals() {}

  static String forType(SchemaBase.Type type, String value) {
    if (value == null) {
      return "NULL";
    }
    switch (type) {
      case INTEGER:
        return integer(value);
      case FLOAT:
        return number(value);
      case BOOLEAN:
        return bool(value);
      case STRING:
      case DATE:
      case DATETIME:
      default:
        // Safe default: quote everything not explicitly typed as numeric/boolean. Quoting an
        // unexpected column type yields a literal PostgreSQL/Oracle will coerce, and never lets
        // raw input reach the statement unescaped.
        return string(value);
    }
  }

  static String string(String value) {
    if (value == null) {
      return "NULL";
    }
    return "'" + value.replace("'", "''") + "'";
  }

  static String integer(String value) {
    try {
      return new BigDecimal(value.trim()).toBigIntegerExact().toString();
    } catch (NumberFormatException | ArithmeticException e) {
      throw new IllegalArgumentException("not a valid integer value: '" + value + "'");
    }
  }

  static String number(String value) {
    try {
      // toPlainString avoids scientific notation (e.g. "1E+3"), which is not portable across all
      // SQL dialects.
      return new BigDecimal(value.trim()).toPlainString();
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("not a valid number value: '" + value + "'");
    }
  }

  static String bool(String value) {
    String normalized = value.trim().toLowerCase(Locale.ROOT);
    switch (normalized) {
      case "true":
      case "t":
      case "1":
      case "yes":
        return "TRUE";
      case "false":
      case "f":
      case "0":
      case "no":
        return "FALSE";
      default:
        throw new IllegalArgumentException("not a valid boolean value: '" + value + "'");
    }
  }
}
