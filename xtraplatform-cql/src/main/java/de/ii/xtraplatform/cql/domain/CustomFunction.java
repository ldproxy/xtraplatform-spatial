/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.immutables.value.Value;

/**
 * @langEn Custom CQL2 functions allow defining reusable filter expressions that can be applied in
 *     CQL2 filter queries. Each function has a name, an optional description, optional arguments, a
 *     return type, and a SQL/dialect expression. The optional `description` field can be used to
 *     document the function's purpose.
 *     <p>#### Arguments
 *     <p>Each argument has a name, an optional description, and a type. The following types are
 *     supported: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `GEOMETRY`, `DATETIME`, `DATE`,
 *     `VALUE_ARRAY`.
 *     <p>#### Return Types
 *     <p>The return type determines how the function can be used in a CQL2 filter. Use `BOOLEAN`
 *     for functions that act as predicates (e.g. used directly as a filter condition), or `FLOAT`,
 *     `INTEGER`, `STRING` etc. for functions that are used as operands in comparisons.
 *     <p>#### Expression
 *     <p>Use exactly one of the following:
 *     <p><code>
 * - `expression` - single SQL template (recommended when functions are defined per provider)
 * - `expressions` - map of SQL templates by dialect (recommended for shared defaults)
 * </code>
 *     <p>The `expressions` map supports the following keys:
 *     <p><code>
 * - `SQL` - generic, used as fallback for all dialects
 * - `SQL/PGIS` - PostgreSQL/PostGIS
 * - `SQL/GPKG` - GeoPackage (SQLite)
 * - `SQL/ORAS` - Oracle
 * </code>
 *     <p>The dialect-specific key takes precedence over `SQL` if both are present. Arguments are
 *     referenced using `$argName` placeholders.
 *     <p>#### Example
 *     <p>The following example defines a function `MY_UPPER` that converts a string property to
 *     uppercase and checks it against a pattern:
 *     <p><code>
 * ```yaml
 * cql2Functions:
 *   - name: MY_UPPER
 *     description: 'Checks if the uppercase value of a string property matches a pattern'
 *     arguments:
 *       - name: value
 *         description: 'Input value to be transformed to upper case'
 *         type: STRING
 *       - name: pattern
 *         description: 'Pattern used for the LIKE comparison'
 *         type: STRING
 *     returns:
 *       - BOOLEAN
 *     expression: 'upper($value) LIKE $pattern'
 * ```
 * </code>
 *     <p>When functions are defined in shared defaults, use `expressions`:
 *     <p><code>
 * ```yaml
 * cql2Functions:
 *   - name: MY_UPPER
 *     arguments:
 *       - name: value
 *         type: STRING
 *       - name: pattern
 *         type: STRING
 *     returns:
 *       - BOOLEAN
 *     expressions:
 *       SQL/PGIS: 'upper($value) LIKE $pattern'
 * ```
 * </code>
 *     <p>This function can then be used in a CQL2 filter like: `MY_UPPER(name,'KUPPEL%')`
 * @langDe Benutzerdefinierte CQL2-Funktionen erlauben die Definition wiederverwendbarer
 *     Filter-Ausdrücke, die in CQL2-Filter-Abfragen eingesetzt werden können. Jede Funktion hat
 *     einen Namen, eine optionale Beschreibung, optionale Argumente, einen Rückgabetyp und einen
 *     SQL-/Dialekt-Ausdruck. Das optionale Feld `description` kann genutzt werden, um den Zweck der
 *     Funktion zu dokumentieren.
 *     <p>#### Argumente
 *     <p>Jedes Argument hat einen Namen, eine optionale Beschreibung und einen Typ. Folgende Typen
 *     werden unterstützt: `STRING`, `INTEGER`, `FLOAT`, `BOOLEAN`, `GEOMETRY`, `DATETIME`, `DATE`,
 *     `VALUE_ARRAY`.
 *     <p>#### Rückgabetypen
 *     <p>Der Rückgabetyp bestimmt, wie die Funktion in einem CQL2-Filter verwendet werden kann.
 *     `BOOLEAN` für Funktionen, die als Prädikate fungieren (d.h. direkt als Filterbedingung
 *     genutzt werden), oder `FLOAT`, `INTEGER`, `STRING` etc. für Funktionen, die als Operanden in
 *     Vergleichen verwendet werden.
 *     <p>#### Ausdruck
 *     <p>Es muss genau eines der folgenden Felder verwendet werden:
 *     <p><code>
 * - `expression` - einzelnes SQL-Template (empfohlen bei Funktionen pro Provider)
 * - `expressions` - Map mit SQL-Templates pro Dialekt (empfohlen für gemeinsame Defaults)
 * </code>
 *     <p>Die `expressions`-Map unterstützt folgende Schlüssel:
 *     <p><code>
 * - `SQL` - generisch, wird als Fallback für alle Dialekte verwendet
 * - `SQL/PGIS` - PostgreSQL/PostGIS
 * - `SQL/GPKG` - GeoPackage (SQLite)
 * - `SQL/ORAS` - Oracle
 * </code>
 *     <p>Der dialektspezifische Schlüssel hat Vorrang vor `SQL`, wenn beide vorhanden sind.
 *     Argumente werden mit `$argName`-Platzhaltern referenziert.
 *     <p>#### Beispiel
 *     <p>Das folgende Beispiel definiert eine Funktion `MY_UPPER`, die eine String-Eigenschaft in
 *     Großbuchstaben umwandelt und gegen ein Muster prüft:
 *     <p><code>
 * ```yaml
 * cql2Functions:
 *   - name: MY_UPPER
 *     description: 'Prüft ob der in Großbuchstaben umgewandelte Wert einer String-Eigenschaft einem Muster entspricht'
 *     arguments:
 *       - name: value
 *         description: 'Eingabewert, der in Großbuchstaben umgewandelt wird'
 *         type: STRING
 *       - name: pattern
 *         description: 'Muster für den LIKE-Vergleich'
 *         type: STRING
 *     returns:
 *       - BOOLEAN
 *     expression: 'upper($value) LIKE $pattern'
 * ```
 * </code>
 *     <p>Wenn Funktionen in gemeinsamen Defaults definiert werden, sollte `expressions` verwendet
 *     werden:
 *     <p><code>
 * ```yaml
 * cql2Functions:
 *   - name: MY_UPPER
 *     arguments:
 *       - name: value
 *         type: STRING
 *       - name: pattern
 *         type: STRING
 *     returns:
 *       - BOOLEAN
 *     expressions:
 *       SQL/PGIS: 'upper($value) LIKE $pattern'
 * ```
 * </code>
 *     <p>Diese Funktion kann dann in einem CQL2-Filter verwendet werden: `MY_UPPER(name,'KUPPEL%')`
 */
@Value.Immutable
@JsonDeserialize(builder = ImmutableCustomFunction.Builder.class)
public interface CustomFunction {

  String getName();

  @Nullable
  @Value.Default
  default String getDescription() {
    return null;
  }

  List<Cql2FunctionArgument> getArguments();

  List<String> getReturns();

  @Nullable
  @Value.Default
  default String getExpression() {
    return null;
  }

  @Value.Default
  default Map<String, String> getExpressions() {
    return Map.of();
  }

  @Value.Check
  default void checkExpressionDefinition() {
    boolean hasExpression = getExpression() != null && !getExpression().isBlank();
    boolean hasExpressions = !getExpressions().isEmpty();

    if (hasExpression == hasExpressions) {
      throw new IllegalStateException(
          String.format(
              "Custom function '%s' must define exactly one of 'expression' or 'expressions'",
              getName()));
    }
  }

  static CustomFunction of(
      String name, List<Cql2FunctionArgument> arguments, List<String> returns) {
    return new ImmutableCustomFunction.Builder()
        .name(name)
        .arguments(arguments)
        .returns(returns)
        .build();
  }

  static CustomFunction of(
      String name,
      @Nullable String description,
      List<Cql2FunctionArgument> arguments,
      List<String> returns,
      String expression) {
    return new ImmutableCustomFunction.Builder()
        .name(name)
        .description(description)
        .arguments(arguments)
        .returns(returns)
        .expression(expression)
        .build();
  }

  static CustomFunction of(
      String name,
      @Nullable String description,
      List<Cql2FunctionArgument> arguments,
      List<String> returns,
      Map<String, String> expressions) {
    return new ImmutableCustomFunction.Builder()
        .name(name)
        .description(description)
        .arguments(arguments)
        .returns(returns)
        .expressions(expressions)
        .build();
  }
}
