/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import static de.ii.xtraplatform.features.domain.FeatureSchema.IS_PROPERTY;

import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @langEn Mapping operations may be needed when the source and target schema structure diverge too
 *     much.
 *     <p>#### Merge
 *     <p>If only some of the `properties` are defined in an external `schema`, or if some of the
 *     `properties` should be mapped to a different table, this provides a convenient way to define
 *     these properties alongside the regular properties.
 *     <p>##### Examples
 *     <p>###### Define only some properties using an external JSON schema
 *     <p><code>
 * ```yaml
 * example:
 *   sourcePath: /main
 *   type: OBJECT
 *   merge:
 *   - properties:
 *       id:
 *         sourcePath: id
 *         type: INTEGER
 *         role: ID
 *   - sourcePath: '[JSON]names'
 *     schema: names.json
 * ```
 * </code>
 *     <p>###### Using columns from a joined table in the main feature
 *     <p><code>
 * ```yaml
 * example:
 *   sourcePath: /main
 *   type: OBJECT
 *   merge:
 *   - properties:
 *       id:
 *         sourcePath: id
 *         type: INTEGER
 *         role: ID
 *   - sourcePath: '[id=id]names'
 *     properties:
 *       name1:
 *         sourcePath: name1
 *         type: STRING
 *       name2:
 *         sourcePath: name2
 *         type: STRING
 * ```
 * </code>
 *     <p>#### Coalesce
 *     <p>If the value for a property may come from more than one `sourcePath`, this allows to
 *     choose the first non-null value.
 *     <p>##### Example
 *     <p><code>
 * ```yaml
 * foo:
 *   type: OBJECT
 *   properties:
 *     bar:
 *       type: VALUE
 *       coalesce:
 *       - sourcePath: bar_stringValue
 *         type: STRING
 *       - sourcePath: bar_integerValue
 *         type: INTEGER
 *       - sourcePath: bar_booleanValue
 *         type: BOOLEAN
 * ```
 * </code>
 *     <p>##### Type compatibility
 *     <p>Constraints on the types of inner properties depending on the type of the outer property
 *     are shown in the table below.
 *     <p><code>
 * | Outer type  | Valid inner types  | Remarks |
 * |---|---|---|
 * | `VALUE`  |  `INTEGER`, `FLOAT`, `STRING`, `BOOLEAN`, `DATETIME`, `DATE`  |   |
 * | `INTEGER`  |  `INTEGER` |   |
 * | `FLOAT`  |  `FLOAT` |   |
 * | `STRING`  |  `STRING`  |   |
 * | `BOOLEAN`  |  `BOOLEAN`  |   |
 * | `DATETIME`  |  `DATETIME`  |   |
 * | `DATE`  |  `DATE`  |   |
 * | `OBJECT`  |  `OBJECT`  | Different `objectType` with different schemas can be used  |
 * | `FEATURE_REF `  |  `FEATURE_REF `  | Different `refType` can be used  |
 * </code>
 *     <p>#### Concat
 *     <p>If the values for an array property or the instances of a feature type may come from more
 *     than one `sourcePath`, this allows to concatenate all available values.
 *     <p>For feature types using concat, the different concatenated "sub-types" must meet the
 *     following constraints:
 *     <p><code>
 * - All ID properties must have the same path and type.
 * - All primary geometry properties must have the same path and be Simple Features geometries.
 * - All primary temporal properties must have the same path and type (e.g., all are DATE instants).
 *     </code>
 *     <p>##### Examples
 *     <p><code>
 * ```yaml
 * foo:
 *   type: OBJECT
 *   properties:
 *     bar:
 *       type: FEATURE_REF_ARRAY
 *       concat:
 *       - sourcePath: '[id=foo_fk]baz1/id'
 *         refType: baz1
 *       - sourcePath: '[id=foo_fk]baz2/id'
 *         refType: baz2
 *       - sourcePath: '[id=foo_fk]bazn/id'
 *         refType: bazn
 * ```
 * </code>
 *     <p><code>
 * ```yaml
 * administrativeunit:
 *   type: OBJECT
 *   concat:
 *   - sourcePath: "/au1"
 *     type: OBJECT
 *     properties:
 *       id:
 *         sourcePath: id1
 *         type: STRING
 *         role: ID
 *   - sourcePath: "/au2"
 *     type: OBJECT
 *     properties:
 *       id:
 *         sourcePath: id2
 *         type: STRING
 *         role: ID
 * ```
 *     </code>
 *     <p>##### Type compatibility
 *     <p>Constraints on the types of inner properties depending on the type of the outer property
 *     are shown in the table below.
 *     <p><code>
 * | Outer type  | Valid inner types  | Remarks |
 * |---|---|---|
 * | `VALUE_ARRAY`  |  `VALUE_ARRAY`, `INTEGER`, `FLOAT`, `STRING`, `BOOLEAN`, `DATETIME`, `DATE`  |   |
 * | `OBJECT_ARRAY`  |  `OBJECT_ARRAY`, `OBJECT`  | Different `objectType` with different schemas can be used  |
 * | `FEATURE_REF_ARRAY `  |  `FEATURE_REF_ARRAY`, `FEATURE_REF `  | Different `refType` can be used  |
 * | `OBJECT`  |  `OBJECT`  | Only for feature types  |
 * </code>
 * @langDe Mapping Operationen können notwendig sein, wenn die Quell- and Ziel-Schema-Struktur zu
 *     unterschiedlich sind.
 *     <p>#### Merge
 *     <p>Wenn nur einige `properties` in einem externen `schema` definiert sind, oder wenn nur
 *     einige `properties` auf eine andere Tabelle gemappt werden sollen, stellt diese Option einen
 *     komfortablen Weg zur Verfügung, um solche properties zusammen mit den regulären properties zu
 *     definieren.
 *     <p>##### Beispiele
 *     <p>###### Einige Properties in einem externen JSON schema definieren
 *     <p><code>
 * ```yaml
 * example:
 *   sourcePath: /main
 *   type: OBJECT
 *   merge:
 *   - properties:
 *       id:
 *         sourcePath: id
 *         type: INTEGER
 *         role: ID
 *   - sourcePath: '[JSON]names'
 *     schema: names.json
 * ```
 * </code>
 *     <p>###### Spalten aus einer gejointen Tabelle im Haupt-Feature verwenden
 *     <p><code>
 * ```yaml
 * example:
 *   sourcePath: /main
 *   type: OBJECT
 *   merge:
 *   - properties:
 *       id:
 *         sourcePath: id
 *         type: INTEGER
 *         role: ID
 *   - sourcePath: '[id=id]names'
 *     properties:
 *       name1:
 *         sourcePath: name1
 *         type: STRING
 *       name2:
 *         sourcePath: name2
 *         type: STRING
 * ```
 * </code>
 *     <p>#### Coalesce
 *     <p>Wenn der Wert für ein Property aus mehr als einem `sourcePath` stammen kann, erlaubt diese
 *     Option den ersten Wert der nicht Null ist zu wählen.
 *     <p>##### Beispiel
 *     <p><code>
 * ```yaml
 * foo:
 *   type: OBJECT
 *   properties:
 *     bar:
 *       type: VALUE
 *       coalesce:
 *       - sourcePath: bar_stringValue
 *         type: STRING
 *       - sourcePath: bar_integerValue
 *         type: INTEGER
 *       - sourcePath: bar_booleanValue
 *         type: BOOLEAN
 * ```
 * </code>
 *     <p>##### Typ-Kompabilität
 *     <p>Die Einschränkungen für die Arten der inneren Eigenschaften in Abhängigkeit von der Art
 *     der äußeren Eigenschaft sind in der nachstehenden Tabelle aufgeführt.
 *     <p><code>
 * | Äußerer Typ | Gültige innere Typen | Bemerkungen |
 * |---|---|---|
 * | `VALUE`  |  `INTEGER`, `FLOAT`, `STRING`, `BOOLEAN`, `DATETIME`, `DATE`  |   |
 * | `INTEGER`  |  `INTEGER` |   |
 * | `FLOAT`  |  `FLOAT` |   |
 * | `STRING`  |  `STRING`  |   |
 * | `BOOLEAN`  |  `BOOLEAN`  |   |
 * | `DATETIME`  |  `DATETIME`  |   |
 * | `DATE`  |  `DATE`  |   |
 * | `OBJECT`  |  `OBJECT`  | Verschiedene `objectType` mit unterschiedlichen Schemata können verwendet werden  |
 * | `FEATURE_REF `  |  `FEATURE_REF `  | Verschiedene `refType` können verwendet werden  |
 * </code>
 *     <p>#### Concat
 *     <p>Wenn die Werte für ein Array-Property oder für eine Objektart aus mehr als einem
 *     `sourcePath` stammen können, erlaubt diese Option alle verfügbaren Werte zu konkatenieren.
 *     <p>Bei Objektarten, die 'concat' verwenden, müssen die verschiedenen verketteten „Sub-Typen“
 *     die folgenden Bedingungen erfüllen:
 *     <p><code>
 * - Alle ID-Eigenschaften müssen den gleichen Pfad und Typ haben.
 * - Alle primären Geometrieeigenschaften müssen den gleichen Pfad haben und Simple-Features-Geometrien sein.
 * - Alle primären zeitlichen Eigenschaften müssen den gleichen Pfad und Typ haben (z.B. alle sind DATE-Instanten).
 *     </code>
 *     <p>##### Beispiele
 *     <p><code>
 * ```yaml
 * foo:
 *   type: OBJECT
 *   properties:
 *     bar:
 *       type: FEATURE_REF_ARRAY
 *       concat:
 *       - sourcePath: '[id=foo_fk]baz1/id'
 *         refType: baz1
 *       - sourcePath: '[id=foo_fk]baz2/id'
 *         refType: baz2
 *       - sourcePath: '[id=foo_fk]bazn/id'
 *         refType: bazn
 * ```
 * </code>
 *     <p><code>
 * ```yaml
 * administrativeunit:
 *   type: OBJECT
 *   concat:
 *   - sourcePath: "/au1"
 *     type: OBJECT
 *     properties:
 *       id:
 *         sourcePath: id1
 *         type: STRING
 *         role: ID
 *   - sourcePath: "/au2"
 *     type: OBJECT
 *     properties:
 *       id:
 *         sourcePath: id2
 *         type: STRING
 *         role: ID
 * ```
 *     </code>
 *     <p>##### Typ-Kompabilität
 *     <p>Die Einschränkungen für die Arten der inneren Eigenschaften in Abhängigkeit von der Art
 *     der äußeren Eigenschaft sind in der nachstehenden Tabelle aufgeführt.
 *     <p><code>
 * | Äußerer Typ | Gültige innere Typen | Bemerkungen |
 * |---|---|---|
 * | `VALUE_ARRAY`  |  `VALUE_ARRAY`, `INTEGER`, `FLOAT`, `STRING`, `BOOLEAN`, `DATETIME`, `DATE`  |   |
 * | `OBJECT_ARRAY`  |  `OBJECT_ARRAY`, `OBJECT`  | Verschiedene `objectType` mit unterschiedlichen Schemata können verwendet werden  |
 * | `FEATURE_REF_ARRAY `  |  `FEATURE_REF_ARRAY`, `FEATURE_REF `  | Verschiedene `refType` können verwendet werden  |
 * </code>
 */
public class MappingOperationResolver implements TypesResolver {

  private static final Pattern CONCAT_PATH_PATTERN = Pattern.compile("[0-9]+_.*");

  private final boolean mergeOnly;

  public MappingOperationResolver() {
    this(false);
  }

  public MappingOperationResolver(boolean mergeOnly) {
    this.mergeOnly = mergeOnly;
  }

  public static boolean isConcatPath(String propertyPath) {
    return CONCAT_PATH_PATTERN.matcher(propertyPath).matches();
  }

  public static boolean isConcatPath(List<String> propertyPath) {
    return propertyPath.stream().anyMatch(MappingOperationResolver::isConcatPath);
  }

  public static String cleanConcatPath(String propertyPath) {
    if (isConcatPath(propertyPath)) {
      return propertyPath.substring(propertyPath.indexOf('_') + 1);
    }

    return propertyPath;
  }

  public static List<String> cleanConcatPath(List<String> propertyPath) {
    return propertyPath.stream()
        .map(MappingOperationResolver::cleanConcatPath)
        .collect(Collectors.toList());
  }

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    if (mergeOnly) {
      return hasMerge(property);
    }
    return hasMerge(property) || hasConcat(property) || hasCoalesce(property);
  }

  @Override
  public FeatureSchema resolve(FeatureSchema property, List<FeatureSchema> parents) {
    FeatureSchema resolved = property;

    if (hasMerge(property)) {
      resolved = resolveMerge(property);
    }

    if (!mergeOnly && hasConcat(property)) {
      resolved = resolveConcat(property);
    }

    if (!mergeOnly && hasCoalesce(property)) {
      resolved = resolveCoalesce(property);
    }

    return resolved;
  }

  private FeatureSchema resolveMerge(FeatureSchema schema) {
    Map<String, FeatureSchema> props = new LinkedHashMap<>();

    schema
        .getMerge()
        .forEach(
            partial -> {
              if (partial.getSourcePath().isPresent()) {
                partial
                    .getPropertyMap()
                    .forEach(
                        (key, property) -> {
                          props.put(
                              key,
                              new ImmutableFeatureSchema.Builder()
                                  .from(property)
                                  .sourcePath(
                                      property
                                          .getSourcePath()
                                          .map(
                                              sourcePath ->
                                                  String.format(
                                                      "%s/%s",
                                                      partial.getSourcePath().get(), sourcePath)))
                                  .sourcePaths(
                                      property.getSourcePaths().stream()
                                          .map(
                                              sourcePath ->
                                                  String.format(
                                                      "%s/%s",
                                                      partial.getSourcePath().get(), sourcePath))
                                          .collect(Collectors.toList()))
                                  .build());
                        });
              } else {
                props.putAll(partial.getPropertyMap());
              }
            });

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .merge(List.of())
        .propertyMap(props)
        .build();
  }

  public static FeatureSchema resolveConcat(FeatureSchema schema) {
    if (schema.getType() == Type.VALUE_ARRAY) {
      String basePath = schema.getSourcePath().map(p -> p + "/").orElse("");

      ImmutableFeatureSchema.Builder builder =
          new ImmutableFeatureSchema.Builder()
              .from(schema)
              .sourcePath(Optional.empty())
              .addTransformations(
                  new ImmutablePropertyTransformation.Builder().concat(false).build());

      for (FeatureSchema concat : schema.getConcat()) {
        builder.addSourcePaths(basePath + concat.getSourcePath().orElse(""));
      }

      builder.concat(
          schema.getConcat().stream()
              .map(
                  s -> {
                    if (Objects.isNull(s.getDesiredType()) || s.getValueType().isEmpty()) {
                      return new ImmutableFeatureSchema.Builder()
                          .from(s)
                          .type(Objects.isNull(s.getDesiredType()) ? schema.getType() : s.getType())
                          .valueType(
                              s.getValueType().orElse(schema.getValueType().orElse(Type.STRING)))
                          .build();
                    }
                    return s;
                  })
              .collect(Collectors.toList()));

      return builder.build();
    }

    if (schema.getType() == Type.OBJECT_ARRAY
        || (schema.getType() == Type.OBJECT && schema.getFullPath().isEmpty())) {
      String basePath = schema.getSourcePath().map(p -> p + "/").orElse("");

      ImmutableFeatureSchema.Builder builder =
          new ImmutableFeatureSchema.Builder()
              .from(schema)
              .sourcePath(Optional.empty())
              .addTransformations(
                  new ImmutablePropertyTransformation.Builder().concat(true).build());

      for (int i = 0; i < schema.getConcat().size(); i++) {
        String basePath2 =
            basePath + schema.getConcat().get(i).getSourcePath().map(p -> p + "/").orElse("");
        String basePath2NoSlash =
            basePath2.endsWith("/") ? basePath2.substring(0, basePath2.length() - 1) : basePath2;

        builder.addSourcePaths(basePath2NoSlash);

        for (FeatureSchema prop : schema.getConcat().get(i).getProperties()) {
          String prefix = i + "_";

          Optional<String> newSourcePath =
              prop.getSourcePath()
                  .map(originalPath -> basePath2 + originalPath)
                  .or(() -> prop.isObject() ? Optional.of(basePath2NoSlash) : Optional.empty());

          builder.putPropertyMap(
              prefix + prop.getName(),
              new ImmutableFeatureSchema.Builder()
                  .from(prop)
                  .sourcePath(newSourcePath)
                  .path(List.of(i + "_" + prop.getName()))
                  .transformations(
                      prop.getTransformations().stream()
                          .map(
                              transformation -> {
                                if (transformation.getRename().isPresent()) {
                                  return new ImmutablePropertyTransformation.Builder()
                                      .rename(transformation.getRename().get())
                                      .renamePathOnly(prefix + transformation.getRename().get())
                                      .build();
                                }
                                return transformation;
                              })
                          .collect(Collectors.toList()))
                  .putAdditionalInfo(IS_PROPERTY, "true"));
        }
      }

      if (schema.getConcat().stream().anyMatch(s -> Objects.isNull(s.getDesiredType()))) {
        builder.concat(
            schema.getConcat().stream()
                .map(
                    s -> {
                      if (Objects.isNull(s.getDesiredType())) {
                        return new ImmutableFeatureSchema.Builder()
                            .from(s)
                            .type(schema.getType())
                            .build();
                      }
                      return s;
                    })
                .collect(Collectors.toList()));
      }

      return builder.build();
    }

    return schema;
  }

  public static FeatureSchema resolveCoalesce(FeatureSchema schema) {
    if (schema.isValue() && !schema.isFeatureRef() && !schema.isArray()) {
      String basePath = schema.getSourcePath().map(p -> p + "/").orElse("");

      ImmutableFeatureSchema.Builder builder =
          new ImmutableFeatureSchema.Builder()
              .from(schema)
              .type(Type.VALUE_ARRAY)
              .valueType(schema.getValueType().orElse(schema.getType()))
              .sourcePath(Optional.empty())
              .addTransformations(
                  new ImmutablePropertyTransformation.Builder().coalesce(false).build());

      for (FeatureSchema coalesce : schema.getCoalesce()) {
        builder.addSourcePaths(basePath + coalesce.getSourcePath().orElse(""));
      }

      builder.coalesce(
          schema.getCoalesce().stream()
              .map(
                  s -> {
                    if (Objects.isNull(s.getDesiredType()) || s.getValueType().isEmpty()) {
                      return new ImmutableFeatureSchema.Builder()
                          .from(s)
                          .type(Objects.isNull(s.getDesiredType()) ? schema.getType() : s.getType())
                          .valueType(s.getValueType().or(schema::getValueType))
                          .build();
                    }
                    return s;
                  })
              .collect(Collectors.toList()));

      return builder.build();
    }

    if (schema.isObject() && !schema.isArray()) {
      String basePath = schema.getSourcePath().map(p -> p + "/").orElse("");

      ImmutableFeatureSchema.Builder builder =
          new ImmutableFeatureSchema.Builder()
              .from(schema)
              .type(Type.OBJECT_ARRAY)
              .sourcePath(Optional.empty())
              .addTransformations(
                  new ImmutablePropertyTransformation.Builder().coalesce(true).build());

      for (int i = 0; i < schema.getCoalesce().size(); i++) {
        String basePath2 =
            basePath + schema.getCoalesce().get(i).getSourcePath().map(p -> p + "/").orElse("");

        for (FeatureSchema prop : schema.getCoalesce().get(i).getProperties()) {
          builder.putPropertyMap(
              i + "_" + prop.getName(),
              new ImmutableFeatureSchema.Builder()
                  .from(prop)
                  .sourcePath(basePath2 + prop.getSourcePath().orElse(""))
                  .path(List.of(i + "_" + prop.getName())));
        }
      }

      if (schema.getCoalesce().stream().anyMatch(s -> Objects.isNull(s.getDesiredType()))) {
        builder.coalesce(
            schema.getCoalesce().stream()
                .map(
                    s -> {
                      if (Objects.isNull(s.getDesiredType())) {
                        return new ImmutableFeatureSchema.Builder()
                            .from(s)
                            .type(schema.getType())
                            .build();
                      }
                      return s;
                    })
                .collect(Collectors.toList()));
      }

      return builder.build();
    }

    return schema;
  }

  private static boolean hasMerge(FeatureSchema schema) {
    return !schema.getMerge().isEmpty();
  }

  private static boolean hasConcat(FeatureSchema schema) {
    return !schema.getConcat().isEmpty();
  }

  private static boolean hasCoalesce(FeatureSchema schema) {
    return !schema.getCoalesce().isEmpty();
  }
}
