/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import static de.ii.xtraplatform.features.domain.transform.FeaturePropertyTransformerRemove.Condition.ALWAYS;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.codelists.domain.Codelist;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.FeatureRefResolver;
import de.ii.xtraplatform.geometries.domain.GeometryType;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.AbstractMap;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public abstract class SchemaDeriver<T> implements SchemaVisitorTopDown<FeatureSchema, T> {

  private final Map<String, Codelist> codelists;

  public SchemaDeriver(Map<String, Codelist> codelists) {
    this.codelists = codelists;
  }

  @Override
  public final T visit(
      FeatureSchema schema, List<FeatureSchema> parents, List<T> visitedProperties) {
    if (parents.isEmpty()) {
      return deriveRootSchemas(schema, visitedProperties);
    }
    if (schema.isValue() || schema.isFeatureRef()) {
      return deriveValueSchema(schema);
    }

    return deriveObjectSchemas(schema, visitedProperties);
  }

  private T deriveRootSchemas(FeatureSchema schema, List<T> visitedProperties) {

    if (!schema.getConcat().isEmpty()
        && visitedProperties.size()
            == schema.getConcat().stream().mapToInt(s -> s.getProperties().size()).sum()) {
      List<T> schemas = new ArrayList<>();
      int k = 0;

      for (int i = 0; i < schema.getConcat().size(); i++) {
        List<T> visitedProperties3 = new ArrayList<>();

        if (!visitedProperties.isEmpty()) {
          for (int j = 0; j < schema.getConcat().get(i).getProperties().size(); j++) {
            visitedProperties3.add(visitedProperties.get(k++));
          }
        }

        schemas.add(deriveRootSchema(schema.getConcat().get(i), visitedProperties3));
      }

      return mergeRootSchemas(schemas);
    }

    return deriveRootSchema(schema, visitedProperties);
  }

  protected abstract T mergeRootSchemas(List<T> rootSchemas);

  private T deriveRootSchema(FeatureSchema schema, List<T> visitedProperties) {

    Map<String, T> definitions = extractDefinitions(visitedProperties);

    Map<String, T> properties =
        IntStream.range(0, visitedProperties.size())
            .mapToObj(i -> new AbstractMap.SimpleEntry<>(i, visitedProperties.get(i)))
            .map(entry -> withPropertySeq(entry.getValue(), entry.getKey()))
            .filter(property -> Objects.nonNull(property) && getPropertyName(property).isPresent())
            .map(
                property ->
                    new SimpleEntry<>(
                        getNameWithoutRole(getPropertyName(property).get()), property))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (first, second) -> first,
                    LinkedHashMap::new));

    List<String> required =
        visitedProperties.stream()
            .filter(
                property ->
                    Objects.nonNull(property)
                        && getPropertyName(property).isPresent()
                        && isPropertyRequired(property))
            .map(property -> getPropertyName(property).get())
            .collect(Collectors.toList());

    return buildRootSchema(schema, properties, definitions, required);
  }

  private Map<String, T> extractDefinitions(List<T> properties) {
    return extractDefinitions(properties.stream())
        .collect(
            Collectors.toMap(
                def -> getPropertyName(def).get(),
                def -> def,
                (first, second) -> second,
                LinkedHashMap::new));
  }

  protected Stream<T> extractDefinitions(Stream<T> properties) {
    return Stream.empty();
  }

  private T deriveObjectSchemas(FeatureSchema schema, List<T> visitedProperties) {

    if (!schema.getConcat().isEmpty()) {
      List<T> schemas = new ArrayList<>();
      int k = 0;

      for (int i = 0; i < schema.getConcat().size(); i++) {
        List<T> visitedProperties3 = new ArrayList<>();

        for (int j = 0; j < schema.getConcat().get(i).getProperties().size(); j++) {
          visitedProperties3.add(visitedProperties.get(k++));
        }

        schemas.add(deriveObjectSchema(schema.getConcat().get(i), visitedProperties3, false));
      }
      T objectSchema =
          withOneOfWrapper(
              schemas, Optional.of(schema.getName()), schema.getLabel(), schema.getDescription());
      if (schema.isArray()) {
        objectSchema = withArrayWrapper(objectSchema, true);
      }
      return objectSchema;
    }

    if (!schema.getCoalesce().isEmpty()) {
      List<T> schemas = new ArrayList<>();
      int k = 0;

      for (int i = 0; i < schema.getCoalesce().size(); i++) {
        List<T> visitedProperties3 = new ArrayList<>();

        for (int j = 0; j < schema.getCoalesce().get(i).getProperties().size(); j++) {
          visitedProperties3.add(visitedProperties.get(k++));
        }

        schemas.add(deriveObjectSchema(schema.getCoalesce().get(i), visitedProperties3, false));
      }
      T objectSchema =
          withOneOfWrapper(
              schemas, Optional.of(schema.getName()), schema.getLabel(), schema.getDescription());
      if (schema.isArray()) {
        objectSchema = withArrayWrapper(objectSchema, true);
      }
      return objectSchema;
    }

    return deriveObjectSchema(schema, visitedProperties, true);
  }

  private T deriveObjectSchema(
      FeatureSchema schema, List<T> visitedProperties, boolean arrayAllowed) {
    Map<String, T> properties =
        IntStream.range(0, visitedProperties.size())
            .mapToObj(i -> new AbstractMap.SimpleEntry<>(i, visitedProperties.get(i)))
            .filter(
                entry ->
                    Objects.nonNull(entry.getValue())
                        && getPropertyName(entry.getValue()).isPresent())
            .map(entry -> withPropertySeq(entry.getValue(), entry.getKey()))
            .map(
                property ->
                    new SimpleEntry<>(
                        getNameWithoutRole(getPropertyName(property).get()), property))
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (first, second) -> first,
                    LinkedHashMap::new));

    List<String> required =
        visitedProperties.stream()
            .filter(
                property ->
                    Objects.nonNull(property)
                        && getPropertyName(property).isPresent()
                        && isPropertyRequired(property))
            .map(property -> getPropertyName(property).get())
            .collect(Collectors.toList());

    T objectSchema = buildObjectSchema(schema, properties, required);

    String objectType = schema.getObjectType().orElse(getObjectType(schema));

    objectSchema = withRefWrapper(objectSchema, objectType);

    if (schema.isArray() && arrayAllowed) {
      objectSchema = withArrayWrapper(objectSchema, true);
    }

    if (schema.getConstraints().isPresent()) {
      objectSchema =
          withConstraints(objectSchema, schema.getConstraints().get(), schema, codelists);
    }

    if (!schema.receivable() && schema.returnable()) {
      objectSchema = withReadOnly(objectSchema);
    } else if (!schema.returnable() && schema.receivable()) {
      objectSchema = withWriteOnly(objectSchema);
    }

    return objectSchema;
  }

  protected T deriveValueSchema(FeatureSchema schema) {
    if (schema.getTransformations().stream()
        .anyMatch(t -> t.getRemove().map(v -> ALWAYS.name().equals(v)).isPresent())) {
      return null;
    }

    T valueSchema = null;
    Type propertyType = schema.getType();
    if (propertyType.equals(Type.VALUE) && schema.getValueType().isPresent()) {
      propertyType = schema.getValueType().get();
    }
    String propertyName = schema.getName();
    Optional<String> label = schema.getLabel();
    Optional<String> description = schema.getDescription();
    Optional<String> unit = schema.getUnit();
    Optional<String> role =
        schema
            .getRole()
            .filter(r -> !Role.FILTER_GEOMETRY.equals(r))
            .map(Enum::name)
            .map(r -> CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_HYPHEN, r))
            .or(() -> schema.getRefType().map(ignore -> "reference"))
            .or(() -> schema.getEmbeddedRole().map(Enum::name));
    Optional<String> refCollectionId = schema.getRefType();
    Optional<String> refUriTemplate =
        schema
            .getRefUriTemplate()
            // via temporary value to avoid URL-encoding side effects
            .map(
                template ->
                    StringTemplateFilters.applyTemplate(
                            template,
                            Map.of(FeatureRefResolver.ID, "__featureId__", "apiUri", "__apiUri__")
                                ::get)
                        .replace("__apiUri__", "{apiUri}")
                        .replace("__featureId__", "{featureId}"));
    Optional<String> codelistId = schema.getConstraints().flatMap(SchemaConstraints::getCodelist);

    switch (propertyType) {
      case VALUE:
        Set<T> valueSchemas =
            schema.getCoalesce().stream()
                .map(
                    s ->
                        getSchemaForLiteralType(
                            s.getValueType().orElse(Type.UNKNOWN),
                            s.getLabel(),
                            s.getDescription(),
                            s.getUnit(),
                            role,
                            refCollectionId,
                            refUriTemplate,
                            codelistId))
                .collect(Collectors.toSet());
        valueSchema = withOneOfWrapper(valueSchemas, Optional.of(propertyName), label, description);
        break;
      case FLOAT:
      case INTEGER:
      case STRING:
      case BOOLEAN:
      case DATETIME:
      case DATE:
        valueSchema =
            getSchemaForLiteralType(
                propertyType,
                label,
                description,
                unit,
                role,
                refCollectionId,
                refUriTemplate,
                codelistId);
        break;
      case VALUE_ARRAY:
        if (!schema.getConcat().isEmpty() && schema.getValueType().isEmpty()) {
          Set<T> valueSchemas2 =
              schema.getConcat().stream()
                  .map(
                      s ->
                          getSchemaForLiteralType(
                              s.getValueType().orElse(Type.UNKNOWN),
                              s.getLabel(),
                              s.getDescription(),
                              s.getUnit(),
                              role,
                              refCollectionId,
                              refUriTemplate,
                              codelistId))
                  .collect(Collectors.toSet());
          valueSchema = withOneOfWrapper(valueSchemas2, Optional.empty(), label, description);
        } else {
          valueSchema =
              getSchemaForLiteralType(
                  schema.getValueType().orElse(Type.UNKNOWN),
                  label,
                  description,
                  unit,
                  role,
                  refCollectionId,
                  refUriTemplate,
                  codelistId);
        }
        break;
      case GEOMETRY:
        valueSchema =
            getSchemaForGeometry(
                schema.getGeometryType().orElse(GeometryType.ANY), label, description, role);
        break;
      case OBJECT:
      case OBJECT_ARRAY:
        if (!schema.isFeatureRef()) {
          break;
        }
        if ((!schema.getConcat().isEmpty() || !schema.getCoalesce().isEmpty())
            && schema.getRefType().isEmpty()) {
          List<T> valueSchemas2 =
              Stream.concat(schema.getConcat().stream(), schema.getCoalesce().stream())
                  .map(
                      s ->
                          getSchemaForLiteralType(
                              s.getType(),
                              s.getLabel(),
                              s.getDescription(),
                              Optional.empty(),
                              Optional.of("reference"),
                              s.getRefType().or(() -> refCollectionId),
                              refUriTemplate,
                              Optional.empty()))
                  .collect(Collectors.toList());
          valueSchema = withOneOfWrapper(valueSchemas2, Optional.empty(), label, description);
        } else {
          valueSchema =
              getSchemaForLiteralType(
                  schema.getValueType().orElse(Type.UNKNOWN),
                  label,
                  description,
                  unit,
                  Optional.of("reference"),
                  refCollectionId,
                  refUriTemplate,
                  Optional.empty());
        }
        break;
      case UNKNOWN:
      default:
        break;
    }

    if (propertyType == Type.GEOMETRY) {
      valueSchema =
          withName(
              valueSchema,
              schema.isPrimaryGeometry()
                  ? getNameWithRole(Role.PRIMARY_GEOMETRY.name(), propertyName)
                  : propertyName);
    } else {
      valueSchema =
          withName(
              valueSchema,
              schema.isId() ? getNameWithRole(Role.ID.name(), propertyName) : propertyName);
    }

    if (schema.isArray()) {
      valueSchema = withArrayWrapper(valueSchema, true);
    }

    if (schema.getConstraints().isPresent()) {
      valueSchema = withConstraints(valueSchema, schema.getConstraints().get(), schema, codelists);
    }

    if (!schema.receivable() && schema.returnable()) {
      valueSchema = withReadOnly(valueSchema);
    } else if (!schema.returnable() && schema.receivable()) {
      valueSchema = withWriteOnly(valueSchema);
    }

    return valueSchema;
  }

  protected abstract Optional<String> getPropertyName(T property);

  protected abstract boolean isPropertyRequired(T property);

  protected abstract Map<String, T> getNestedProperties(T property);

  protected abstract T buildRootSchema(
      FeatureSchema schema,
      Map<String, T> properties,
      Map<String, T> definitions,
      List<String> requiredProperties);

  protected abstract T buildObjectSchema(
      FeatureSchema schema, Map<String, T> properties, List<String> requiredProperties);

  protected abstract T getSchemaForLiteralType(
      Type type,
      Optional<String> label,
      Optional<String> description,
      Optional<String> unit,
      Optional<String> role,
      Optional<String> refCollectionId,
      Optional<String> refUriTemplate,
      Optional<String> codelistId);

  protected abstract T getSchemaForGeometry(
      GeometryType geometryType,
      Optional<String> title,
      Optional<String> description,
      Optional<String> role);

  protected abstract T withPropertySeq(T schema, int propertySeq);

  protected abstract T withName(T schema, String propertyName);

  protected abstract T withRequired(T schema);

  protected abstract T withConstraints(
      T schema,
      SchemaConstraints constraints,
      FeatureSchema property,
      Map<String, Codelist> codelists);

  protected abstract T withReadOnly(T schema);

  protected abstract T withWriteOnly(T schema);

  protected abstract T withRefWrapper(T schema, String objectType);

  protected T withArrayWrapper(T schema) {
    return withArrayWrapper(schema, false);
  }

  protected abstract T withArrayWrapper(T schema, boolean moveTitleAndDescription);

  protected abstract T withOneOfWrapper(
      Collection<T> schema,
      Optional<String> name,
      Optional<String> label,
      Optional<String> description);

  protected final String getNameWithRole(String role, String propertyName) {
    return String.format("_%s_ROLE_%s", role, propertyName);
  }

  protected String getNameWithoutRole(String name) {
    int index = name.indexOf("_ROLE_");
    if (index > -1) {
      return name.substring(index + 6);
    }

    return name;
  }

  protected boolean nameHasRole(String name, String role) {
    return name.startsWith(getNameWithRole(role, ""));
  }

  protected boolean nameHasRole(String name) {
    return name.contains("_ROLE_");
  }

  protected Optional<T> findByRole(Map<String, T> properties, Role role) {
    return findByRole(properties, role.name());
  }

  protected Optional<T> findByRole(Map<String, T> properties, String role) {
    return properties.values().stream()
        .flatMap(
            property -> {
              Collection<T> nestedProperties = getNestedProperties(property).values();
              if (!nestedProperties.isEmpty()) {
                return nestedProperties.stream();
              }
              return Stream.of(property);
            })
        .filter(
            property ->
                getPropertyName(property).filter(name -> nameHasRole(name, role)).isPresent())
        .findFirst();
  }

  // TODO: nested
  protected Map<String, T> withoutRoles(Map<String, T> properties, Role... roles) {
    return properties.entrySet().stream()
        .filter(
            entry ->
                getPropertyName(entry.getValue())
                    .filter(
                        name ->
                            Arrays.stream(roles).noneMatch(role -> nameHasRole(name, role.name())))
                    .isPresent())
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // TODO: nested
  protected Map<String, T> withoutRoles(Map<String, T> properties) {
    return properties.entrySet().stream()
        .filter(
            entry ->
                getPropertyName(entry.getValue()).filter(name -> !nameHasRole(name)).isPresent())
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  public static String getObjectType(FeatureSchema schema) {
    return "type_" + Integer.toHexString(schema.hashCode());
  }
}
