/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.transform;

import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema;
import de.ii.xtraplatform.features.domain.ImmutableFeatureSchema.Builder;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaBase.Role;
import de.ii.xtraplatform.features.domain.SchemaBase.Scope;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.TypesResolver;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @langEn Feature references (type `FEATURE_REF` or `FEATURE_REF_ARRAY`) are object properties with
 *     three pre-defined properties: `id`, `title`, and `type`.
 *     <p><code>
 * - `id` is the foreign key, that is the ID property of a referenced feature, and is either a `STRING` or `INTEGER`.
 * - `title` is the title to use when presenting the link to a user, a `STRING`. The default value is the `id`, if the property is not specified.
 * - `type` is the feature type of the referenced feature in the same feature provider, a `STRING`.
 * </code>
 *     <p>Limitations: These properties do not support transformations (e.g. `stringFormat`) or
 *     `coalesce`.
 *     <p>#### Encoding feature references
 *     <p>When requested via the API, the feature reference can be encoded according to different
 *     profiles using the query parameter `profile` and depending on the negotiated feature format.
 *     The following profiles are supported:
 *     <p><code>
 * - `rel-as-key`: the identifier of the feature in its collection (the `featureId`);
 * - `rel-as-uri`: the URI of the feature;
 * - `rel-as-link`: an object with two properties: `href` with the URI of the feature and `title` with a title of the feature.
 * </code>
 *     <p>The `rel-as-link` profile is typically not supported for feature formats that do not
 *     support object properties, e.g., CSV or FlatGeobuf. In HTML, a link is encoded as an `<a>`
 *     element, in GML using `XLink` attributes.
 *     <p>The profile is negotiated based on the requested profile (default is `rel-as-link`) and
 *     the supported profiles of the negotiated format based on the `Accept` header and the query
 *     parameter `f`.
 *     <p>#### Configuration
 *     <p>##### Simple Case
 *     <p>If the default value of `title` (that is, the `id`) is sufficient and the target features
 *     are in the same API and all in the same collection, the properties of the object do not need
 *     to be specified in the schema. It is sufficient to specify the following configuration
 *     property:
 *     <p><code>
 * - `sourcePath`: The value with the `id` of the referenced feature.
 * - `type`: The type of the `id`, either `STRING` (the default) or `INTEGER`.
 * - `refType`: the identifier of the feature type / collection of the referenced feature.
 * </code>
 *     <p>In the following example, the `abs` column is the foreign key of the referenced feature in
 *     the `abschnitteaeste` feature type:
 *     <p><code>
 * ```yaml
 * abs:
 *   sourcePath: abs
 *   type: FEATURE_REF
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   refType: abschnitteaeste
 * ```
 * </code>
 *     <p>In the next example, there are two columns (`abs` and `ast`) which are foreign keys of the
 *     referenced feature in the `abschnitte` or `aeste` feature type. Only one of the two values is
 *     set and the first value that is not `null` is used:
 *     <p><code>
 * ```yaml
 * abs:
 *   type: FEATURE_REF
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   coalesce:
 *   - sourcePath: abs
 *     refType: abschnitte
 *   - sourcePath: ast
 *     refType: aeste
 * ```
 * </code>
 *     <p>##### Advanced Cases
 *     <p>If the `title` should differ from `id`, if the type of the referenced feature is
 *     determined from the data, or if the referenced resource is outside of the API, the properties
 *     of the feature reference are explicitly specified in the feature schema.
 *     <p>Example:
 *     <p><code>
 * ```yaml
 * unfaelle:
 *   sourcePath: "[abs=abs]unfaelle_point"
 *   type: FEATURE_REF_ARRAY
 *   label: Unfälle
 *   description: Unfälle auf dem Abschnitt oder Ast
 *   properties:
 *     id:
 *       type: INTEGER
 *       sourcePath: fid
 *     title:
 *       type: STRING
 *       sourcePath: unfzeit
 *     type:
 *       type: STRING
 *       constantValue: unfaelle
 * ```
 * </code>
 *     <p>In addition, the following configuration options can be specified:
 *     <p><code>
 * - `refKeyTemplate`: the string template for the value in the `rel-as-key` profile. Parameters are `id` and `type`. The default is `{{id}}`, if `type` is constant, otherwise `{{type}}::{{id}}`.
 * - `refUriTemplate`: the string template of the URI of the referenced feature. Parameters are `id`, `type`, and `apiUri` (the URI of the landing page of the API). The default is `{{apiUri}}/collections/{{type}}/items/{{id}}`.
 * </code>
 *     <p>Example:
 *     <p><code>
 * ```yaml
 * externalReferences:
 *   sourcePath: "[fk=oid]externalref"
 *   type: FEATURE_REF_ARRAY
 *   label: External References
 *   refUriTemplate: "https://example.com/foo/bar/{{type}}/{{id}}"
 *   refKeyTemplate: "{{type}}_{{id}}"
 *   properties:
 *   id:
 *     type: INTEGER
 *     sourcePath: oid
 *   title:
 *     type: STRING
 *     sourcePath: label
 *   type:
 *     type: STRING
 *     sourcePath: type
 * ```
 * </code>
 * @langDe Objektreferenzen (Typ ist `FEATURE_REF` or `FEATURE_REF_ARRAY`) sind objektwertige
 *     Eigenschaften mit drei vordefinierten Eigenschaften: `id`, `title` und `type`.
 *     <p><code>
 * - `id` ist der Fremdschlüssel, d.h. die ID-Eigenschaft eines referenzierten Features. Typ ist entweder ein `STRING` oder `INTEGER`.
 * - `title` ist die Bezeichnung, die verwendet wird, wenn der Link einem Benutzer angezeigt wird, ein `STRING`. Der Standardwert ist die `id`, wenn die Eigenschaft nicht angegeben wird.
 * - `type` ist die Objektart des referenzierten Features im selben Feature Provider, ein `STRING`.
 * </code>
 *     <p>Einschränkungen: Diese Eigenschaften unterstützen keine Transformationen (z.B.
 *     `stringFormat`) oder `coalesce`.
 *     <p>#### Kodieren von Objektreferenzen
 *     <p>Wenn Objekte über die API angefordert werden, können Objektreferenzen mit Hilfe des
 *     Abfrageparameters `profile` nach verschiedenen Profilen kodiert werden, je nach dem
 *     ausgehandelten Datenformat. Folgende Profile werden unterstützt:
 *     <p><code>
 * - `rel-as-key`: Die Kennung des Objekts in seiner Collection (die `featureId`);
 * - `rel-as-uri`: Die URI des Objekts;
 * - `rel-as-link`: Ein Objekt mit zwei Eigenschaften: `href` mit dem URI des Objekts und `title` mit einer Bezeichnung des Objekts.
 * </code>
 *     <p>Das Profil `rel-as-link` wird in der Regel für Datenformate nicht unterstützt, die keine
 *     objektwertigen Eigenschaften unterstützen, z.B. CSV oder FlatGeobuf. In HTML wird ein Link
 *     als `<a>` Element kodiert, in GML mit `XLink` Attributen.
 *     <p>Das Profil wird auf der Grundlage des angeforderten Profils (Standard ist `rel-as-link`)
 *     und der unterstützten Profile des ausgehandelten Datenformats auf der Grundlage des
 *     `Accept`-Headers und des Abfrageparameters `f` ausgehandelt.
 *     <p>#### Konfiguration
 *     <p>##### Einfacher Fall
 *     <p>Wenn der Standardwert von `title` (d.h. die `id`) ausreicht und die Zielobjekte in
 *     derselben API und alle in derselben Collection sind, müssen die Eigenschaften des
 *     FEATURE_REF-Objekts nicht im Schema angegeben werden. Es genügt, die folgende
 *     Konfigurationseigenschaften anzugeben:
 *     <p><code>
 * - `sourcePath`: Der Wert mit der `id` des referenzierten Objekts.
 * - `type`: Der Typ der `id`, entweder `STRING` (der Standard) oder `INTEGER`.
 * - `refType`: Die Kennung der Objektart / Collection des referenzierten Objekts.
 * </code>
 *     <p>Im folgenden Beispiel ist die Spalte `abs` der Fremdschlüssel des referenzierten Objekts
 *     in der Objektart `abschnitteaeste`:
 *     <p><code>
 * ```yaml
 * abs:
 *   sourcePath: abs
 *   type: FEATURE_REF
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   refType: abschnitteaeste
 * ```
 * </code>
 *     <p>Im nächsten Beispiel gibt es zwei Spalten (`abs` und `ast`), die Fremdschlüssel des
 *     referenzierten Features im Objekttyp `abschnitte` oder `aeste` sind. Nur einer der beiden
 *     Werte wird gesetzt und der erste Wert, der nicht `null` ist, wird verwendet:
 *     <p><code>
 * ```yaml
 * abs:
 *   type: FEATURE_REF
 *   label: Abschnitt/Ast
 *   description: 16-stellige Kennung des Abschnittes oder Astes
 *   coalesce:
 *   - sourcePath: abs
 *     refType: abschnitte
 *   - sourcePath: ast
 *     refType: aeste
 * ```
 * </code>
 *     <p>##### Fortgeschrittene Fälle
 *     <p>Wenn sich der `title` von der `id` unterscheiden soll, wenn der Typ des referenzierten
 *     Objekts aus den Daten bestimmt wird oder wenn die referenzierte Ressource außerhalb der API
 *     liegt, werden die Eigenschaften der Objektreferenz explizit im Feature-Schema angegeben.
 *     <p>Example:
 *     <p><code>
 * ```yaml
 * unfaelle:
 *   sourcePath: "[abs=abs]unfaelle_point"
 *   type: FEATURE_REF_ARRAY
 *   label: Unfälle
 *   description: Unfälle auf dem Abschnitt oder Ast
 *   properties:
 *     id:
 *       type: INTEGER
 *       sourcePath: fid
 *     title:
 *       type: STRING
 *       sourcePath: unfzeit
 *     type:
 *       type: STRING
 *       constantValue: unfaelle
 * ```
 * </code>
 *     <p>Zusätzlich können die folgenden Konfigurationsoptionen angegeben werden:
 *     <p><code>
 * - `refKeyTemplate`: Das String-Template für den Wert im `rel-as-key` Profil. Parameter sind `id` und `type`. Der Standardwert ist `{{id}}`, wenn `type` konstant ist, sonst `{{type}}::{{id}}`.
 * - `refUriTemplate`: Das String-Template der URI des referenzierten Features. Parameter sind `id`, `type` und `apiUri` (die URI der Landing Page der API). Der Standardwert ist `{{apiUri}}/collections/{{type}}/items/{{id}}`.
 * </code>
 *     <p>Beispiel:
 *     <p><code>
 * ```yaml
 * externalReferences:
 *   sourcePath: "[fk=oid]externalref"
 *   type: FEATURE_REF_ARRAY
 *   label: External References
 *   refUriTemplate: "https://example.com/foo/bar/{{type}}/{{id}}"
 *   refKeyTemplate: "{{type}}_{{id}}"
 *   properties:
 *   id:
 *     type: INTEGER
 *     sourcePath: oid
 *   title:
 *     type: STRING
 *     sourcePath: label
 *   type:
 *     type: STRING
 *     sourcePath: type
 * ```
 * </code>
 */
public class FeatureRefResolver implements TypesResolver {

  public static final String ID = "id";
  public static final String TYPE = "type";
  public static final String TITLE = "title";
  public static final String QUERYABLE = "queryable";
  public static final String URI_TEMPLATE = "uriTemplate";
  public static final String KEY_TEMPLATE = "keyTemplate";
  public static final String SUB_ID = "{{id}}";
  public static final String SUB_TYPE = "{{type}}";
  public static final String SUB_TITLE = "{{title}}";
  public static final String SUB_URI_TEMPLATE = "{{uriTemplate}}";
  public static final String SUB_KEY_TEMPLATE = "{{keyTemplate}}";
  public static final String REF_TYPE_DYNAMIC = "DYNAMIC";

  private final Set<String> connectors;

  public FeatureRefResolver(Set<String> connectors) {
    this.connectors = connectors.stream().map(c -> "[" + c + "]").collect(Collectors.toSet());
  }

  @Override
  public boolean needsResolving(
      FeatureSchema property, boolean isFeature, boolean isInConcat, boolean isInCoalesce) {
    return property.getType() == Type.FEATURE_REF
        || property.getType() == Type.FEATURE_REF_ARRAY
        || property.getProperties().stream().anyMatch(SchemaBase::isFeatureRef);
  }

  @Override
  public FeatureSchema resolve(FeatureSchema schema, List<FeatureSchema> parents) {
    if (schema.isFeatureRef()) {
      if (!schema.getConcat().isEmpty() || !schema.getCoalesce().isEmpty()) {
        return new Builder()
            .from(schema)
            .type(schema.isArray() ? Type.OBJECT_ARRAY : Type.OBJECT)
            .refType(schema.getRefType().orElse(REF_TYPE_DYNAMIC))
            .build();
      }

      return resolve(schema, schema.getProperties(), Optional.empty(), Optional.empty());
    }

    if (schema.getProperties().stream().anyMatch(SchemaBase::isFeatureRef)) {
      Map<String, FeatureSchema> visitedPropertiesMap =
          asMap(schema.getProperties().stream().flatMap(addQueryableDuplicateIfNecessary()));

      return new ImmutableFeatureSchema.Builder()
          .from(schema)
          .propertyMap(visitedPropertiesMap)
          .build();
    }

    return schema;
  }

  private boolean isConnected(String sourcePath) {
    return connectors.stream().anyMatch(sourcePath::contains);
  }

  private boolean isConnected(Optional<String> sourcePath) {
    return sourcePath.isPresent() && isConnected(sourcePath.get());
  }

  private static Function<FeatureSchema, Stream<? extends FeatureSchema>>
      addQueryableDuplicateIfNecessary() {
    return property -> {
      if (property.isFeatureRef()
          && (isStatic(property.getRefType())
              || property.getProperties().stream()
                  .anyMatch(p -> p.getName().equals("type") && p.getSourcePath().isPresent()))) {
        Optional<FeatureSchema> idProperty =
            property.getProperties().stream()
                .filter(Objects::nonNull)
                .filter(p -> Objects.equals(p.getName(), FeatureRefResolver.ID))
                .findFirst();
        if (idProperty.isPresent()) {
          return Stream.of(
              property,
              new Builder()
                  .name(property.getName() + "_" + QUERYABLE)
                  .addPath(property.getPath().get(property.getPath().size() - 1) + "_" + QUERYABLE)
                  .parentPath(property.getParentPath())
                  .type(property.isArray() ? Type.VALUE_ARRAY : Type.VALUE)
                  .valueType(idProperty.get().getType())
                  .refType(property.getRefType().get())
                  .label(property.getLabel())
                  .description(property.getDescription())
                  .sourcePath(
                      property.getSourcePath().map(s -> s + "/").orElse("")
                          + idProperty.get().getSourcePath().orElse(""))
                  .excludedScopes(property.getExcludedScopes())
                  .addAllExcludedScopes(Scope.allBut(Scope.QUERYABLE, Scope.SORTABLE))
                  .addAllTransformations(
                      idProperty.map(FeatureSchema::getTransformations).orElse(List.of()))
                  .addTransformations(
                      new ImmutablePropertyTransformation.Builder()
                          .rename(property.getName())
                          .build())
                  .build());
        }
      }
      return Stream.of(property);
    };
  }

  public List<FeatureSchema> resolveAll(
      List<FeatureSchema> schemas,
      Optional<Type> fallbackValueType,
      Optional<String> fallbackRefType) {
    return schemas.stream()
        .map(schema -> resolve(schema, schema.getProperties(), fallbackValueType, fallbackRefType))
        .collect(Collectors.toList());
  }

  public FeatureSchema resolve(
      FeatureSchema schema,
      List<FeatureSchema> properties,
      Optional<Type> fallbackValueType,
      Optional<String> fallbackRefType) {
    Type valueType = schema.getValueType().orElse(fallbackValueType.orElse(Type.STRING));
    Optional<String> refType = schema.getRefType().or(() -> fallbackRefType);
    boolean isStatic = isStatic(refType);
    List<Scope> excludedScopes = isStatic ? List.of(Scope.QUERYABLE, Scope.SORTABLE) : List.of();

    if (properties.isEmpty() && schema.getSourcePath().isEmpty()) {
      return new Builder()
          .from(schema)
          .type(schema.isArray() ? Type.OBJECT_ARRAY : Type.OBJECT)
          .valueType(Optional.empty())
          .role(Role.FEATURE_REF)
          .build();
    }

    if (properties.isEmpty()) {
      String sourcePath = schema.getSourcePath().orElse("");
      Optional<String> objectSourcePath =
          sourcePath.contains("/")
              ? Optional.of(sourcePath.substring(0, sourcePath.lastIndexOf('/')))
              : Optional.empty();
      String idSourcePath =
          sourcePath.contains("/")
              ? sourcePath.substring(sourcePath.lastIndexOf('/') + 1)
              : sourcePath;

      Builder builder =
          new Builder()
              .from(schema)
              .type(schema.isArray() ? Type.OBJECT_ARRAY : Type.OBJECT)
              .valueType(Optional.empty())
              .role(Role.FEATURE_REF)
              .sourcePath(objectSourcePath);

      if (objectSourcePath.isPresent() && isConnected(objectSourcePath.get())) {
        builder
            .sourcePath(Optional.empty())
            .addTransformations(
                new ImmutablePropertyTransformation.Builder()
                    .objectMapDuplicate(Map.of(TITLE, ID))
                    .build())
            .addTransformations(
                new ImmutablePropertyTransformation.Builder()
                    .objectAddConstants(Map.of(TYPE, refType.orElse("")))
                    .build())
            .putProperties2(
                ID,
                new Builder()
                    .type(valueType)
                    .sourcePath(sourcePath)
                    .excludedScopes(excludedScopes));

        if (schema.getRefUriTemplate().isPresent()) {
          builder.addTransformations(
              new ImmutablePropertyTransformation.Builder()
                  .objectAddConstants(Map.of(URI_TEMPLATE, schema.getRefUriTemplate().get()))
                  .build());
        }
        if (schema.getRefKeyTemplate().isPresent()) {
          builder.addTransformations(
              new ImmutablePropertyTransformation.Builder()
                  .objectAddConstants(Map.of(KEY_TEMPLATE, schema.getRefKeyTemplate().get()))
                  .build());
        }
      } else {
        builder
            .putProperties2(
                ID,
                new Builder()
                    .type(valueType)
                    .sourcePath(idSourcePath)
                    .excludedScopes(excludedScopes))
            .putProperties2(
                TITLE,
                new Builder()
                    .type(Type.STRING)
                    .sourcePath(idSourcePath)
                    .excludedScopes(excludedScopes))
            .putProperties2(
                TYPE,
                new Builder()
                    .type(Type.STRING)
                    .constantValue(refType)
                    .excludedScopes(excludedScopes));

        if (schema.getRefUriTemplate().isPresent()) {
          builder.putProperties2(
              URI_TEMPLATE,
              new Builder().type(Type.STRING).constantValue(schema.getRefUriTemplate()));
        }
        if (schema.getRefKeyTemplate().isPresent()) {
          builder.putProperties2(
              KEY_TEMPLATE,
              new Builder().type(Type.STRING).constantValue(schema.getRefKeyTemplate()));
        }
      }

      return builder.build();
    }

    List<FeatureSchema> newVisitedProperties =
        new ArrayList<>(
            properties.stream()
                .map(
                    property -> new Builder().from(property).excludedScopes(excludedScopes).build())
                .collect(Collectors.toList()));
    List<PropertyTransformation> newTransformations = new ArrayList<>(schema.getTransformations());

    if (properties.stream().noneMatch(schema1 -> Objects.equals(schema1.getName(), TITLE))) {
      if (isConnected(schema.getSourcePath())) {
        newTransformations.add(
            new ImmutablePropertyTransformation.Builder()
                .objectMapDuplicate(Map.of(TITLE, ID))
                .build());
      } else {
        FeatureSchema idSchema =
            properties.stream()
                .filter(schema1 -> Objects.equals(schema1.getName(), ID))
                .findFirst()
                .orElseThrow();

        newVisitedProperties.add(
            new Builder()
                .from(idSchema)
                .name(TITLE)
                .type(Type.STRING)
                .path(List.of(TITLE))
                .excludedScopes(excludedScopes)
                .build());
      }
    }

    if (properties.stream().noneMatch(schema1 -> Objects.equals(schema1.getName(), TYPE))
        && schema.getRefType().isPresent()) {
      if (isConnected(schema.getSourcePath())) {
        newTransformations.add(
            new ImmutablePropertyTransformation.Builder()
                .objectAddConstants(Map.of(TYPE, refType.orElse("")))
                .build());
      } else {
        newVisitedProperties.add(
            new Builder()
                .name(TYPE)
                .type(Type.STRING)
                .path(List.of(TYPE))
                .parentPath(schema.getPath())
                .constantValue(refType)
                .excludedScopes(excludedScopes)
                .build());
      }
    }
    if (schema.getRefUriTemplate().isPresent()) {
      if (isConnected(schema.getSourcePath())) {
        newTransformations.add(
            new ImmutablePropertyTransformation.Builder()
                .objectAddConstants(Map.of(URI_TEMPLATE, schema.getRefUriTemplate().get()))
                .build());
      } else {
        newVisitedProperties.add(
            new Builder()
                .name(URI_TEMPLATE)
                .type(Type.STRING)
                .path(List.of(URI_TEMPLATE))
                .parentPath(schema.getPath())
                .constantValue(schema.getRefUriTemplate())
                .excludedScopes(excludedScopes)
                .build());
      }
    }
    if (schema.getRefKeyTemplate().isPresent()) {
      if (isConnected(schema.getSourcePath())) {
        newTransformations.add(
            new ImmutablePropertyTransformation.Builder()
                .objectAddConstants(Map.of(KEY_TEMPLATE, schema.getRefKeyTemplate().get()))
                .build());
      } else {
        newVisitedProperties.add(
            new Builder()
                .name(KEY_TEMPLATE)
                .type(Type.STRING)
                .path(List.of(KEY_TEMPLATE))
                .parentPath(schema.getPath())
                .constantValue(schema.getRefKeyTemplate())
                .excludedScopes(excludedScopes)
                .build());
      }
    }

    return new ImmutableFeatureSchema.Builder()
        .from(schema)
        .type(schema.isArray() ? Type.OBJECT_ARRAY : Type.OBJECT)
        .refType(refType.orElse(REF_TYPE_DYNAMIC))
        .role(Role.FEATURE_REF)
        .propertyMap(asMap(newVisitedProperties.stream()))
        .transformations(newTransformations)
        .build();
  }

  private static boolean isStatic(Optional<String> refType) {
    return refType.filter(refType2 -> !Objects.equals(refType2, REF_TYPE_DYNAMIC)).isPresent();
  }
}
