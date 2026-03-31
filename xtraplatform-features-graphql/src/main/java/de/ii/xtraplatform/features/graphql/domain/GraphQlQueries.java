/*
 * Copyright 2023 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.graphql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.strings.domain.StringTemplateFilters;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableGraphQlQueries.Builder.class)
public interface GraphQlQueries {

  /**
   * @langEn Options for collection queries.
   * @langDe Optionen für Collection-Queries.
   * @since v3.5
   */
  CollectionQuery getCollection();

  /**
   * @langEn Options for single feature queries. If not set, a collection query will be used.
   * @langDe Optionen für Einzel-Feature-Queries. Wenn nicht gesetzt wird ein Collection-Query
   *     verwendet.
   * @since v3.5
   * @default null
   */
  Optional<SingleQuery> getSingle();

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableSingleQuery.Builder.class)
  interface SingleQuery {
    /**
     * @langEn Name of the GraphQL query.
     * @langDe Name des GraphQL-Queries.
     * @since v3.5
     */
    String getName();

    default String getName(String type) {
      return StringTemplateFilters.applyTemplate(getName(), Map.of("type", type)::get);
    }

    /**
     * @langEn Arguments for the GraphQL query.
     * @langDe Argumente für das GraphQL-Query.
     * @since v3.5
     */
    @Nullable
    QueryArgumentsSingle getArguments();

    /**
     * @langEn Subfields for specific types.
     * @langDe Subfields für spezielle Typen.
     * @since v3.5
     */
    @Nullable
    QueryFields getFields();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableCollectionQuery.Builder.class)
  interface CollectionQuery {
    /**
     * @langEn Name of the GraphQL query.
     * @langDe Name des GraphQL-Queries.
     * @since v3.5
     */
    String getName();

    default String getName(String type) {
      return StringTemplateFilters.applyTemplate(getName(), Map.of("type", type)::get);
    }

    /**
     * @langEn Arguments for the GraphQL query.
     * @langDe Argumente für das GraphQL-Query.
     * @since v3.5
     */
    @Nullable
    QueryArgumentsCollection getArguments();

    /**
     * @langEn Subfields or arguments for specific types.
     * @langDe Subfields oder Argumente für spezielle Typen.
     * @since v3.5
     */
    @Nullable
    QueryFields getFields();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableQueryArgumentsSingle.Builder.class)
  interface QueryArgumentsSingle {
    /**
     * @langEn Argument to select a feature with a specific id. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{sourcePath}}`
     *     is replaced with the name of the id property and `{{value}}` is replaced with the actual
     *     id.
     * @langDe Argument um ein Feature mit einer speziellen Id auszuwählen. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem
     *     `{{sourcePath}}` mit den Namen des Id-Property ersetzt wird und `{{value}}` mit der Id
     *     ersetzt wird.
     * @since v3.5
     */
    Optional<String> getId();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableQueryArgumentsCollection.Builder.class)
  interface QueryArgumentsCollection {

    /**
     * @langEn Argument to select a feature with a specific id. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{sourcePath}}`
     *     is replaced with the name of the id property and `{{value}}` is replaced with the actual
     *     id.
     * @langDe Argument um ein Feature mit einer speziellen Id auszuwählen. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem
     *     `{{sourcePath}}` mit den Namen des Id-Property ersetzt wird und `{{value}}` mit der Id
     *     ersetzt wird.
     * @since v3.5
     */
    Optional<String> getId();

    /**
     * @langEn Argument to limit the number of selected features. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{value}}` is
     *     replaced with the limit.
     * @langDe Argument um die Anzahl der ausgewählten Features zu beschränken. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem `{{value}}`
     *     mit dem Limit ersetzt wird.
     * @since v3.5
     * @default null
     */
    Optional<String> getLimit();

    /**
     * @langEn Argument to change the index of the first feature in the overall result set. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{value}}` is
     *     replaced with the offset.
     * @langDe Argument um den Index des ersten Features in der Gesamtergebnismenge zu ändern.
     *     [String template](../details/transformations.md#examples-for-stringformat) bei dem
     *     `{{value}}` mit dem Offset ersetzt wird.
     * @since v3.5
     * @default null
     */
    Optional<String> getOffset();

    /**
     * @langEn Argument that wraps filter expressions. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{value}}` is
     *     replaced with the filter expressions.
     * @langDe Argument das als Wrapper für Filter-Ausdrücke dient. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem `{{value}}`
     *     mit den Filter-Ausdrücken ersetzt wird.
     * @since v3.5
     * @default null
     */
    Optional<String> getFilter();

    /**
     * @langEn Argument for a bounding box filter. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{sourcePath}}`
     *     is replaced with the name of the primary spatial property and `{{value}}` is replaced
     *     with spatial value (see `geometry` below).
     * @langDe Argument für einen Bounding-Box-Filter. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem
     *     `{{sourcePath}}` mit den Namen des primären Geometrie-Property ersetzt wird und
     *     `{{value}}` mit der Geometrie (siehe `geometry` unten).
     * @since v3.5
     * @default null
     */
    Optional<String> getBbox();

    /**
     * @langEn Argument that wraps geometries in filter expressions. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{value}}` is
     *     replaced with the spatial value. A filter to convert the geometry to a textual
     *     representation is required, currently only `toWkt` is supported (`{{value \| toWkt}}`).
     * @langDe Argument das als Wrapper für Geometrien in Filter-Ausdrücken dient. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem `{{value}}`
     *     mit der Geometrie ersetzt wird. Ein Filter der die Geometrie in eine Text-Repräsentation
     *     wandelt wird benötigt, aktuell wird nur `toWkt` unterstützt (`{{value \| toWkt}}`).
     * @since v3.5
     * @default null
     */
    Optional<String> getGeometry();
  }

  @Value.Immutable
  @JsonDeserialize(builder = ImmutableQueryFields.Builder.class)
  interface QueryFields {
    /**
     * @langEn Subfield or argument for properties of type `GEOMETRY`. [String
     *     template](../details/transformations.md#examples-for-stringformat) where `{{sourcePath}}`
     *     is replaced with the name of the spatial property.
     * @langDe Subfield oder Argument für Properties vom Typ `GEOMETRY`. [String
     *     template](../details/transformations.md#examples-for-stringformat) bei dem
     *     `{{sourcePath}}` mit den Namen des räumlichen Properties ersetzt wird.
     * @since v3.5
     * @default null
     */
    Optional<String> getGeometry();
  }
}
