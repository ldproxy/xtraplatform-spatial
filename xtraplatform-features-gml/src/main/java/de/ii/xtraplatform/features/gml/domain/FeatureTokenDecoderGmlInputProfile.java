/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.gml.domain;

import de.ii.xtraplatform.crs.domain.EpsgCrs;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.immutables.value.Value;

/**
 * Reverse profile of the GML output configuration, consumed by the GML decoders on the input path.
 * Mirrors the encoding-time options from {@code GmlConfiguration} so that input documents shaped
 * like the encoder's output can be decoded losslessly. Defaults are conservative empties; an empty
 * profile means the decoder runs without any reverse-mapping behaviour.
 */
@Value.Immutable
public interface FeatureTokenDecoderGmlInputProfile {

  /**
   * Reverse-mapping from the {@code srsName} value seen on a geometry to the resolved {@link
   * EpsgCrs}. Required for input shaped after ALKIS NAS, which uses ADV URN forms such as {@code
   * urn:adv:crs:DE_DHDN_3GK2_NW101} that the built-in EPSG / OGC URN parser cannot resolve.
   */
  Map<String, EpsgCrs> getSrsNameMappings();

  /**
   * Per wire {@code srsName}, the difference between the false easting of the mapped CRS and the
   * false easting used by coordinates carrying that srsName (e.g. 3000000 for German Gauss-Krüger
   * coordinates written without the zone prefix, mapped to a zone-prefixed EPSG CRS). Added to the
   * easting (first ordinate) of every decoded position so the emitted coordinates conform to the
   * mapped CRS; the encoder subtracts it on output. Only srsNames with a non-zero difference are
   * present.
   */
  Map<String, Double> getSrsNameFalseEastingDifferences();

  /**
   * Optional prefix stripped from the value of {@code gml:id} before it is emitted as the feature
   * id token. Empty string means no prefix is stripped.
   */
  @Value.Default
  default String getGmlIdPrefix() {
    return "";
  }

  /**
   * Path-keyed codelist declarations as carried by {@code GmlConfiguration#codelistProperties}.
   * Routing of {@code xlink:href} on codelist properties is driven by the {@code FeatureSchema}'s
   * own {@code codelist} constraint (analogous to the feature-reference routing driven by the
   * schema's {@code FEATURE_REF}/{@code FEATURE_REF_ARRAY} type); this map is kept for completeness
   * and future routing decisions that need the path-level mapping.
   */
  Map<String, String> getCodelistProperties();

  /**
   * Reverse of {@code GmlConfiguration#featureRefTemplate}. When set, an incoming {@code
   * xlink:href} on a feature-reference property is matched against the template and reduced to the
   * bare {@code {{value}}} segment before being emitted as the property value. The template uses
   * {@code {{value}}} as the placeholder; everything else is treated literally (e.g. {@code
   * urn:adv:oid:{{value}}} on {@code urn:adv:oid:DENW36ALl800005x} yields {@code
   * DENW36ALl800005x}). When unset, the href is emitted unchanged.
   */
  @Value.Default
  default String getFeatureRefTemplate() {
    return "";
  }

  /**
   * Reverse of {@code GmlConfiguration#codelistUriTemplate}. When set, an incoming {@code
   * xlink:href} on a codelist-valued property is matched against the template (with the schema's
   * codelist id substituted for {@code {{codelistId}}}) and reduced to the bare {@code {{value}}}
   * segment before being emitted. When unset, the href is emitted unchanged.
   */
  @Value.Default
  default String getCodelistUriTemplate() {
    return "";
  }

  /**
   * Reverse of {@code GmlConfiguration#uomMappings} combined with {@code UomStyle.TEMPLATE}: keys
   * are the wire-form {@code uom} attribute values written by the encoder; values are the canonical
   * units (matching the {@code unit} declared on the property in the provider schema). The {@code
   * uom} attribute itself is not mapped to the feature representation — the canonical unit lives in
   * the schema. The decoder reverse-maps the wire value through this map and warns when the result
   * does not match the schema's {@code unit}; when no entry matches, the wire value is compared
   * directly.
   */
  Map<String, String> getUomMappings();

  /**
   * When {@code true}, the decoder matches an incoming XML element local name against each schema
   * property's {@code alias} (falling back to the property name if no alias is set) instead of
   * matching against the property name directly. Mirrors the encoder's {@code
   * GmlConfiguration#useAlias} flag: the encoder writes the alias as the element local name when
   * this is on, so the decoder must look it up the same way.
   */
  @Value.Default
  default boolean getUseAlias() {
    return false;
  }

  /**
   * Reverse of {@code GmlConfiguration#applicationNamespaces}: prefix → URI map of the
   * application-defined namespaces the encoder writes into the output. Currently informational —
   * the decoder resolves prefixes through the namespace map passed to its constructor (which merges
   * predefined namespaces with these); kept here so the input profile carries the full encoder-side
   * configuration.
   */
  Map<String, String> getApplicationNamespaces();

  /**
   * Reverse of {@code GmlConfiguration#defaultNamespace}: the prefix declared as the default
   * namespace by the encoder. When set, property elements without an explicit {@code prefix:} in
   * the schema name/alias and without a {@code objectTypeNamespaces} mapping are expected on the
   * wire in this prefix's namespace; the decoder rejects mismatching elements.
   */
  @Value.Default
  default String getDefaultNamespace() {
    return "";
  }

  /**
   * Reverse of {@code GmlConfiguration#objectTypeNamespaces}: maps an object type's name (the
   * schema's {@code objectType} value, including the feature type) to the prefix the encoder uses
   * for its GML object element <em>and</em> its property elements. The decoder enforces the mapped
   * prefix's namespace URI on those property elements; properties with an explicit {@code prefix:}
   * in the schema name/alias override the mapping.
   */
  Map<String, String> getObjectTypeNamespaces();

  /**
   * Reverse of {@code GmlConfiguration#variableObjectElementNames}: maps an object type's name (the
   * schema's {@code objectType} value) to the wire-element-name → source-value mapping the encoder
   * applied. When the wire element of the feature root matches one of the configured qualified
   * names for the feature schema's {@code objectType}, the decoder accepts the element as the
   * feature type and emits the mapped source value at {@link VariableObjectName#getProperty
   * VariableObjectName#getProperty()}.
   */
  Map<String, VariableObjectName> getVariableObjectElementNames();

  /**
   * Reverse of {@code GmlConfiguration#featureCollectionElementName}. When set, the decoder accepts
   * a document whose root element is the configured wrapper (e.g. {@code sf:FeatureCollection}) and
   * descends through it to the feature element instead of treating the wrapper as a feature type.
   * The value must be in the form {@code prefix:localName}; the prefix is resolved against the
   * namespace map passed to the decoder's constructor. When unset, the document root must directly
   * be the feature element. Only a single feature inside the wrapper is supported — a second
   * feature sibling is rejected as multi-feature ingest.
   */
  @Value.Default
  default String getFeatureCollectionElementName() {
    return "";
  }

  /**
   * Reverse of {@code GmlConfiguration#featureMemberElementName}. When set, the decoder expects the
   * configured member element (e.g. {@code sf:featureMember}) as the child of the feature
   * collection wrapper (or as the document root, when only this option is configured) and descends
   * through it to the feature element. Same {@code prefix:localName} resolution as {@link
   * #getFeatureCollectionElementName()}.
   */
  @Value.Default
  default String getFeatureMemberElementName() {
    return "";
  }

  List<String> getXmlAttributes();

  Map<String, List<String>> getValueWrap();

  /**
   * Reverse of {@code GmlConfiguration#objectTypeSuffixedProperties}: the property id (technical
   * full path) of each FEATURE_REF property whose GML element on the wire is the base property
   * name/alias plus a {@code _<ObjectType>} suffix naming the referenced feature type (e.g. base
   * element {@code gehoertZuBauwerk} → {@code gehoertZuBauwerk_AX_Turm}). Membership is tested
   * against the property's technical full path, not its on-the-wire name/alias. For these
   * properties the decoder accepts an element whose local name is the base name/alias optionally
   * followed by a {@code _<segment>} suffix and maps it to the property. The suffix is ignored: the
   * referenced object type is carried independently through the FEATURE_REF join, so it need not be
   * captured from the wire.
   */
  Set<String> getObjectTypeSuffixedProperties();

  /**
   * Reverse of {@code GmlConfiguration#positionVariants}: per geometry property — keyed by the
   * property's technical full path; the alias-form path is honored as well, mirroring {@link
   * #getValueWrap()} — the routing of positions in non-native CRSs to CRS-specific sibling
   * properties. See {@link GmlGeometryVariants}.
   */
  Map<String, GmlGeometryVariants> getGeometryVariants();

  static FeatureTokenDecoderGmlInputProfile empty() {
    return ImmutableFeatureTokenDecoderGmlInputProfile.builder().build();
  }
}
