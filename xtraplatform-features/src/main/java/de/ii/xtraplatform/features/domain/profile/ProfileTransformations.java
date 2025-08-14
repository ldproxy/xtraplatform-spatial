/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain.profile;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.ii.xtraplatform.features.domain.FeatureSchema;
import de.ii.xtraplatform.features.domain.SchemaBase;
import de.ii.xtraplatform.features.domain.SchemaConstraints;
import de.ii.xtraplatform.features.domain.transform.FeatureRefResolver;
import de.ii.xtraplatform.features.domain.transform.ImmutablePropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.List;
import java.util.Objects;
import javax.ws.rs.core.MediaType;
import org.immutables.value.Value;

@Value.Immutable
@Value.Style(builder = "new")
@JsonDeserialize(builder = ImmutableProfileTransformations.Builder.class)
public interface ProfileTransformations extends PropertyTransformations {

  String REL_AS_KEY = "rel-as-key";
  String REL_AS_URI = "rel-as-uri";
  String REL_AS_LINK = "rel-as-link";
  String VAL_AS_CODE = "val-as-code";
  String VAL_AS_TITLE = "val-as-title";

  String URI_TEMPLATE =
      String.format(
          "{{%s | orElse:'{{apiUri}}/collections/%s/items/%s'}}",
          FeatureRefResolver.URI_TEMPLATE, FeatureRefResolver.SUB_TYPE, FeatureRefResolver.SUB_ID);

  String KEY_TEMPLATE =
      String.format(
          "{{%s | orElse:'%s::%s'}}",
          FeatureRefResolver.KEY_TEMPLATE, FeatureRefResolver.SUB_TYPE, FeatureRefResolver.SUB_ID);

  String HTML_LINK_TEMPLATE =
      String.format("<a href=\"%s\">%s</a>", URI_TEMPLATE, FeatureRefResolver.SUB_TITLE);

  List<String> VALUES = List.of(REL_AS_KEY, REL_AS_URI, REL_AS_LINK, VAL_AS_CODE, VAL_AS_TITLE);

  static void addPredefined(
      String profileId,
      FeatureSchema schema,
      String mediaType,
      ImmutableProfileTransformations.Builder builder) {
    if (!VALUES.contains(profileId)) {
      return;
    }

    if (REL_AS_KEY.equals(profileId)
        || REL_AS_URI.equals(profileId)
        || REL_AS_LINK.equals(profileId)) {
      schema.getAllNestedProperties().stream()
          .filter(SchemaBase::isFeatureRef)
          .forEach(
              property -> {
                switch (profileId) {
                  case REL_AS_KEY:
                    reduceToKey(property, builder);
                    break;
                  case REL_AS_URI:
                    reduceToUri(property, builder);
                    break;
                  case REL_AS_LINK:
                    if (mediaType.equals(MediaType.TEXT_HTML)) {
                      reduceToLink(property, builder);
                    } else {
                      mapToLink(property, builder);
                    }
                    break;
                }
              });
    } else if (VAL_AS_TITLE.equals(profileId)) {
      schema.getAllNestedProperties().stream()
          .filter(p -> p.getConstraints().map(c -> c.getCodelist().isPresent()).orElse(false))
          .forEach(property -> mapToTitle(property, builder));
    }
  }

  static void reduceToKey(FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            property
                        .getRefType()
                        .filter(
                            refType ->
                                !Objects.equals(refType, FeatureRefResolver.REF_TYPE_DYNAMIC))
                        .isPresent()
                    && property.getRefKeyTemplate().isEmpty()
                ? new ImmutablePropertyTransformation.Builder()
                    .objectRemoveSelect(FeatureRefResolver.ID)
                    .objectReduceSelect(FeatureRefResolver.ID)
                    .build()
                : new ImmutablePropertyTransformation.Builder()
                    .objectRemoveSelect(FeatureRefResolver.ID)
                    .objectReduceFormat(KEY_TEMPLATE)
                    .build()));
  }

  static void reduceToUri(FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectReduceFormat(URI_TEMPLATE)
                .build()));
  }

  static void mapToLink(FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectMapFormat(
                    ImmutableMap.of("title", FeatureRefResolver.SUB_TITLE, "href", URI_TEMPLATE))
                .build()));
  }

  static void reduceToLink(
      FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    builder.putTransformations(
        property.getFullPathAsString(),
        ImmutableList.of(
            new ImmutablePropertyTransformation.Builder()
                .objectRemoveSelect(FeatureRefResolver.ID)
                .objectReduceFormat(HTML_LINK_TEMPLATE)
                .build()));
  }

  static void mapToTitle(FeatureSchema property, ImmutableProfileTransformations.Builder builder) {
    property
        .getConstraints()
        .flatMap(SchemaConstraints::getCodelist)
        .ifPresent(
            codelist -> {
              builder.putTransformations(
                  property.getFullPathAsString(),
                  ImmutableList.of(
                      new ImmutablePropertyTransformation.Builder().codelist(codelist).build()));
            });
  }
}
