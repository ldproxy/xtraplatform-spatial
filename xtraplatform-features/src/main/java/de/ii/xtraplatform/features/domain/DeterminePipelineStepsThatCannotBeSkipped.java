/*
 * Copyright 2025 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.google.common.collect.ImmutableSet;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.crs.domain.OgcCrs;
import de.ii.xtraplatform.features.domain.FeatureStream.PipelineSteps;
import de.ii.xtraplatform.features.domain.SchemaBase.Type;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformation;
import de.ii.xtraplatform.features.domain.transform.PropertyTransformations;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

public class DeterminePipelineStepsThatCannotBeSkipped
    implements SchemaVisitorTopDown<FeatureSchema, Set<PipelineSteps>> {

  private final EpsgCrs nativeCrs;
  private final EpsgCrs targetCrs;
  private final TypeQuery query;
  private final Optional<PropertyTransformations> propertyTransformations;
  private final boolean simplifyGeometries;
  private final boolean deriveMetadataFromContent;
  private final boolean requiresPropertiesInSequence;
  private final boolean supportSecondaryGeometry;
  private final boolean distinguishNullAndMissing;
  private final String featureType;

  public DeterminePipelineStepsThatCannotBeSkipped(
      TypeQuery query,
      String featureType,
      Optional<PropertyTransformations> propertyTransformations,
      EpsgCrs nativeCrs,
      EpsgCrs targetCrs,
      boolean deriveMetadataFromContent,
      boolean requiresPropertiesInSequence,
      boolean supportSecondaryGeometry,
      boolean distinguishNullAndMissing,
      boolean simplifyGeometries) {
    this.query = query;
    this.propertyTransformations = propertyTransformations;
    this.nativeCrs = nativeCrs;
    this.targetCrs = targetCrs;
    this.deriveMetadataFromContent = deriveMetadataFromContent;
    this.requiresPropertiesInSequence = requiresPropertiesInSequence;
    this.supportSecondaryGeometry = supportSecondaryGeometry;
    this.distinguishNullAndMissing = distinguishNullAndMissing;
    this.featureType = featureType;
    this.simplifyGeometries = simplifyGeometries;
  }

  @Override
  public Set<PipelineSteps> visit(
      FeatureSchema schema,
      List<FeatureSchema> parents,
      List<Set<PipelineSteps>> visitedProperties) {
    ImmutableSet.Builder<PipelineSteps> steps = ImmutableSet.builder();

    if (parents.isEmpty()) {
      // at the root level: aggregate information from properties and test global settings

      // coordinate processing is needed if a target CRS differs from the native CRS or geometries
      // are simplified
      if (!targetCrs.equals(nativeCrs)
          || (simplifyGeometries)
          || (!(OgcCrs.CRS84.equals(nativeCrs) || OgcCrs.CRS84h.equals(nativeCrs))
              && supportSecondaryGeometry
              && schema.isSecondaryGeometry())) {
        steps.add(PipelineSteps.COORDINATES);
      }

      // metadata processing (extents, etag) is needed only if the response is not sent as a stream
      if (deriveMetadataFromContent) {
        steps.add(PipelineSteps.METADATA, PipelineSteps.ETAG);
      }

      // aggregate information from visited properties
      visitedProperties.forEach(steps::addAll);

      // post-process special cases
      Set<PipelineSteps> intermediateResult = steps.build();

      // include transformations from the feature provider as in the feature stream
      PropertyTransformations mergedTransformations =
          FeatureStreamImpl.getPropertyTransformations(
              Map.of(featureType, schema), query, propertyTransformations);

      // if null values are not removed, cleaning is not needed
      if (intermediateResult.contains(PipelineSteps.CLEAN)
          && (mergedTransformations.hasTransformation(
                  PropertyTransformations.WILDCARD, pt -> !pt.getRemoveNullValues().orElse(true))
              || !distinguishNullAndMissing)) {
        steps = ImmutableSet.builder();
        intermediateResult.stream().filter(s -> s != PipelineSteps.CLEAN).forEach(steps::add);
      }

      // mapping is also needed, if specific property transformations are applied (the ones with a
      // wildcard are handled otherwise: nulls are removed in the CLEAN step and flattening is
      // already handled by including MAPPING for any objects or arrays);
      // if only value transformations are applied, and no other mapping is needed, just execute
      // the value transformations, but skip schema transformations and token slice transformers
      if (!intermediateResult.contains(PipelineSteps.MAPPING_SCHEMA)) {
        if (requiresPropertiesInSequence) {
          steps.add(PipelineSteps.MAPPING_SCHEMA);
          steps.add(PipelineSteps.MAPPING_VALUES);
        } else if (mergedTransformations.getTransformations().entrySet().stream()
            .filter(entry -> !PropertyTransformations.WILDCARD.equals(entry.getKey()))
            .map(Entry::getValue)
            .flatMap(Collection::stream)
            .allMatch(PropertyTransformation::onlyValueTransformations)) {
          steps.add(PipelineSteps.MAPPING_VALUES);
        } else if (mergedTransformations.getTransformations().entrySet().stream()
            .anyMatch(entry -> !PropertyTransformations.WILDCARD.equals(entry.getKey()))) {
          steps.add(PipelineSteps.MAPPING_SCHEMA);
          steps.add(PipelineSteps.MAPPING_VALUES);
        }
      } else {
        steps.add(PipelineSteps.MAPPING_VALUES);
      }

    } else {
      // at property level: determine needed steps based on schema information

      // mapping is needed for any complex schema: concat/coalesce/merge, an array/object, or use of
      // a sub-decoder
      if (!schema.getConcat().isEmpty()
          || !schema.getCoalesce().isEmpty()
          || !schema.getMerge().isEmpty()
          || schema.isArray()
          || schema.isObject()
          || schema
              .getSourcePath()
              .filter(sourcePath -> sourcePath.matches(".+?\\[[^=\\]]+].+"))
              .isPresent()) {
        steps.add(PipelineSteps.MAPPING_SCHEMA);
      }

      // geometry processing is needed for geometries with constraints that require special handling
      // to upgrade the geometry type
      if (schema.getType() == Type.GEOMETRY
          && schema
              .getConstraints()
              .filter(constraints -> constraints.isClosed() || constraints.isComposite())
              .isPresent()) {
        steps.add(PipelineSteps.GEOMETRY);
      }

      // unless all properties are required, cleaning maybe needed to remove null values
      if (schema.getConstraints().filter(SchemaConstraints::isRequired).isEmpty()) {
        steps.add(PipelineSteps.CLEAN);
      }
    }

    return steps.build();
  }
}
