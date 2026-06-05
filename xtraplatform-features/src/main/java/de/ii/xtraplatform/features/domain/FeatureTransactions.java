/*
 * Copyright 2022 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain;

import com.fasterxml.jackson.databind.JsonNode;
import de.ii.xtraplatform.crs.domain.BoundingBox;
import de.ii.xtraplatform.crs.domain.EpsgCrs;
import de.ii.xtraplatform.features.domain.FeatureStream.ResultBase;
import java.util.List;
import java.util.Optional;
import org.immutables.value.Value;

public interface FeatureTransactions {
  String PATCH_NULL_VALUE = "###NULL###";
  String CAPABILITY = "mutations";

  @Value.Immutable
  interface MutationResult extends FeatureStream.ResultBase {

    enum Type {
      CREATE,
      REPLACE,
      UPDATE,
      DELETE
    }

    abstract class Builder extends ResultBase.Builder<MutationResult, MutationResult.Builder> {
      public abstract Builder addIds(String... ids);
    }

    Type getType();

    List<String> getIds();

    Optional<BoundingBox> getSpatialExtent();

    Optional<Tuple<Long, Long>> getTemporalExtent();

    @Value.Default
    @Override
    default boolean isEmpty() {
      return getIds().isEmpty();
    }
  }

  MutationResult createFeatures(
      String featureType,
      FeatureTokenSource featureTokenSource,
      EpsgCrs crs,
      Optional<String> featureId);

  MutationResult updateFeature(
      String type, String id, FeatureTokenSource featureTokenSource, EpsgCrs crs, boolean partial);

  MutationResult deleteFeature(String featureType, String id);

  /**
   * Property-level partial update. {@code path} is a list of schema property identifiers naming the
   * target property (one element for a top-level property, more for nested ones). An empty {@code
   * value} clears the property (translates to SQL {@code NULL} or the {@link #PATCH_NULL_VALUE}
   * sentinel for value-array entries).
   */
  @Value.Immutable
  interface PropertyUpdate {
    List<String> getPath();

    Optional<JsonNode> getValue();
  }

  /**
   * Opens a session that executes a sequence of mutator calls against a single underlying SQL
   * transaction. {@link Session#commit()} makes the changes durable; {@link Session#close()}
   * without a prior {@code commit()} rolls back. The default implementation throws {@link
   * UnsupportedOperationException} for providers that have not yet adopted the API.
   */
  default Session openSession() {
    throw new UnsupportedOperationException(
        "Multi-action transaction sessions are not supported by this feature provider");
  }

  /**
   * Session-scoped transaction. The mutator methods have the same contract as their top-level
   * counterparts on {@link FeatureTransactions}, but all calls execute against one underlying SQL
   * transaction.
   */
  interface Session extends AutoCloseable {

    MutationResult createFeatures(
        String featureType,
        FeatureTokenSource featureTokenSource,
        EpsgCrs crs,
        Optional<String> featureId);

    /**
     * Drains multiple feature token sources sequentially against the same underlying transaction,
     * giving the implementation a chance to batch generated SQL across features. The default
     * implementation iterates {@link #createFeatures(String, FeatureTokenSource, EpsgCrs,
     * Optional)} once per source.
     *
     * <p>The returned result aggregates ids from every source in source order; on the first source
     * that returns an error, iteration stops and the error is propagated with the ids collected so
     * far.
     */
    default MutationResult createFeatures(
        String featureType, Iterable<FeatureTokenSource> featureTokenSources, EpsgCrs crs) {
      ImmutableMutationResult.Builder result =
          ImmutableMutationResult.builder().type(MutationResult.Type.CREATE).hasFeatures(false);
      for (FeatureTokenSource src : featureTokenSources) {
        MutationResult one = createFeatures(featureType, src, crs, Optional.empty());
        for (String id : one.getIds()) {
          result.addIds(id);
        }
        if (one.getError().isPresent()) {
          return result.error(one.getError().get()).build();
        }
      }
      return result.build();
    }

    MutationResult updateFeature(
        String type,
        String id,
        FeatureTokenSource featureTokenSource,
        EpsgCrs crs,
        boolean partial);

    MutationResult deleteFeature(String featureType, String id);

    /**
     * Applies a property-level partial update to a single existing feature, in place, on this
     * session's open transaction (so the update sees prior writes against the same session). The
     * default implementation throws {@link UnsupportedOperationException}; the SQL provider's
     * session implements it as a native {@code UPDATE} statement on the feature's main table.
     */
    default MutationResult patchFeature(
        String featureType, String featureId, List<PropertyUpdate> updates, EpsgCrs crs) {
      throw new UnsupportedOperationException(
          "Property-level updates are not supported by this feature provider session");
    }

    /** Commits all mutations performed against this session. Throws if already finalised. */
    void commit();

    /** Rolls back all mutations performed against this session. Idempotent. */
    void rollback();

    /** Equivalent to {@link #rollback()} if {@link #commit()} has not been called. */
    @Override
    void close();
  }
}
