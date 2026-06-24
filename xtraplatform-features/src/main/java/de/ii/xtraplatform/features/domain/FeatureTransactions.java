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
import java.util.Map;
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

    /**
     * Cross-feature batched CREATE with per-role column overrides. After each feature is decoded,
     * the provider applies {@code roleOverrides} to the corresponding row: an entry with a non-null
     * value forces the role-bearing column to that value (the caller pre-formats it as the SQL
     * provider would expect — e.g. an RFC 3339 string for {@code DATETIME}); an entry with a {@code
     * null} value clears the column so it lands as SQL {@code NULL}. Roles whose column cannot be
     * resolved from the type's schema mapping are ignored. The default implementation drops {@code
     * roleOverrides} and delegates to {@link #createFeatures(String, Iterable, EpsgCrs)} for
     * providers that have not yet adopted the API.
     */
    default MutationResult createFeatures(
        String featureType,
        Iterable<FeatureTokenSource> featureTokenSources,
        EpsgCrs crs,
        Map<SchemaBase.Role, Object> roleOverrides) {
      return createFeatures(featureType, featureTokenSources, crs);
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

    /**
     * Sets the {@code PRIMARY_INTERVAL_END} role-bearing column of the open version of {@code
     * featureId} to {@code retirementTimestamp}, on this session's open transaction. Optimistic
     * concurrency: the {@code UPDATE} matches only the row whose end column is currently {@code
     * NULL}. Returns a result whose {@link MutationResult#getIds()} is the retired feature's
     * role-id (i.e. the value the {@code ID} role column held); empty when no open version was
     * found — the caller maps that to a 409-style conflict. Roles that the type's schema mapping
     * does not bind to a column produce an error in the result.
     *
     * <p>The default implementation throws {@link UnsupportedOperationException} for providers that
     * have not yet adopted the API.
     */
    default MutationResult retireFeature(
        String featureType, String featureId, java.time.Instant retirementTimestamp) {
      return retireFeature(featureType, featureId, retirementTimestamp, Optional.empty());
    }

    /**
     * Variant of {@link #retireFeature(String, String, java.time.Instant)} that adds an
     * If-Unmodified-Since-style predicate: the open version's {@code PRIMARY_INTERVAL_START} must
     * equal {@code expectedStart}, otherwise the {@code UPDATE} matches 0 rows and the caller maps
     * that to a 412 Precondition Failed. Empty {@code expectedStart} keeps the three-arg semantics.
     */
    default MutationResult retireFeature(
        String featureType,
        String featureId,
        java.time.Instant retirementTimestamp,
        Optional<java.time.Instant> expectedStart) {
      throw new UnsupportedOperationException(
          "Feature retirement is not supported by this feature provider session");
    }

    /**
     * Reject an insert that targets a {@code featureId} which already exists in any version — open
     * or retired. Versioned-collection clients add new versions of an existing feature through
     * {@code Replace} / {@code Update} / {@code Delete}, not through {@code Insert}, so any
     * existing row for the same id is a conflict regardless of its interval state. Returns a result
     * with {@code error} set when a row is found; an empty success result otherwise. The default
     * implementation returns success (no check) for providers that have not adopted the API.
     */
    default MutationResult assertNoConflictingVersion(String featureType, String featureId) {
      return ImmutableMutationResult.builder()
          .type(MutationResult.Type.CREATE)
          .hasFeatures(false)
          .build();
    }

    /**
     * Capture the {@code PRIMARY_INTERVAL_START} value of the open version of {@code featureId}, as
     * the same string the encoder would store. Empty when no open version exists. Used by versioned
     * mutation paths to populate the {@code PREDECESSOR_INTERVAL_START} denorm column on the new
     * version before the retire/insert pair runs. The default returns empty for providers that have
     * not adopted the API.
     */
    default Optional<String> getOpenVersionStart(String featureType, String featureId) {
      return Optional.empty();
    }

    /**
     * Same as {@link #patchFeature(String, String, List, EpsgCrs)} but additionally constrains the
     * target to the row whose {@code PRIMARY_INTERVAL_END} role-bearing column is currently {@code
     * NULL} — i.e. the currently-open version of {@code featureId}. The {@code updates} may include
     * setting the end column itself (the retire-with-modifications case). Returns an empty result
     * when no open version matches, which the caller maps to a 409-style conflict.
     */
    default MutationResult patchOpenVersion(
        String featureType, String featureId, List<PropertyUpdate> updates, EpsgCrs crs) {
      return patchOpenVersion(featureType, featureId, updates, crs, Optional.empty());
    }

    /**
     * Variant of {@link #patchOpenVersion(String, String, List, EpsgCrs)} that adds an
     * If-Unmodified-Since-style predicate: the open version's {@code PRIMARY_INTERVAL_START} must
     * equal {@code expectedStart}, otherwise the {@code UPDATE} matches 0 rows and the caller maps
     * that to a 412 Precondition Failed.
     */
    default MutationResult patchOpenVersion(
        String featureType,
        String featureId,
        List<PropertyUpdate> updates,
        EpsgCrs crs,
        Optional<java.time.Instant> expectedStart) {
      throw new UnsupportedOperationException(
          "Open-version patching is not supported by this feature provider session");
    }

    /**
     * Clones the open version of {@code featureId} into a new row (forcing {@code
     * PRIMARY_INTERVAL_START} to {@code mutationTimestamp} and {@code PRIMARY_INTERVAL_END} to
     * {@code NULL}), applies {@code updates} to the new row, and retires the old row by setting its
     * {@code PRIMARY_INTERVAL_END} to {@code mutationTimestamp}. Optimistic concurrency matches
     * only the row whose {@code PRIMARY_INTERVAL_END} is currently {@code NULL}; an empty result
     * signals 409-style conflict.
     */
    default MutationResult cloneAndPatchFeature(
        String featureType,
        String featureId,
        List<PropertyUpdate> updates,
        java.time.Instant mutationTimestamp,
        EpsgCrs crs) {
      return cloneAndPatchFeature(
          featureType, featureId, updates, mutationTimestamp, crs, Optional.empty());
    }

    /**
     * Variant of {@link #cloneAndPatchFeature(String, String, List, java.time.Instant, EpsgCrs)}
     * that adds an If-Unmodified-Since-style predicate: the open version's {@code
     * PRIMARY_INTERVAL_START} must equal {@code expectedStart}, otherwise the open-version lookup
     * matches 0 rows and the caller maps that to a 412 Precondition Failed.
     */
    default MutationResult cloneAndPatchFeature(
        String featureType,
        String featureId,
        List<PropertyUpdate> updates,
        java.time.Instant mutationTimestamp,
        EpsgCrs crs,
        Optional<java.time.Instant> expectedStart) {
      throw new UnsupportedOperationException(
          "Clone-and-patch is not supported by this feature provider session");
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
