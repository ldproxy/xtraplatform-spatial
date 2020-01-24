package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.immutables.value.Value;

import java.util.List;
import java.util.Optional;

@Value.Immutable
@JsonDeserialize(builder = ImmutableCqlPredicate.Builder.class)
public interface CqlPredicate extends LogicalExpression, ScalarExpression, SpatialExpression, TemporalExpression, CqlNode {

    @Value.Check
    default void check() {
        int count = getExpressions().size();

        Preconditions.checkState(count == 1, "a cql predicate must have exactly one child, found %s", count);
    }

    @JsonIgnore
    @Value.Derived
    @Value.Auxiliary
    default List<CqlNode> getExpressions() {
        return ImmutableList.of(
                getAnd(),
                getOr(),
                getNot(),
                getEq(),
                getNeq(),
                getGt(),
                getGte(),
                getLt(),
                getLte(),
                getLike(),
                getBetween(),
                getEquals(),
                getDisjoint(),
                getTouches(),
                getWithin(),
                getOverlaps(),
                getCrosses(),
                getIntersects(),
                getContains(),
                getAfter(),
                getBefore(),
                getBegins(),
                getBegunBy(),
                getTContains(),
                getDuring(),
                getEndedBy(),
                getEnds(),
                getTEquals(),
                getMeets(),
                getMetBy(),
                getTOverlaps(),
                getOverlappedBy()
        )
                            .stream()
                            .filter(Optional::isPresent)
                            .map(Optional::get)
                            .collect(ImmutableList.toImmutableList());
    }

    @Override
    default String toCqlText() {
        return getExpressions().get(0)
                               .toCqlText();
    }
}
