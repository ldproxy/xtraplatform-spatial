package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.cql.infra.ObjectVisitor;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableEq.Builder.class)
public interface Eq extends ScalarOperation, CqlNode {

    abstract class Builder extends ScalarOperation.Builder<Eq> {
    }

    @Override
    default String toCqlText() {
        return ScalarOperation.super.toCqlText("=");
    }

    @Override
    default <T> T accept(ObjectVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
