package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableLte.Builder.class)
public interface Lte extends ScalarOperation, CqlNode {

    abstract class Builder extends ScalarOperation.Builder<Lte> {
    }

    @Override
    default String toCqlText() {
        return ScalarOperation.super.toCqlText("<=");
    }

}
