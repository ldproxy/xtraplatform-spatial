package de.ii.xtraplatform.cql.domain;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(builder = ImmutableLt.Builder.class)
public interface Lt extends ScalarOperation, CqlNode {

    static Lt of(String property, ScalarLiteral scalarLiteral) {
        return new ImmutableLt.Builder().property(property)
                                        .value(scalarLiteral)
                                        .build();
    }

    abstract class Builder extends ScalarOperation.Builder<Lt> {
    }

}
