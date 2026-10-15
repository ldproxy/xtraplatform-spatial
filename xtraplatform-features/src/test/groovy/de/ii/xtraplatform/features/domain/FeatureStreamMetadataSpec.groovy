/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import spock.lang.Specification

import java.util.concurrent.CompletableFuture
import java.util.concurrent.CompletionException
import java.util.OptionalLong

class FeatureStreamMetadataSpec extends Specification {

    def 'a stream that ends with an error before it started completes the metadata with the error'() {
        given:
        def error = new IllegalStateException("rejected by the database")
        def result = CompletableFuture.completedFuture(
                ImmutableResult.builder().isEmpty(true).hasFeatures(false).error(error).build())
        def metadata = new CompletableFuture<CollectionMetadata>()

        when:
        FeatureStreamImpl.completeMetadataOnEnd(result, metadata).toCompletableFuture().join()
        metadata.join()

        then:
        def e = thrown(CompletionException)
        e.cause.is(error)
    }

    def 'a stream that fails before it started completes the metadata with the failure'() {
        given:
        def error = new IllegalArgumentException("no connector")
        def result = CompletableFuture.<FeatureStream.Result> failedFuture(error)
        def metadata = new CompletableFuture<CollectionMetadata>()

        when:
        FeatureStreamImpl.completeMetadataOnEnd(result, metadata)
        metadata.join()

        then:
        def e = thrown(CompletionException)
        e.cause.is(error)
    }

    def 'a stream that ends without an error before it started completes the metadata exceptionally'() {
        given:
        def result = CompletableFuture.completedFuture(
                ImmutableResult.builder().isEmpty(true).hasFeatures(false).build())
        def metadata = new CompletableFuture<CollectionMetadata>()

        when:
        FeatureStreamImpl.completeMetadataOnEnd(result, metadata).toCompletableFuture().join()
        metadata.join()

        then:
        def e = thrown(CompletionException)
        e.cause instanceof IllegalStateException
    }

    def 'metadata published when the stream started is kept'() {
        given:
        def published = ImmutableCollectionMetadata.of(OptionalLong.of(0L), OptionalLong.of(0L))
        def metadata = CompletableFuture.<CollectionMetadata> completedFuture(published)
        def error = new IllegalStateException("failed while streaming")
        def result = CompletableFuture.completedFuture(
                ImmutableResult.builder().isEmpty(true).hasFeatures(false).error(error).build())

        when:
        FeatureStreamImpl.completeMetadataOnEnd(result, metadata).toCompletableFuture().join()

        then:
        metadata.join().is(published)
    }
}
