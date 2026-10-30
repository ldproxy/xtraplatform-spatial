/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.features.domain

import jakarta.ws.rs.InternalServerErrorException
import java.util.concurrent.CompletionException
import spock.lang.Specification

class FeatureStreamErrorSpec extends Specification {

    def 'a cancelled job is reported as the cancellation, not as a server error: #error.class.simpleName'() {

        when:

        FeatureStream.processStreamError(error)

        then:

        thrown(JobCancelledException)

        where:

        error << [
                new JobCancelledException(),
                new CompletionException(new JobCancelledException()),
                new RuntimeException(new RuntimeException(new JobCancelledException())),
        ]
    }

    def 'another runtime error is a server error'() {

        when:

        FeatureStream.processStreamError(new RuntimeException(new IllegalStateException("broken")))

        then:

        thrown(InternalServerErrorException)
    }
}
