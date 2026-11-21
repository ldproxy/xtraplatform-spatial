/*
 * Copyright 2026 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.crs.infra

import de.ii.xtraplatform.base.domain.resiliency.VolatileRegistry
import de.ii.xtraplatform.blobs.domain.ResourceStore
import de.ii.xtraplatform.crs.domain.CrsTransformer
import de.ii.xtraplatform.crs.domain.EpsgCrs
import de.ii.xtraplatform.proj.domain.ProjLoaderImpl
import spock.lang.Specification
import spock.lang.Timeout

import java.nio.file.Path
import java.util.concurrent.CompletableFuture
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.function.Function

class CrsTransformerFactoryProjConcurrencySpec extends Specification {

    CrsTransformerFactoryProj transformerFactory

    // one thread per task, both have to run at the same time
    ExecutorService executor = Executors.newFixedThreadPool(2)

    def cleanup() {
        executor.shutdownNow()
    }

    // the PROJ search path is global, so the same directories as in CrsTransformerAreaOfInterestSpec
    def setup() {
        Path gridDirectory = Path.of(getClass().getResource("/proj/de_hvbg_hessen_HeTA2010.tif").toURI()).getParent()
        ResourceStore projStore = Stub()
        projStore.asLocalPath(*_) >> Optional.of(gridDirectory)
        ResourceStore resourceStore = Stub()
        resourceStore.with(*_) >> projStore
        VolatileRegistry volatileRegistry = Stub()
        volatileRegistry.onAvailable(*_) >> CompletableFuture.completedFuture(null)
        transformerFactory = new CrsTransformerFactoryProj(new ProjLoaderImpl(Path.of(System.getProperty("java.io.tmpdir"), "proj", "data")), resourceStore, volatileRegistry)
        transformerFactory.onStart(false).toCompletableFuture().join()
    }

    /*
     * The transformer for a 3D source needs the horizontal CRS, for EPSG:4979 that is EPSG:4326 from
     * the CRS cache. Thread "cache" computes the cache entry of EPSG:4326 and, like createCrs, needs
     * the PROJ lock for it. Thread "transformer" creates a transformer from EPSG:4979 and must not
     * hold the PROJ lock while it waits for that cache entry, otherwise both wait forever.
     */
    @Timeout(30)
    def 'creating a transformer does not wait for the CRS cache while holding the PROJ lock'() {

        given:
        EpsgCrs source = EpsgCrs.of(4979)
        EpsgCrs target = EpsgCrs.of(25832)
        EpsgCrs horizontal = EpsgCrs.of(4326)
        // the CRSs of the transformer are in the cache, only the horizontal CRS is missing
        transformerFactory.isSupported(source)
        transformerFactory.isSupported(target)

        CountDownLatch cacheEntryReserved = new CountDownLatch(1)
        CountDownLatch transformerStarted = new CountDownLatch(1)
        boolean projLockAcquired = false

        Function<EpsgCrs, Object> computeCacheEntry = { EpsgCrs crs ->
            cacheEntryReserved.countDown()
            transformerStarted.await()
            // give the transformer thread time to reach the cache entry
            Thread.sleep(500)
            projLockAcquired = transformerFactory.projLock.tryLock(5, TimeUnit.SECONDS)
            if (!projLockAcquired) {
                return null
            }
            try {
                return transformerFactory.createCrs(crs).orElse(null)
            } finally {
                transformerFactory.projLock.unlock()
            }
        }

        when:
        CompletableFuture<Object> cacheEntry = CompletableFuture.supplyAsync({
            transformerFactory.crsCache.computeIfAbsent(horizontal, computeCacheEntry)
        }, executor)
        cacheEntryReserved.await()
        CompletableFuture<Optional<CrsTransformer>> transformer = CompletableFuture.supplyAsync({
            transformerStarted.countDown()
            transformerFactory.getTransformer(source, target)
        }, executor)
        Object horizontalCrs = cacheEntry.get(20, TimeUnit.SECONDS)
        Optional<CrsTransformer> result = transformer.get(20, TimeUnit.SECONDS)

        then:
        projLockAcquired
        horizontalCrs != null
        result.isPresent()
    }
}
