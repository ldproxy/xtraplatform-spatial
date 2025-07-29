package de.ii.xtraplatform.feature.core;

/*
import akka.Done;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import de.ii.xsf.configstore.api.KeyValueStore;
import de.ii.xsf.configstore.api.WriteTransaction;
import de.ii.xsf.dropwizard.api.Jackson;
import de.ii.xtraplatform.entity.api.EntityRepository;
import de.ii.xtraplatform.entity.api.PersistentEntity;
import de.ii.xtraplatform.entity.repository.EntityInstantiator;
import de.ii.xtraplatform.feature.provider.api.FeatureConsumer;
import de.ii.xtraplatform.feature.provider.api.FeatureQuery;
import de.ii.xtraplatform.feature.provider.api.FeatureStream;
import de.ii.xtraplatform.feature.provider.api.ImmutableFeatureQuery;
import de.ii.xtraplatform.feature.provider.api.GeometryType;
import de.ii.xtraplatform.feature.provider.api.TargetMapping;
import de.ii.xtraplatform.feature.transformer.api.AbstractFeatureTransformerService;
import de.ii.xtraplatform.feature.transformer.api.FeatureTransformer;
import de.ii.xtraplatform.feature.transformer.api.FeatureTransformerService2;
import org.apache.felix.ipojo.Factory;
import org.junit.After;
import org.junit.Before;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.CoreOptions;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.options.DefaultCompositeOption;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.ow2.chameleon.testing.helpers.IPOJOHelper;
import org.ow2.chameleon.testing.helpers.OSGiAssert;
import org.ow2.chameleon.testing.helpers.OSGiHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static de.ii.xtraplatform.feature.provider.api.TargetMapping.BASE_TYPE;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
*/
//@RunWith(PaxExam.class)
public class TestEntityRepository {
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(TestEntityRepository.class);
//
//    private static final List<String> testBundles = ImmutableList.<String>builder()
//            .add("xtraplatform-api")
//            .add("xtraplatform-kvstore-api")
//            .add("xtraplatform-kvstore-inmemory")
//            //.add("xtraplatform-config-store-api")
//            .add("xtraplatform-dropwizard")
//
//            //TODO: check and reduce
//            .add("xtraplatform-core")
//            .add("xtraplatform-akka")
//            .add("xtraplatform-akka-http")
//            .add("xtraplatform-ogc-wfs-api")
//            .add("xtraplatform-geotools")
//
//            .add("xtraplatform-crs-api")
//            .add("xtraplatform-crs-transformation-geotools")
//            .add("xtraplatform-feature-provider-api")
//            .add("xtraplatform-feature-transformer-api")
//            .add("xtraplatform-feature-provider-pgis")
//            .add("xtraplatform-feature-provider")
//
//            .add("xtraplatform-entity-api")
//            .add("xtraplatform-entity-repository")
//            .add("xtraplatform-service-api")
//            .build();
//
//    @Inject
//    private BundleContext context;
//
//    //@Inject
//    private EntityRepository entityRepository;
//    private KeyValueStore keyValueStore;
//
//    private OSGiHelper osgi;
//    private IPOJOHelper ipojo;
//
//    //@Inject @Filter("(sophistication=major)")
//    //private CoolService coolService;
//
//    //@Rule
//    //public MethodRule rule = new MyMethodRule();
//
//    @Configuration
//    public Option[] config1() throws IOException {
//        //TODO: generate a file with local bundle urls, load it here
//        Properties properties = new Properties();
//        File bundles = new File("build/classes/test/META-INF/maven/dependencies.properties");
//        System.out.println("BUNDLES: " + bundles.getAbsolutePath());
//        properties.load(new FileReader(bundles));
//
//        //bundle(new File('./../out/production/Filter4osgi.jar').toURI().toString())
//        DefaultCompositeOption defaultCompositeOption = new DefaultCompositeOption(junitBundles());
//
//        Arrays.stream(properties.getProperty("bundles")
//                                .split(","))
//              .filter(bundle -> !Strings.isNullOrEmpty(bundle))
//              .filter(bundle -> !(bundle.contains("xtraplatform") || bundle.contains("felix.http")) || testBundles.stream()
//                                                                                                                  .anyMatch(bundle::contains))
//              .map(CoreOptions::bundle)
//              .forEach(defaultCompositeOption::add)
//        //.forEach(b -> System.out.println(b))
//        ;
//
//        // for dropwizard
//        defaultCompositeOption.add(CoreOptions.systemPackages("sun.reflect", "sun.misc", "sun.security.x509", "sun.security.util"));
//        defaultCompositeOption.add(CoreOptions.frameworkProperty("de.ii.xtraplatform.directories.data")
//                                              .value("src/test/resources"));
//
//        return defaultCompositeOption.getOptions();
//    }
//
//    @Before
//    public void setUp() {
//        System.out.println("BC: " + context.getBundle()
//                                           .getSymbolicName());
//
//        osgi = new OSGiHelper(context) {
//            @Override
//            public <T> T waitForService(Class<T> itf, String filter, long timeout) {
//                try {
//                    return super.waitForService(itf, filter, timeout);
//                } catch (AssertionError e) {
//                    throw new AssertionError(itf.getName() + ": no matching service found after timeout of " + timeout + "ms");
//                }
//            }
//        };
//        ipojo = new IPOJOHelper(context);
//    }
//
//    @After
//    public void tearDown() {
//        osgi.dispose();
//        ipojo.dispose();
//    }
//
//    //@Test
//    public void test1() {
//        OSGiAssert osgiAssert = new OSGiAssert(context);
//
//        // did all required bundles start?
//        testBundles.forEach(bundle -> LOGGER.debug("BUNDLE {} {} {}", bundle, osgi.getBundle(bundle)
//                                                                                  .getBundleId(), osgi.getBundle(bundle)
//                                                                                                      .getState()));
//        testBundles.forEach(bundle -> osgiAssert.assertBundleState(bundle, Bundle.ACTIVE));
//
//        // did all required services start?
//        osgi.waitForService(KeyValueStore.class, null, 5000); // akka start takes long
//        osgi.waitForService(Jackson.class, null, 1000);
//        this.entityRepository = osgi.waitForService(EntityRepository.class, null, 1000);
//        osgi.waitForService(EntityInstantiator.class, null, 1000);
//        LOGGER.debug("REPO {}", entityRepository);
//
//        // create two TestService entities
//        createEntity("foo", true);
//        createEntity("bar", false);
//
//        //TODO instance is either in registry or in factory, but not both
//        osgi.waitForService(PersistentEntity.class.getName(), null, 1000, true);
//
//        Factory factory = ipojo.getFactory(AbstractFeatureTransformerService.class.getName());
//        LOGGER.debug("INSTANCES {} {} {}", factory.getInstancesNames(), factory.getComponentDescription()
//                                                                               .getprovidedServiceSpecification(), osgi.isServiceAvailable(PersistentEntity.class));
//
//        String instanceName = AbstractFeatureTransformerService.class.getName() + "/" + "foo";
//        //ComponentInstance instance = ipojo.getInstanceByName(instanceName);
////        Optional<ComponentInstance> instance = factory.getInstances()
////                                                      .stream()
////                                                      .filter(i -> i.getInstanceName()
////                                                                    .equals(instanceName))
////                                                      .findFirst();
////        assertTrue("instance not found: " + instanceName, instance.isPresent());
//
//        assertTrue("instance not valid: " + instanceName, ipojo.isInstanceValid(instanceName));
//
//
//        //assertEquals("It's so hot!", entityInstantiator.getResult());
//
//
////        executorService.schedule(() -> {
////            try {
////                entityStore.replaceEntity(ImmutableServiceData.builder().id("foo").label("Foobar").build());
////
////                LOGGER.debug("REPLACED");
////                // TODO: not replaced yet, so how to return data on update? by really returning the data from replaceEntity and not the entity itself?
////                // TODO: another point: retracting the instances for updates will not work in every case as state is lost
////            } catch (IOException e) {
////                //ignore
////            }
////        }, 5, TimeUnit.SECONDS);
//    }
//
//    //@Test
//    public void testLoadEntitiesFromStore() throws IOException, InterruptedException, ExecutionException, TimeoutException {
//        OSGiAssert osgiAssert = new OSGiAssert(context);
//
//        // did all required bundles start?
//        testBundles.forEach(bundle -> LOGGER.debug("BUNDLE {} {} {}", bundle, osgi.getBundle(bundle)
//                                                                                  .getBundleId(), osgi.getBundle(bundle)
//                                                                                                      .getState()));
//        testBundles.forEach(bundle -> osgiAssert.assertBundleState(bundle, Bundle.ACTIVE));
//
//        // did all required services start?
//        this.keyValueStore = osgi.waitForService(KeyValueStore.class, null, 5000); // akka start takes long
//        osgi.waitForService(Jackson.class, null, 1000);
//
//        // create two TestService entities
//        createJsonEntity("oneo", true);
//        //createJsonEntity("bar", false);
//
//        this.entityRepository = osgi.waitForService(EntityRepository.class, null, 1000);
//        osgi.waitForService(EntityInstantiator.class, null, 1000);
//        LOGGER.debug("REPO {}", entityRepository);
//
//        //TODO instance is either in registry or in factory, but not both
//        FeatureTransformerService2 featureTransformerService = osgi.waitForService(FeatureTransformerService2.class, null, 10000, true);
//
//        Factory factory = ipojo.getFactory(AbstractFeatureTransformerService.class.getName());
//        LOGGER.debug("INSTANCES {} {} {}", factory.getInstancesNames(), factory.getComponentDescription()
//                                                                               .getprovidedServiceSpecification(), osgi.isServiceAvailable(PersistentEntity.class));
//
//        String instanceName = AbstractFeatureTransformerService.class.getName() + "/" + "oneo";
//        assertTrue("instance not valid: " + instanceName, ipojo.isInstanceValid(instanceName));
//
//        String filter = "BBOX(/fundorttiere/[id=id]artbeobachtung/[geom=id]geom/geom, -85.0511287798066,-180,84.92832092949963,90) AND /fundorttiere/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser/name LIKE '*Mat*' AND /fundorttiere/[id=id]osirisobjekt/veroeffentlichtam DURING 1970-01-01T00:00:00Z/2018-07-17T07:14:27Z";
//        String filter2 = "/fundorttiere/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser/name LIKE '*Mat*'";
//        String filter3 = "/fundorttiere/[id=id]osirisobjekt/veroeffentlichtam DURING 1970-01-01T00:00:00Z/2018-07-17T07:14:27Z";
//
//        FeatureQuery query = ImmutableFeatureQuery.builder().type("fundorttiere")
//                                                    .filter("IN ('24')")
//                                                    .build();
//
//        FeatureQuery query2 = ImmutableFeatureQuery.builder().type("fundorttiere")
//                                                      .limit(10)
//                                                      .build();
//        // wait for crs transformer
//        TimeUnit.SECONDS.sleep(5);
//        for (int i = 0; i < 5; i++) {
//            FeatureStream<FeatureTransformer> stream = featureTransformerService.getFeatureProvider()
//                                                                                .getFeatureTransformStream(query2);
//
//            LoggingFeatureTransformer loggingFeatureTransformer = new LoggingFeatureTransformer();
//            CompletionStage<Done> done = stream.apply(loggingFeatureTransformer);
//
//            done.toCompletableFuture()
//                .get(20, TimeUnit.SECONDS);
//
//            LOGGER.debug(loggingFeatureTransformer.log.toString());
//        }
//    }
//
//    private void createJsonEntity(String id, boolean shouldStart) throws IOException {
//        String[] path = {"entities", "services"};
//
//        WriteTransaction<String> transaction = keyValueStore.getChildStore(path)
//                                                            .openWriteTransaction(id);
//
//        try {
//            transaction.write(String.format(entityTemplate, id, shouldStart));
//        } catch (IOException e) {
//            LOGGER.debug("CREATE ERROR", e);
//            throw e;
//        }
//
//        LOGGER.debug("PRELOADED {}", id);
//    }
//
//    private void createEntity(String id, boolean shouldStart) {
//        /*try {
//            new EntityRepositoryForType(entityRepository, AbstractFeatureTransformerService.class.getName()).createEntity(ImmutableFeatureTransformerServiceData.builder()
//                                                                                                                                                                .id(id)
//                                                                                                                                                                .label(id.toUpperCase())
//                                                                                                                                                                .createdAt(Instant.now()
//                                                                                                                                                                                  .toEpochMilli())
//                                                                                                                                                                .lastModified(Instant.now()
//                                                                                                                                                                                     .toEpochMilli())
//                                                                                                                                                                .serviceType("WFS3")
//                                                                                                                                                                .putFeatureTypes("fundortpflanzen", ImmutableFeatureTypeConfigurationWfs3.builder()
//                                                                                                                                                                                                                                         .id("fundortpflanzen")
//                                                                                                                                                                                                                                         .label("FundortPflanzen")
//                                                                                                                                                                                                                                         .extent(new FeatureTypeConfigurationWfs3.FeatureTypeExtent(new TemporalExtent(0, 0),new BoundingBox()))
//                                                                                                                                                                                                                                         .build())
//                                                                                                                                                                .featureProvider(ImmutableFeatureProviderDataPgis.builder()
//                                                                                                                                                                                                                 .connectionInfo(ImmutableConnectionInfo.builder()
//                                                                                                                                                                                                                                                        .host("192.168.230.76")
//                                                                                                                                                                                                                                                        .database("simpledemo_hale")
//                                                                                                                                                                                                                                                        .user("postgres")
//                                                                                                                                                                                                                                                        .password("postgres")
//                                                                                                                                                                                                                                                        .build())
//                                                                                                                                                                                                                 .putMappings("fundortpflanzen", ImmutableFeatureTypeMapping.builder()
//                                                                                                                                                                                                                                                                            .putMappings("/fundortpflanzen", ImmutableSourcePathMapping.builder()
//                                                                                                                                                                                                                                                                                                                                       .putMappings("general", ImmutableTargetMappingWfs3.builder()
//                                                                                                                                                                                                                                                                                                                                                                                         .name("{{id}}")
//                                                                                                                                                                                                                                                                                                                                                                                         .enabled(true)
//                                                                                                                                                                                                                                                                                                                                                                                         .type(TargetMappingWfs3.WFS3_TYPES.SPATIAL)
//                                                                                                                                                                                                                                                                                                                                                                                         .build())
//                                                                                                                                                                                                                                                                                                                                       .build())
//                                                                                                                                                                                                                                                                            .build())
//                                                                                                                                                                                                                 .build())
//                                                                                                                                                                // REGISTRATION
//                                                                                                                                                                .shouldStart(shouldStart)
//                                                                                                                                                                .build());
//        } catch (IOException e) {
//            LOGGER.debug("CREATE ERROR", e);
//        }*/
//
//    }
//
//    static class LoggingFeatureTransformer implements FeatureTransformer {
//        public StringBuilder log = new StringBuilder();
//
//        @Override
//        public String getTargetFormat() {
//            return BASE_TYPE;
//        }
//
//        @Override
//        public void onStart(OptionalLong numberReturned, OptionalLong numberMatched) throws Exception {
//            log.append(String.format("START: %d %d\n", numberReturned.orElse(-1), numberMatched.orElse(-1)));
//        }
//
//        @Override
//        public void onEnd() throws Exception {
//            log.append(String.format("END\n"));
//        }
//
//        @Override
//        public void onFeatureStart(TargetMapping mapping) throws Exception {
//            log.append(Objects.nonNull(mapping) ? mapping.getName() : "NOMAPPING");
//            log.append("{\n");
//        }
//
//        @Override
//        public void onFeatureEnd() throws Exception {
//            log.append("}\n");
//        }
//
//        @Override
//        public void onPropertyStart(TargetMapping mapping, List<Integer> multiplicities) throws Exception {
//            log.append("    ");
//            log.append(Objects.nonNull(mapping) ? mapping.getName() : "NOMAPPING");
//            if (Objects.nonNull(mapping) && mapping.getName().contains("["))
//                log.append("[").append(multiplicities).append("]");
//            log.append(": ");
//        }
//
//        @Override
//        public void onPropertyText(String text) throws Exception {
//            log.append(text);
//        }
//
//
//        @Override
//        public void onPropertyEnd() throws Exception {
//            log.append("\n");
//        }
//
//        @Override
//        public void onGeometryStart(TargetMapping mapping, GeometryType type, Integer dimension) throws Exception {
//            log.append("    ");
//            log.append(Objects.nonNull(mapping) ? mapping.getName() : "NOMAPPING");
//            log.append("|");
//            log.append(type);
//            log.append(": ");
//        }
//
//        @Override
//        public void onGeometryNestedStart() throws Exception {
//            log.append("\nNESTOPEN");
//        }
//
//        @Override
//        public void onGeometryCoordinates(String text) throws Exception {
//            log.append("\n")
//               .append(text);
//        }
//
//        @Override
//        public void onGeometryNestedEnd() throws Exception {
//            log.append("\nNESTCLOSE");
//        }
//
//        @Override
//        public void onGeometryEnd() throws Exception {
//            log.append("\n");
//        }
//    }
//
//    static class LoggingFeatureConsumer implements FeatureConsumer {
//        public StringBuilder log = new StringBuilder();
//
//        @Override
//        public void onStart(OptionalLong numberReturned, OptionalLong numberMatched) throws Exception {
//            log.append(String.format("START: %d %d\n", numberReturned.orElse(-1), numberMatched.orElse(-1)));
//        }
//
//        @Override
//        public void onEnd() throws Exception {
//            log.append(String.format("END\n"));
//        }
//
//        @Override
//        public void onFeatureStart(List<String> path) throws Exception {
//            log.append(path);
//            log.append("{\n");
//        }
//
//        @Override
//        public void onFeatureEnd(List<String> path) throws Exception {
//            log.append("{\n");
//        }
//
//        @Override
//        public void onPropertyStart(List<String> path, List<Integer> multiplicities) throws Exception {
//            log.append("    ");
//            log.append(path);
//            log.append(": ");
//        }
//
//        @Override
//        public void onPropertyText(String text) throws Exception {
//            log.append(text);
//        }
//
//        @Override
//        public void onPropertyEnd(List<String> path) throws Exception {
//            log.append("\n");
//        }
//    }
//
//    static String entityTemplate;
//
//    static {
//        try {
//            entityTemplate = new String(Files.readAllBytes(Paths.get("/home/zahnen/development/ldproxy/build/data/config-store/entities/services/oneo")));
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    static String entityTemplate2 = "{\n" +
//            "      \"featureTypes\" : {\n" +
//            "        \"city\" : {\n" +
//            "          \"temporalExtent\" : {\n" +
//            "            \"start\" : 0,\n" +
//            "            \"end\" : 0\n" +
//            "          },\n" +
//            "          \"spatialExtent\" : {\n" +
//            "            \"xmin\" : -180.0,\n" +
//            "            \"ymin\" : -90.0,\n" +
//            "            \"xmax\" : 180.0,\n" +
//            "            \"ymax\" : 90.0,\n" +
//            "            \"coords\" : [ -180.0, -90.0, 180.0, 90.0 ],\n" +
//            "            \"epsgCrs\" : {\n" +
//            "              \"code\" : 4326,\n" +
//            "              \"longitudeFirst\" : false\n" +
//            "            }\n" +
//            "          },\n" +
//            "          \"id\" : \"city\",\n" +
//            "          \"label\" : \"Stadt\"\n" +
//            "        }\n" +
//            "      },\n" +
//            "      \"featureProvider\" : {\n" +
//            "        \"providerType\" : \"PGIS\",\n" +
//            "        \"connectionInfo\" : {\n" +
//            "          \"host\" : \"192.168.230.76\",\n" +
//            "          \"database\" : \"simpledemo_hale\",\n" +
//            "          \"user\" : \"postgres\",\n" +
//            "          \"password\" : \"postgres\"\n" +
//            "        },\n" +
//            "        \"mappings\" : {\n" +
//            "          \"city\" : {\n" +
//            "            \"/city\" : {\n" +
//            "              \"general\" : {\n" +
//            "                \"mappingType\" : \"WFS3\",\n" +
//            "                \"enabled\" : true,\n" +
//            "                \"type\" : \"SPATIAL\",\n" +
//            "                \"name\" : \"{{id}}\"\n" +
//            "              }\n" +
//            "            },\n" +
//            "            \"/city/id\" : {\n" +
//            "              \"general\" : {\n" +
//            "                \"mappingType\" : \"WFS3\",\n" +
//            "                \"enabled\" : true,\n" +
//            "                \"type\" : \"SPATIAL\",\n" +
//            "                \"name\" : \"id\"\n" +
//            "              }\n" +
//            "            },\n" +
//            "            \"/city/name\" : {\n" +
//            "              \"general\" : {\n" +
//            "                \"mappingType\" : \"WFS3\",\n" +
//            "                \"enabled\" : true,\n" +
//            "                \"type\" : \"SPATIAL\",\n" +
//            "                \"name\" : \"name\"\n" +
//            "              }\n" +
//            "            },\n" +
//            "            \"/city/[id=cid]alternativename/name\" : {\n" +
//            "              \"general\" : {\n" +
//            "                \"mappingType\" : \"WFS3\",\n" +
//            "                \"enabled\" : true,\n" +
//            "                \"type\" : \"SPATIAL\",\n" +
//            "                \"name\" : \"alternativename\"\n" +
//            "              }\n" +
//            "            },\n" +
//            "            \"/city/[id=cid]city_river/[rid=id]river/name\" : {\n" +
//            "              \"general\" : {\n" +
//            "                \"mappingType\" : \"WFS3\",\n" +
//            "                \"enabled\" : true,\n" +
//            "                \"type\" : \"SPATIAL\",\n" +
//            "                \"name\" : \"river\"\n" +
//            "              }\n" +
//            "            }\n" +
//            "          }\n" +
//            "        }\n" +
//            "      },\n" +
//            "      \"serviceType\" : \"WFS3\",\n" +
//            "      \"label\" : \"BAR\",\n" +
//            "      \"id\" : \"%s\",\n" +
//            "      \"shouldStart\" : %b,\n" +
//            "      \"createdAt\" : 1530794333758,\n" +
//            "      \"lastModified\" : 1530794333758\n" +
//            "    }\n";
//
//    static String artendaten = "{\n" +
//            "    \"serviceType\": \"WFS3\",\n" +
//            "    \"createdAt\": 1531329541131,\n" +
//            "    \"description\": \"...\",\n" +
//            "    \"featureTypes\":\n" +
//            "    {\n" +
//            "        \"fundortpflanzen\":\n" +
//            "        {\n" +
//            "            \"extent\":\n" +
//            "            {\n" +
//            "                \"spatial\":\n" +
//            "                {\n" +
//            "                    \"ymin\": -90,\n" +
//            "                    \"xmin\": -180,\n" +
//            "                    \"ymax\": 90,\n" +
//            "                    \"xmax\": 180\n" +
//            "                },\n" +
//            "                \"temporal\":\n" +
//            "                {\n" +
//            "                    \"start\": null,\n" +
//            "                    \"end\": null\n" +
//            "                }\n" +
//            "            },\n" +
//            "            \"description\": \"\",\n" +
//            "            \"id\": \"fundortpflanzen\",\n" +
//            "            \"label\": \"FundortePflanzen\"\n" +
//            "        },\n" +
//            "        \"fundorttiere\":\n" +
//            "        {\n" +
//            "            \"extent\":\n" +
//            "            {\n" +
//            "                \"spatial\":\n" +
//            "                {\n" +
//            "                    \"ymin\": -90,\n" +
//            "                    \"xmin\": -180,\n" +
//            "                    \"ymax\": 90,\n" +
//            "                    \"xmax\": 180\n" +
//            "                },\n" +
//            "                \"temporal\":\n" +
//            "                {\n" +
//            "                    \"start\": null,\n" +
//            "                    \"end\": null\n" +
//            "                }\n" +
//            "            },\n" +
//            "            \"description\": \"\",\n" +
//            "            \"id\": \"fundorttiere\",\n" +
//            "            \"label\": \"FundorteTiere\"\n" +
//            "        }\n" +
//            "    },\n" +
//            "    \"id\": \"%s\",\n" +
//            "    \"shouldStart\": %b,\n" +
//            "    \"label\": \"Web API für OSIRIS-Neo\",\n" +
//            "    \"lastModified\": 1531329541131,\n" +
//            "    \"featureProvider\":\n" +
//            "    {\n" +
//            "        \"mappings\":\n" +
//            "        {\n" +
//            "            \"fundortpflanzen\":\n" +
//            "            {\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/begehungsmethode\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"begehungsmethode\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Begehungsmethode\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/aufnahmezeitpunkt\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].aufnahmezeitpunkt\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Aufnahmezeitpunkt\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hinweise\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/unschaerfe\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"unschaerfe\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Unschärfe\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/[id=artbeobachtung_id]artbeobachtung_2_erfasser\\/[erfasser_id=id]erfasser\\/name\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"erfasser[erfasser].name\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Erfassername\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/datumabgleich\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].datumabgleich\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Abgleichsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/pflanzenart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"pflanzenart\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Pflanzenart\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/anzahl\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"anzahl\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Anzahl\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/[geom=id]geom\\/geom\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"geometrie\",\n" +
//            "                        \"type\": \"SPATIAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": false,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"geometrie\",\n" +
//            "                        \"type\": \"GEOMETRY\",\n" +
//            "                        \"geometryType\": \"GENERIC\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"GEOMETRY\",\n" +
//            "                        \"geometryType\": \"GENERIC\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/fotoverweis\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].fotoverweis\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Foto\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/informationsquelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"informationsquelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Informationsquelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Administrative Hinweise\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/id\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"id\",\n" +
//            "                        \"type\": \"ID\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Id\",\n" +
//            "                        \"type\": \"ID\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"ID\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/kreisschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].kreisschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Kreis\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"itemType\": \"http:\\/\\/schema.org\\/Place\",\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"{{Full Name}}\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/bemerkunginformationsquelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkunginformationsquelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bemerkung zur Informationsquelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/gemeindeschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].gemeindeschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Gemeinde\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/verbandsgemeindeschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].verbandsgemeindeschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Verbandsgemeinde\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/haeufigkeit\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"haeufigkeit\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Häufigkeit\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/bemerkungpflanzenart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkungpflanzenart\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bemerkung zur Pflanzenart\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/kennung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"kennung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Objektkennung\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/letzteskartierdatum\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"letzteskartierdatum\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Kartierdurchlauf letzter Durchgang\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/beobachtetam\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"beobachtetam\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Beobachtungsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/veroeffentlichtam\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"veroeffentlichtam\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Einschpeicherungsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/bezeichnung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bezeichnung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Objektbezeichnung\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/[id=artbeobachtung_id]artbeobachtung_2_erfasser\\/[erfasser_id=id]erfasser\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"erfasser[erfasser].bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hinweise zum Erfasser\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/verantwortlichestelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"verantwortlichestelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Verantwortliche Stelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_fachreferenz\\/[fachreferenz_id=id]osirisobjekt\\/id:objektart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].fachreferenz[osirisobjekt]\",\n" +
//            "                        \"pattern\": \"{{serviceUrl}}\\/collections\\/{{objektart}}\\/items\\/{{id}}\",\n" +
//            "                        \"type\": \"REFERENCE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"fachreferenz\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/hauptfoto\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].hauptfoto\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hauptfoto?\",\n" +
//            "                        \"type\": \"BOOLEAN\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"BOOLEAN\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/deckungsgrad\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"deckungsgrad\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Deckungsgrad\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/[id=ortsangaben_id]ortsangaben_flurstueckskennzeichen\\/flurstueckskennzeichen\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].flurstueckskennzeichen[ortsangaben_flurstueckskennzeichen]\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Flurstück\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundortpflanzen\\/[id=id]artbeobachtung\\/bemerkungort\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkungort\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bermerkung zum Fundort\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                }\n" +
//            "            },\n" +
//            "            \"fundorttiere\":\n" +
//            "            {\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/informationsquelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"informationsquelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Informationsquelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/[id=artbeobachtung_id]artbeobachtung_2_erfasser\\/[erfasser_id=id]erfasser\\/name\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"erfasser[erfasser].name\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Erfassername\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/erfassungsmethode\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"erfassungsmethode\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Erfassungsmethode\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/kreisschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].kreisschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Kreis\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/id\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"id\",\n" +
//            "                        \"type\": \"ID\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Id\",\n" +
//            "                        \"type\": \"ID\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"ID\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/bemerkunginformationsquelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkunginformationsquelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bemerkung zur Informationsquelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/beobachtetam\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"beobachtetam\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Beobachtungsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/haeufigkeit\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"haeufigkeit\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Häufigkeit\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/bemerkungpopulationsstadium\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkungpopulationsstadium\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bemerkung zum Populationsstadium\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/verantwortlichestelle\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"verantwortlichestelle\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Verantwortliche Stelle\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/begehungsmethode\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"begehungsmethode\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Begehungsmethode\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/[id=artbeobachtung_id]artbeobachtung_2_erfasser\\/[erfasser_id=id]erfasser\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"erfasser[erfasser].bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hinweise zum Erfasser\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/bemerkungort\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkungort\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bermerkung zum Fundort\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/hauptfoto\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].hauptfoto\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hauptfoto?\",\n" +
//            "                        \"type\": \"BOOLEAN\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"BOOLEAN\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/gemeindeschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].gemeindeschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Gemeinde\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/nachweis\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"nachweis\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Nachweisart\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/aufnahmezeitpunkt\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].aufnahmezeitpunkt\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Aufnahmezeitpunkt\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/datumabgleich\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].datumabgleich\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Abgleichsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/anzahl\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"anzahl\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Anzahl\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/fotoverweis\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].fotoverweis\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Foto\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/tierart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"tierart\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Tierart\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/[geom=id]geom\\/geom\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"geometrie\",\n" +
//            "                        \"type\": \"SPATIAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": false,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"geometrie\",\n" +
//            "                        \"type\": \"GEOMETRY\",\n" +
//            "                        \"geometryType\": \"GENERIC\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"GEOMETRY\",\n" +
//            "                        \"geometryType\": \"GENERIC\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/[id=ortsangaben_id]ortsangaben_flurstueckskennzeichen\\/flurstueckskennzeichen\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].flurstueckskennzeichen[ortsangaben_flurstueckskennzeichen]\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Flurstück\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/veroeffentlichtam\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": true,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"veroeffentlichtam\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Einschpeicherungsdatum\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"itemType\": \"http:\\/\\/schema.org\\/Place\",\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"{{Full Name}}\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/unschaerfe\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"unschaerfe\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Unschärfe\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"NUMBER\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]artbeobachtung\\/letzteskartierdatum\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"letzteskartierdatum\",\n" +
//            "                        \"type\": \"TEMPORAL\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Kartierdurchlauf letzter Durchgang\",\n" +
//            "                        \"format\": \"d.MM.yyyy[', 'HH:mm:ss[' 'z]]\",\n" +
//            "                        \"type\": \"DATE\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_foto\\/[foto_id=id]foto\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"foto[foto].bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Hinweise\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/bemerkungtierart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkungtierart\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Bemerkung zum Populationsstadium\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/bemerkung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bemerkung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Administrative Hinweise\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/kennung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"kennung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Objektkennung\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_ortsangabe\\/[ortsangabe_id=id]ortsangaben\\/verbandsgemeindeschluessel\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].ortsangabe[ortsangabe].verbandsgemeindeschluessel\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Verbandsgemeinde\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/bezeichnung\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"bezeichnung\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Objektbezeichnung\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/[id=id]osirisobjekt\\/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz\\/[raumreferenz_id=id]raumreferenz\\/[id=raumreferenz_id]raumreferenz_2_fachreferenz\\/[fachreferenz_id=id]osirisobjekt\\/id:objektart\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"raumreferenz[raumreferenz].fachreferenz[osirisobjekt]\",\n" +
//            "                        \"pattern\": \"{{serviceUrl}}\\/collections\\/{{objektart}}\\/items\\/{{id}}\",\n" +
//            "                        \"type\": \"REFERENCE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"fachreferenz\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                },\n" +
//            "                \"\\/fundorttiere\\/vorkommen\":\n" +
//            "                {\n" +
//            "                    \"general\":\n" +
//            "                    {\n" +
//            "                        \"filterable\": false,\n" +
//            "                        \"mappingType\": \"GENERIC_PROPERTY\",\n" +
//            "                        \"name\": \"vorkommen\",\n" +
//            "                        \"type\": \"VALUE\",\n" +
//            "                        \"enabled\": true\n" +
//            "                    },\n" +
//            "                    \"text\\/html\":\n" +
//            "                    {\n" +
//            "                        \"showInCollection\": true,\n" +
//            "                        \"mappingType\": \"MICRODATA_PROPERTY\",\n" +
//            "                        \"name\": \"Status\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    },\n" +
//            "                    \"application\\/geo+json\":\n" +
//            "                    {\n" +
//            "                        \"mappingType\": \"GEO_JSON_PROPERTY\",\n" +
//            "                        \"type\": \"STRING\"\n" +
//            "                    }\n" +
//            "                }\n" +
//            "            }\n" +
//            "        },\n" +
//            "        \"connectionInfo\":\n" +
//            "        {\n" +
//            "            \"database\": \"artendaten\",\n" +
//            "            \"password\": \"postgres\",\n" +
//            "            \"host\": \"localhost\",\n" +
//            "            \"user\": \"postgres\"\n" +
//            "        },\n" +
//            "        \"providerType\": \"PGIS\"\n" +
//            "    }\n" +
//            "}";
//
}