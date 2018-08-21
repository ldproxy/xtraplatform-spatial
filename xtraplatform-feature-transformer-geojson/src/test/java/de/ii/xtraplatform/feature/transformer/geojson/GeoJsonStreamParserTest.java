/**
 * Copyright 2018 interactive instruments GmbH
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package de.ii.xtraplatform.feature.transformer.geojson;

import akka.actor.ActorSystem;
import akka.stream.ActorMaterializer;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.StreamConverters;
import akka.testkit.javadsl.TestKit;
import akka.util.ByteString;
import com.google.common.collect.ImmutableList;
import de.ii.xtraplatform.feature.query.api.FeatureConsumer;
import de.ii.xtraplatform.feature.transformer.api.ImmutableFeatureTypeMapping;
import de.ii.xtraplatform.feature.transformer.api.ImmutableSourcePathMapping;
import de.ii.xtraplatform.feature.transformer.api.LoggingFeatureTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;

/**
 * @author zahnen
 */
public class GeoJsonStreamParserTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GeoJsonStreamParserTest.class);


    static ActorSystem system;
    static ActorMaterializer materializer;

    @BeforeClass(groups = {"default"})
    public static void setup() {
        system = ActorSystem.create();
        materializer = ActorMaterializer.create(system);
    }

    @AfterClass(groups = {"default"})
    public static void teardown() {
        TestKit.shutdownActorSystem(system);
        system = null;
    }

    @Test
    public void test() {
        LoggingFeatureConsumer featureConsumer = new LoggingFeatureConsumer();

        Source.from(ImmutableList.of(ByteString.fromString(featureCollection)))
              .runWith(GeoJsonStreamParser.consume(featureConsumer), materializer)
              .toCompletableFuture()
              .join();

        LOGGER.debug("{}", featureConsumer.log);
    }

    @Test
    public void test2() {

        ImmutableFeatureTypeMapping mapping = ImmutableFeatureTypeMapping.builder()
                                                                         .putMappings("erfasser/name",
                                                                                 ImmutableSourcePathMapping.builder()
                                                                                                           .putMappings("SQL", new MappingSwapper.MappingReadFromWrite("/fundortpflanzen/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser[erfasser]/name", null))
                                                                                                           .build())
                                                                         .putMappings("foto/hauptfoto",
                                                                                 ImmutableSourcePathMapping.builder()
                                                                                                           .putMappings("SQL", new MappingSwapper.MappingReadFromWrite("/fundortpflanzen/[id=id]osirisobjekt/[id=osirisobjekt_id]osirisobjekt_2_foto/[foto_id=id]foto/hauptfoto", null))
                                                                                                           .build())
                                                                         .putMappings("geometry",
                                                                                 ImmutableSourcePathMapping.builder()
                                                                                                           .putMappings("SQL", new MappingSwapper.MappingReadFromWrite("/fundortpflanzen/[id=id]artbeobachtung/[geom=id]geom/geom", null))
                                                                                                           .build())

                                                                         .putMappings("raumreferenz/ortsangabe/kreisschluessel",
                                                                                 ImmutableSourcePathMapping.builder()
                                                                                                           .putMappings("SQL", new MappingSwapper.MappingReadFromWrite("/fundortpflanzen/[id=id]osirisobjekt/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz/[raumreferenz_id=id]raumreferenz/[id=raumreferenz_id]raumreferenz_2_ortsangabe/[ortsangabe_id=id]ortsangaben/kreisschluessel", null))
                                                                                                           .build())
                                                                         .putMappings("raumreferenz/ortsangabe/flurstueckskennzeichen",
                                                                                 ImmutableSourcePathMapping.builder()
                                                                                                           .putMappings("SQL", new MappingSwapper.MappingReadFromWrite("/fundortpflanzen/[id=id]osirisobjekt/[id=osirisobjekt_id]osirisobjekt_2_raumreferenz/[raumreferenz_id=id]raumreferenz/[id=raumreferenz_id]raumreferenz_2_ortsangabe/[ortsangabe_id=id]ortsangaben/[id=ortsangaben_id]ortsangaben_flurstueckskennzeichen/flurstueckskennzeichen", null))
                                                                                                           .build())
                                                                         .build();

        //for (int i = 0; i < 5; i++) {
            LoggingFeatureTransformer featureTransformer = new LoggingFeatureTransformer();

            InputStream inputStream = new ByteArrayInputStream(featureCollection.getBytes(StandardCharsets.UTF_8));
            StreamConverters.fromInputStream(() -> inputStream)
                  .runWith(GeoJsonStreamParser.transform(mapping, featureTransformer), materializer)
                  .toCompletableFuture()
                  .join();

            String actual = featureTransformer.toString();
            LOGGER.debug("{}", actual);

            assertEquals(actual, EXPECTED);
        //}
    }

    static final String EXPECTED = "START: 3 12\n" +
            "NOMAPPING{\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[geom=id]geom/geom|MULTI_POINT: \n" +
            "NESTOPEN\n" +
            "6.295202392345018 50.11336914792363 \n" +
            "NESTCLOSE\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser[erfasser]/name[[1, 1]]: Schausten, Hermann\n" +
            "}\n" +
            "NOMAPPING{\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[geom=id]geom/geom|MULTI_POINT: \n" +
            "NESTOPEN\n" +
            "6.406233970262905 50.1501333536934 \n" +
            "NESTCLOSE\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser[erfasser]/name[[1, 1]]: Schausten, Hermann\n" +
            "}\n" +
            "NOMAPPING{\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[geom=id]geom/geom|MULTI_POLYGON: \n" +
            "NESTOPEN\n" +
            "NESTOPEN\n" +
            "NESTOPEN\n" +
            "8.18523495507722 49.698295103021096 8.185283687843047 49.69823291309017 8.185340750477529 49.698164995459464 8.185391481556831 49.69814732787338 8.185557748451103 49.69821659947788 8.185681115675656 49.698286680057166 8.185796151881165 49.69836248910692 8.185961921022393 49.698420628419534 8.186049719358214 49.6984635312452 8.186164506999352 49.69853377407446 8.186313615874417 49.698603368350874 8.18641074595947 49.69866280390489 8.186559354976497 49.698721267043595 8.186734205060421 49.698790375060966 8.186890646862924 49.69883198009526 8.187089988207557 49.698872774294806 8.187349890545597 49.69892356343489 8.187601463548328 49.698980080734394 8.187783395765324 49.699015632752 8.187982738625365 49.69905642542124 8.188130851166061 49.69910375574693 8.188168673707677 49.69918102154122 8.1880944531289 49.69924926460418 8.188020232326654 49.699317508517055 8.187821138132422 49.69928228086303 8.187691935612703 49.69927358265798 8.187586973691364 49.69923100592957 8.187413620449282 49.699195291629444 8.187283418074694 49.699164331341315 8.187169126694274 49.69910522042357 8.1870046048774 49.69907490852495 8.186813592751827 49.6990283863928 8.186665980985028 49.698992185201874 8.186490380076323 49.69890638130109 8.186386170170767 49.698880499270615 8.186332690454336 49.69883694914269 8.186262800444936 49.69881041853736 8.186182830746077 49.69875065910831 8.186034471654624 49.698697760446365 8.18587603337319 49.69861163188807 8.18569210521939 49.698531554501336 8.18558489839894 49.69843888772686 8.185479191668042 49.698379613325045 8.185356323459409 49.698320662450435 8.185295763987918 49.69831066523123 8.18523495507722 49.698295103021096 \n" +
            "NESTCLOSE\n" +
            "NESTCLOSE\n" +
            "NESTCLOSE\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser[erfasser]/name[[1, 1]]: Bösl, Matthias\n" +
            "    /fundortpflanzen/[id=id]artbeobachtung/[id=artbeobachtung_id]artbeobachtung_2_erfasser/[erfasser_id=id]erfasser[erfasser]/name[[2, 1]]: Bösl, Matthias\n" +
            "}\n" +
            "END\n";

    static class LoggingFeatureConsumer implements FeatureConsumer {
        public StringBuilder log = new StringBuilder();

        @Override
        public void onStart(OptionalLong numberReturned, OptionalLong numberMatched) throws Exception {
            log.append(String.format("START: %d %d\n", numberReturned.orElse(-1), numberMatched.orElse(-1)));
        }

        @Override
        public void onEnd() throws Exception {
            log.append(String.format("END\n"));
        }

        @Override
        public void onFeatureStart(List<String> path) throws Exception {
            log.append(path);
            log.append("{\n");
        }

        @Override
        public void onFeatureEnd(List<String> path) throws Exception {
            log.append("}\n");
        }

        @Override
        public void onPropertyStart(List<String> path, List<Integer> multiplicities) throws Exception {
            log.append("    ");
            log.append(path);
            log.append(": ");
            log.append(multiplicities);
            log.append(":");
        }

        @Override
        public void onPropertyText(String text) throws Exception {
            log.append(text);
        }

        @Override
        public void onPropertyEnd(List<String> path) throws Exception {
            log.append("\n");
            log.append("   >");
            log.append(path);
            log.append("\n");
        }
    }

    static final String featureCollection = "{\n" +
            "  \"type\" : \"FeatureCollection\",\n" +
            "  \"links\" : [ {\n" +
            "    \"href\" : \"http://localhost:7080/rest/services/oneo/collections/fundorttiere/items?f=json&count=3&offset=5\",\n" +
            "    \"rel\" : \"self\",\n" +
            "    \"type\" : \"application/json\",\n" +
            "    \"title\" : \"this document\"\n" +
            "  }, {\n" +
            "    \"href\" : \"http://localhost:7080/rest/services/oneo/collections/fundorttiere/items?count=3&offset=5&f=html\",\n" +
            "    \"rel\" : \"alternate\",\n" +
            "    \"type\" : \"text/html\",\n" +
            "    \"title\" : \"this document as HTML\"\n" +
            "  }, {\n" +
            "    \"href\" : \"http://localhost:7080/rest/services/oneo/collections/fundorttiere/items?f=json&page=3&limit=3\",\n" +
            "    \"rel\" : \"next\",\n" +
            "    \"type\" : \"application/json\",\n" +
            "    \"title\" : \"next page\"\n" +
            "  }, {\n" +
            "    \"href\" : \"http://localhost:7080/rest/services/oneo/collections/fundorttiere/items?f=json&page=1&limit=3\",\n" +
            "    \"rel\" : \"prev\",\n" +
            "    \"type\" : \"application/json\",\n" +
            "    \"title\" : \"previous page\"\n" +
            "  } ],\n" +
            "  \"numberReturned\" : 3,\n" +
            "  \"numberMatched\" : 12,\n" +
            "  \"timeStamp\" : \"2018-07-22T09:45:54Z\",\n" +
            "  \"features\" : [ {\n" +
            "    \"type\" : \"Feature\",\n" +
            "    \"id\" : \"19\",\n" +
            "    \"geometry\" : {\n" +
            "      \"type\" : \"Point\",\n" +
            "      \"coordinates\" : [ 6.295202392345018, 50.11336914792363 ]\n" +
            "    },\n" +
            "    \"properties\" : {\n" +
            "      \"kennung\" : \"580340001-1\",\n" +
            "      \"bezeichnung\" : \"Buteo buteo\",\n" +
            "      \"verantwortlichestelle\" : \"710121\",\n" +
            "      \"anzahl\" : \"0\",\n" +
            "      \"begehungsmethode\" : \"710311\",\n" +
            "      \"beobachtetam\" : \"2010-03-01\",\n" +
            "      \"haeufigkeit\" : \"\",\n" +
            "   \"raumreferenz\" : [ {\n" +
            "      \"ortsangabe\" : [ {\n" +
            "        \"kreisschluessel\" : \"11\",\n" +
            "        \"flurstueckskennzeichen\" : \"34\",\n" +
            "        \"flurstueckskennzeichen\" : \"35\",\n" +
            "        \"flurstueckskennzeichen\" : \"36\"\n" +
            "      }, {\n" +
            "        \"flurstueckskennzeichen\" : \"37\"\n" +
            "      }, {\n" +
            "        \"kreisschluessel\" : \"12\",\n" +
            "        \"flurstueckskennzeichen\" : \"39\",\n" +
            "        \"flurstueckskennzeichen\" : \"40\"\n" +
            "      } ]\n" +
            "    } ]," +
            "      \"informationsquelle\" : \"710316\",\n" +
            "      \"unschaerfe\" : \"0\",\n" +
            "      \"erfasser\" : [ {\n" +
            "        \"name\" : \"Schausten, Hermann\"\n" +
            "      } ],\n" +
            "      \"tierart\" : \"103010\",\n" +
            "      \"vorkommen\" : \"708780\",\n" +
            "      \"nachweis\" : \"708763\",\n" +
            "      \"erfassungsmethode\" : \"710324\"\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"type\" : \"Feature\",\n" +
            "    \"id\" : \"20\",\n" +
            "    \"geometry\" : {\n" +
            "      \"type\" : \"MultiPoint\",\n" +
            "      \"coordinates\" : [ [ 6.406233970262905, 50.1501333536934 ] ]\n" +
            "    },\n" +
            "    \"properties\" : {\n" +
            "      \"kennung\" : \"580410003-1\",\n" +
            "      \"bezeichnung\" : \"Buteo buteo\",\n" +
            "      \"verantwortlichestelle\" : \"710121\",\n" +
            "      \"anzahl\" : \"0\",\n" +
            "      \"begehungsmethode\" : \"710311\",\n" +
            "      \"beobachtetam\" : \"2010-03-01\",\n" +
            "      \"haeufigkeit\" : \"\",\n" +
            "      \"informationsquelle\" : \"710316\",\n" +
            "      \"unschaerfe\" : \"0\",\n" +
            "      \"erfasser\" : [ {\n" +
            "        \"name\" : \"Schausten, Hermann\"\n" +
            "      } ],\n" +
            "      \"tierart\" : \"103010\",\n" +
            "      \"vorkommen\" : \"708780\",\n" +
            "      \"nachweis\" : \"708763\",\n" +
            "      \"erfassungsmethode\" : \"710324\"\n" +
            "    }\n" +
            "  }, {\n" +
            "    \"type\" : \"Feature\",\n" +
            "    \"id\" : \"21\",\n" +
            "    \"geometry\" : {\n" +
            "      \"type\" : \"MultiPolygon\",\n" +
//            "      \"coordinates\" : [ [ [ [ 8.18523495507722, 49.698295103021096 ], [ 8.185283687843047, 49.69823291309017 ], [ 8.185340750477529, 49.698164995459464 ], [ 8.185391481556831, 49.69814732787338 ], [ 8.185557748451103, 49.69821659947788 ], [ 8.185681115675656, 49.698286680057166 ], [ 8.185796151881165, 49.69836248910692 ], [ 8.185961921022393, 49.698420628419534 ], [ 8.186049719358214, 49.6984635312452 ], [ 8.186164506999352, 49.69853377407446 ], [ 8.186313615874417, 49.698603368350874 ], [ 8.18641074595947, 49.69866280390489 ], [ 8.186559354976497, 49.698721267043595 ], [ 8.186734205060421, 49.698790375060966 ], [ 8.186890646862924, 49.69883198009526 ], [ 8.187089988207557, 49.698872774294806 ], [ 8.187349890545597, 49.69892356343489 ], [ 8.187601463548328, 49.698980080734394 ], [ 8.187783395765324, 49.699015632752 ], [ 8.187982738625365, 49.69905642542124 ], [ 8.188130851166061, 49.69910375574693 ], [ 8.188168673707677, 49.69918102154122 ], [ 8.1880944531289, 49.69924926460418 ], [ 8.188020232326654, 49.699317508517055 ], [ 8.187821138132422, 49.69928228086303 ], [ 8.187691935612703, 49.69927358265798 ], [ 8.187586973691364, 49.69923100592957 ], [ 8.187413620449282, 49.699195291629444 ], [ 8.187283418074694, 49.699164331341315 ], [ 8.187169126694274, 49.69910522042357 ], [ 8.1870046048774, 49.69907490852495 ], [ 8.186813592751827, 49.6990283863928 ], [ 8.186665980985028, 49.698992185201874 ], [ 8.186490380076323, 49.69890638130109 ], [ 8.186386170170767, 49.698880499270615 ], [ 8.186332690454336, 49.69883694914269 ], [ 8.186262800444936, 49.69881041853736 ], [ 8.186182830746077, 49.69875065910831 ], [ 8.186034471654624, 49.698697760446365 ], [ 8.18587603337319, 49.69861163188807 ], [ 8.18569210521939, 49.698531554501336 ], [ 8.18558489839894, 49.69843888772686 ], [ 8.185479191668042, 49.698379613325045 ], [ 8.185356323459409, 49.698320662450435 ], [ 8.185295763987918, 49.69831066523123 ], [ 8.18523495507722, 49.698295103021096 ] ] ] ]\n" +
            "      \"coordinates\" : [ [ [ [ 7.5529442171247965, 49.493747559742744 ], [ 7.552884643138733, 49.493838386437226 ], [ 7.552758707346035, 49.493969221367905 ], [ 7.55271390000454, 49.49402757984274 ], [ 7.552630129427795, 49.49401168936384 ], [ 7.552569611273632, 49.49407362920248 ], [ 7.552638941093539, 49.49412028281182 ], [ 7.552544266271485, 49.49425402737361 ], [ 7.5523036894588325, 49.49454930019701 ], [ 7.552240664858226, 49.49452296471254 ], [ 7.552299839901534, 49.494427006046394 ], [ 7.552230748835393, 49.49437693701518 ], [ 7.552187238854002, 49.494316393810834 ], [ 7.552086351926784, 49.49425342322965 ], [ 7.55202151639964, 49.49417615161781 ], [ 7.551892331783519, 49.4942017000574 ], [ 7.551788069028163, 49.4941689111182 ], [ 7.551826049450407, 49.494098593740176 ], [ 7.551871240204275, 49.49404131984126 ], [ 7.55196667017906, 49.493896071305 ], [ 7.552022283226857, 49.49385062170393 ], [ 7.552161689724859, 49.49393260483045 ], [ 7.552273635913913, 49.49388750045071 ], [ 7.55231278686695, 49.49384769159441 ], [ 7.552243916663433, 49.49378548168754 ], [ 7.552370520044429, 49.4937771715063 ], [ 7.552389943961042, 49.493750698605254 ], [ 7.55249792804327, 49.49377903593309 ], [ 7.5525766996382915, 49.49380520853418 ], [ 7.5526475677177185, 49.493902704774264 ], [ 7.552771534752741, 49.49388077827793 ], [ 7.552837731322171, 49.49383231236109 ], [ 7.552767936577701, 49.49376865195431 ], [ 7.552708880225525, 49.4937014378064 ], [ 7.552593104385553, 49.493665716984324 ], [ 7.552505691088453, 49.493609481930264 ], [ 7.552629256824502, 49.49350749248846 ], [ 7.55270568480668, 49.49339303930145 ], [ 7.552762193997587, 49.493270331019836 ], [ 7.5528836745869565, 49.49309851176491 ], [ 7.553017582696796, 49.49311403983936 ], [ 7.553116106475518, 49.49306040110632 ], [ 7.553161305548863, 49.49300744454501 ], [ 7.553092829080204, 49.49296466988673 ], [ 7.5530057577510785, 49.49294837856809 ], [ 7.552906205785104, 49.49297600795547 ], [ 7.552929897911298, 49.49287969495895 ], [ 7.553119821365876, 49.49288755810456 ], [ 7.5533981700880375, 49.49296884905244 ], [ 7.553437676255432, 49.493066852096554 ], [ 7.553372478056704, 49.49314240666606 ], [ 7.553239903240621, 49.49339549502391 ], [ 7.553058918008086, 49.49357151766177 ], [ 7.552984391443451, 49.49354656788287 ], [ 7.552905590391344, 49.49358228277943 ], [ 7.5527936643467415, 49.493636113451544 ], [ 7.552809148973741, 49.493705750831786 ], [ 7.5529442171247965, 49.493747559742744 ] ], [ [ 7.552818779493506, 49.49336980732074 ], [ 7.552835297806458, 49.49346995166553 ], [ 7.552902326839939, 49.493482214330854 ], [ 7.55299616309122, 49.49349391280596 ], [ 7.553082006676886, 49.4934535184459 ], [ 7.553146369713969, 49.49338290100387 ], [ 7.553057538347426, 49.493318552865205 ], [ 7.552818779493506, 49.49336980732074 ] ] ], [ [ [ 7.552555288420557, 49.492756629505166 ], [ 7.552273396869156, 49.49272530598879 ], [ 7.552210978494883, 49.492636370835086 ], [ 7.552373162684743, 49.49265765077779 ], [ 7.552406782303064, 49.49259465475674 ], [ 7.552273424460295, 49.49254180239775 ], [ 7.552403049923132, 49.49229047724774 ], [ 7.552550640524104, 49.492038657150495 ], [ 7.552725624781281, 49.492098776547124 ], [ 7.552857137686028, 49.492097093841494 ], [ 7.5531489068013045, 49.4920696203537 ], [ 7.553173954506901, 49.49210060762555 ], [ 7.552971225430013, 49.49230235453913 ], [ 7.5528159514390465, 49.49249165170339 ], [ 7.552636843070686, 49.492693063903 ], [ 7.552555288420557, 49.492756629505166 ] ] ], [ [ [ 7.552996469433779, 49.49281846032824 ], [ 7.55263929565359, 49.49276910479985 ], [ 7.552777906347934, 49.49261108289434 ], [ 7.552832600606915, 49.492545202095094 ], [ 7.5529608934126005, 49.492582518538825 ], [ 7.552965895647259, 49.49253463622221 ], [ 7.552917568471554, 49.49248293834223 ], [ 7.55310710394282, 49.49225565975703 ], [ 7.553254203299175, 49.49214685775781 ], [ 7.553322543394208, 49.492189540670836 ], [ 7.553427707052797, 49.49214012255596 ], [ 7.553345958210619, 49.4920932241121 ], [ 7.553305434355429, 49.49207193812299 ], [ 7.553484427948155, 49.49190704433159 ], [ 7.553604773743146, 49.491932836659366 ], [ 7.5537334091068935, 49.49184134396748 ], [ 7.553803831950263, 49.49176648418507 ], [ 7.553875791263615, 49.49178006728419 ], [ 7.553813110126492, 49.49186869675237 ], [ 7.5537620016968425, 49.49193480275923 ], [ 7.553711965474871, 49.49204886705171 ], [ 7.553584154271235, 49.49222951335773 ], [ 7.553431854220385, 49.492266468308856 ], [ 7.553346749339391, 49.49232873072461 ], [ 7.553370037772073, 49.49242877977982 ], [ 7.553228393168773, 49.492600797094575 ], [ 7.5530593155559425, 49.492738378897286 ], [ 7.552967883796327, 49.4928137849604 ], [ 7.552996469433779, 49.49281846032824 ] ] ], [ [ [ 7.553270686054621, 49.49288062987578 ], [ 7.553244922181469, 49.49287392095634 ], [ 7.553321067164613, 49.49277835392725 ], [ 7.55333948326678, 49.492734507349866 ], [ 7.553225957263389, 49.492688110894484 ], [ 7.553282583400324, 49.49261757649903 ], [ 7.55333413711487, 49.49256451944752 ], [ 7.5533846186892255, 49.49246350406474 ], [ 7.553415836942052, 49.49239759922552 ], [ 7.553439535938296, 49.492305693898196 ], [ 7.553612144075011, 49.492268453181175 ], [ 7.553664417143541, 49.492228448149184 ], [ 7.553778226381416, 49.49202621788501 ], [ 7.55385435416931, 49.49190303474081 ], [ 7.553918529605499, 49.49181055595951 ], [ 7.553969044704028, 49.49172708155218 ], [ 7.55417051509074, 49.49175064920437 ], [ 7.554214957763229, 49.49182172783735 ], [ 7.554027097052586, 49.49197364863125 ], [ 7.553832498050448, 49.492227671372504 ], [ 7.553650691290773, 49.49256299126723 ], [ 7.553474715601982, 49.49292527971017 ], [ 7.553270686054621, 49.49288062987578 ] ] ], [ [ [ 7.5515795274260835, 49.496012852465036 ], [ 7.551180526203469, 49.49607909788146 ], [ 7.55081359755741, 49.49609276153601 ], [ 7.550900030017955, 49.49587363981769 ], [ 7.551065981603906, 49.49562268140606 ], [ 7.551089668113938, 49.4955220510033 ], [ 7.5507913823610595, 49.495390670872844 ], [ 7.550493533412628, 49.495272608465484 ], [ 7.550370678621395, 49.4951912805604 ], [ 7.55041029110022, 49.4951733366833 ], [ 7.550573644761023, 49.49526281805745 ], [ 7.55079638178268, 49.49533829098468 ], [ 7.551033402274394, 49.49543984884432 ], [ 7.551209424076389, 49.49550726964843 ], [ 7.55122080002275, 49.49565988226221 ], [ 7.55122643099302, 49.4958386891489 ], [ 7.551441052925303, 49.49586638414733 ], [ 7.5515627348328875, 49.49591270454177 ], [ 7.5515795274260835, 49.496012852465036 ] ] ], [ [ [ 7.5515737156588845, 49.49562022537129 ], [ 7.551299602981012, 49.49561958093809 ], [ 7.551269685681862, 49.49551962838538 ], [ 7.551059164633962, 49.49540455023887 ], [ 7.550808445548738, 49.495294545131806 ], [ 7.550463445653463, 49.49515511274364 ], [ 7.550558733394615, 49.495005545853374 ], [ 7.5505976211081975, 49.4949700520171 ], [ 7.550644907919274, 49.49498683608973 ], [ 7.550716057542052, 49.49511681002101 ], [ 7.550817860397811, 49.495172147270054 ], [ 7.55097221536862, 49.49518757406334 ], [ 7.551127345592083, 49.495242229816924 ], [ 7.551209392147368, 49.495297858953116 ], [ 7.551267171457319, 49.495441068034225 ], [ 7.551328882606732, 49.49548384896843 ], [ 7.551397546909713, 49.49554839559494 ], [ 7.551476625645056, 49.495508097604336 ], [ 7.551577527266352, 49.49552852117449 ], [ 7.5515737156588845, 49.49562022537129 ] ] ], [ [ [ 7.552671884894208, 49.49329825576477 ], [ 7.5525605612385345, 49.49323803347498 ], [ 7.552439488130321, 49.4932134003597 ], [ 7.552321325869818, 49.4931275456246 ], [ 7.552523809518959, 49.49294837496464 ], [ 7.552602984163517, 49.49282505087322 ], [ 7.552889538656015, 49.49287154401153 ], [ 7.5528288940288935, 49.49306427392739 ], [ 7.552720067398485, 49.49320971536083 ], [ 7.552671884894208, 49.49329825576477 ] ] ], [ [ [ 7.5519063991669775, 49.496004082054576 ], [ 7.551693260883794, 49.49602435137782 ], [ 7.551739757979651, 49.49580122081518 ], [ 7.551781569711501, 49.495647924941345 ], [ 7.551973623785657, 49.49558862181592 ], [ 7.552082785872236, 49.49566141097427 ], [ 7.5518650917057615, 49.49574719921824 ], [ 7.5518343313719685, 49.49583487794395 ], [ 7.551908871535082, 49.49586882390095 ], [ 7.5518981339142135, 49.49595189586652 ], [ 7.5519063991669775, 49.496004082054576 ] ] ] ]\n" +
            "    },\n" +
            "    \"properties\" : {\n" +
            "      \"kennung\" : \"631510001-1\",\n" +
            "      \"bezeichnung\" : \"Gundersheim, südlich der Autobahn\",\n" +
            "      \"verantwortlichestelle\" : \"710121\",\n" +
            "      \"anzahl\" : \"0\",\n" +
            "      \"begehungsmethode\" : \"710311\",\n" +
            "      \"beobachtetam\" : \"2000-03-01\",\n" +
            "      \"haeufigkeit\" : \"\",\n" +
            "      \"informationsquelle\" : \"710316\",\n" +
            "      \"unschaerfe\" : \"0\",\n" +
            "      \"erfasser\" : [ {\n" +
            "        \"name\" : \"Bösl, Matthias\"\n" +
            "      }, {\n" +
            "        \"name\" : \"Bösl, Matthias\"\n" +
            "      } ],\n" +
            "      \"tierart\" : \"103061\",\n" +
            "      \"vorkommen\" : \"708780\",\n" +
            "      \"nachweis\" : \"708763\",\n" +
            "      \"erfassungsmethode\" : \"710324\"\n" +
            "    }\n" +
            "  } ]\n" +
            "}";

    static final String singleFeature = "{\n" +
            "  \"type\" : \"Feature\",\n" +
            //"  \"id\" : \"24\",\n" +
            "  \"geometry\" : {\n" +
            "      \"type\" : \"MultiPolygon\",\n" +
            "      \"coordinates\" : [ [ [ [ 8.18523495507722, 49.698295103021096 ], [ 8.185283687843047, 49.69823291309017 ], [ 8.185340750477529, 49.698164995459464 ], [ 8.185391481556831, 49.69814732787338 ], [ 8.185557748451103, 49.69821659947788 ], [ 8.185681115675656, 49.698286680057166 ], [ 8.185796151881165, 49.69836248910692 ], [ 8.185961921022393, 49.698420628419534 ], [ 8.186049719358214, 49.6984635312452 ], [ 8.186164506999352, 49.69853377407446 ], [ 8.186313615874417, 49.698603368350874 ], [ 8.18641074595947, 49.69866280390489 ], [ 8.186559354976497, 49.698721267043595 ], [ 8.186734205060421, 49.698790375060966 ], [ 8.186890646862924, 49.69883198009526 ], [ 8.187089988207557, 49.698872774294806 ], [ 8.187349890545597, 49.69892356343489 ], [ 8.187601463548328, 49.698980080734394 ], [ 8.187783395765324, 49.699015632752 ], [ 8.187982738625365, 49.69905642542124 ], [ 8.188130851166061, 49.69910375574693 ], [ 8.188168673707677, 49.69918102154122 ], [ 8.1880944531289, 49.69924926460418 ], [ 8.188020232326654, 49.699317508517055 ], [ 8.187821138132422, 49.69928228086303 ], [ 8.187691935612703, 49.69927358265798 ], [ 8.187586973691364, 49.69923100592957 ], [ 8.187413620449282, 49.699195291629444 ], [ 8.187283418074694, 49.699164331341315 ], [ 8.187169126694274, 49.69910522042357 ], [ 8.1870046048774, 49.69907490852495 ], [ 8.186813592751827, 49.6990283863928 ], [ 8.186665980985028, 49.698992185201874 ], [ 8.186490380076323, 49.69890638130109 ], [ 8.186386170170767, 49.698880499270615 ], [ 8.186332690454336, 49.69883694914269 ], [ 8.186262800444936, 49.69881041853736 ], [ 8.186182830746077, 49.69875065910831 ], [ 8.186034471654624, 49.698697760446365 ], [ 8.18587603337319, 49.69861163188807 ], [ 8.18569210521939, 49.698531554501336 ], [ 8.18558489839894, 49.69843888772686 ], [ 8.185479191668042, 49.698379613325045 ], [ 8.185356323459409, 49.698320662450435 ], [ 8.185295763987918, 49.69831066523123 ], [ 8.18523495507722, 49.698295103021096 ] ] ] ]\n" +
            "  },\n" +
            "  \"properties\" : {\n" +
            "    \"kennung\" : \"611320001-1\",\n" +
            "    \"bezeichnung\" : \"Gensingen, Globus-Markt, Binger Straße 1\",\n" +
            "    \"verantwortlichestelle\" : \"710121\",\n" +
            "      \"foto\" : [ {\n" +
            "        \"hauptfoto\" : \"11\"\n" +
            "      }, {\n" +
            "        \"hauptfoto\" : \"37\"\n" +
            "      }, {\n" +
            "        \"hauptfoto\" : \"12\",\n" +
            "        \"bemerkung\" : \"39\"\n" +
            "      } ],\n" +
            "    \"anzahl\" : \"0\",\n" +
            "    \"begehungsmethode\" : \"710311\",\n" +
            "    \"beobachtetam\" : \"2000-05-05\",\n" +
            "    \"haeufigkeit\" : \"\",\n" +
            "   \"raumreferenz\" : [ {\n" +
            "      \"ortsangabe\" : [ {\n" +
            "        \"kreisschluessel\" : \"11\"\n" +
            "      }, {\n" +
            "        \"flurstueckskennzeichen\" : [\"37\"]\n" +
            "      }, {\n" +
            "        \"kreisschluessel\" : \"12\",\n" +
            "        \"flurstueckskennzeichen\" : [\"39\", \"40\"]\n" +
            "      } ]\n" +
            "    } ]," +
            "    \"informationsquelle\" : \"710316\",\n" +
            "    \"unschaerfe\" : \"0\",\n" +
            "    \"erfasser\" : [ {\n" +
            "      \"name\" : \"Simon, Ludwig\"\n" +
            "    } ],\n" +
            "    \"tierart\" : \"103061\",\n" +
            "    \"vorkommen\" : \"708780\",\n" +
            "    \"nachweis\" : \"708763\",\n" +
            "    \"erfassungsmethode\" : \"710324\"\n" +
            "  }\n" +
            "}";
}