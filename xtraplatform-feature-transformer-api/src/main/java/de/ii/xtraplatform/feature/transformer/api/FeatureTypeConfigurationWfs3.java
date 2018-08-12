package de.ii.xtraplatform.feature.transformer.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import de.ii.xtraplatform.crs.api.BoundingBox;
import org.immutables.value.Value;

/**
 * @author zahnen
 */
@Value.Immutable
@Value.Modifiable
@Value.Style(deepImmutablesDetection = true)
@JsonDeserialize(as = ImmutableFeatureTypeConfigurationWfs3.class)
public abstract class FeatureTypeConfigurationWfs3 extends FeatureTypeConfiguration {

    //public abstract TemporalExtent getTemporalExtent();

    //public abstract BoundingBox getSpatialExtent();

    public abstract FeatureTypeExtent getExtent();
    //TODO
    public static class FeatureTypeExtent {
        TemporalExtent temporal;

        BoundingBox spatial;

        public FeatureTypeExtent() {
        }

        public FeatureTypeExtent(TemporalExtent temporal, BoundingBox spatial) {
            this.temporal = temporal;
            this.spatial = spatial;
        }

        public TemporalExtent getTemporal() {
            return temporal;
        }

        public void setTemporal(TemporalExtent temporal) {
            this.temporal = temporal;
        }

        public BoundingBox getSpatial() {
            return spatial;
        }

        public void setSpatial(BoundingBox spatial) {
            this.spatial = spatial;
        }
    }
}
