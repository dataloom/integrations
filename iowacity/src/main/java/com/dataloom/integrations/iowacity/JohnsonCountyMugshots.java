package com.dataloom.integrations.iowacity;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.mappers.ObjectMappers;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class JohnsonCountyMugshots {
    private static final Logger              logger         = LoggerFactory.getLogger( JohnsonCountyMugshots.class );
    private static final Map<String, String> imageIdToMni   = new HashMap<>( 130486 );
    private static final Base64.Encoder      encoder        = Base64.getEncoder();

    public static String                     SUBJECT_ID_FQN = "nc.SubjectIdentification";

    public static String                     SUBJECTS_NAME  = "jcsubjects";
    public static String                     SUBJECTS_ALIAS = "subject";

    public static String                     MUGSHOT_FQN    = "publicsafety.mugshot";
    public static Environment                environment    = RetrofitFactory.Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException {
        if ( args.length < 5 ) {
            System.err.println( "Expected: <photoCSV> <smartIndexCSV> <photoDir> <jwtToken>" );
        }
        final String extractedPhotosCSV = args[ 1 ];
        final String smartIndexCSV = args[ 2 ];
        final String photoDirectory = args[ 3 ];
        final String jwtToken = args[ 4 ];

        final SparkSession sparkSession = MissionControl.getSparkSession();

        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( extractedPhotosCSV );

        Dataset<Row> smartIndex = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( smartIndexCSV );

        // Create index for MNIs
        smartIndex.toLocalIterator().forEachRemaining( row -> {
            final String imageId = row.getAs( "Image_ID" );
            final String indexId = row.getAs( "Index_ID" );
            if ( StringUtils.isBlank( imageId ) ) {
                logger.warn( "Image id is blank for mni {}", indexId );
            }

            if ( StringUtils.isBlank( indexId ) ) {
                logger.warn( "index id is blank for mni {}", imageId );
            }
            if ( imageIdToMni.putIfAbsent( imageId, indexId ) != null ) {
                logger.error( "Duplicate image id {} for mni {}", imageId, indexId );
            }
        } );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( SUBJECTS_ALIAS )
                .to( SUBJECTS_NAME )
                .useCurrentSync()
                .addProperty( SUBJECT_ID_FQN )
                .value( row -> {
                    final String imageId = row.getAs( "Image_ID" );

                    if ( StringUtils.isBlank( imageId ) ) {
                        logger.warn( "This isn't going well. Found a blank image id" );
                        return "";
                    }

                    final String mni = imageIdToMni.getOrDefault( imageId, "" );

                    if ( StringUtils.isBlank( mni ) ) {
                        logger.warn( "Unable to find MNI for image id {}", imageId );
                    }
                    return "{" + mni + "}";
                } )
                .ok()
                .addProperty( MUGSHOT_FQN )
                .value( row -> {
                    String photoFile = row.getAs( "ExtractFilePath" );
                    try {
                        return getBase64Image( photoDirectory, photoFile );
                    } catch ( IOException e ) {
                        logger.error( "Unable to open photo at: ",
                                new File( photoDirectory, photoFile ).getAbsolutePath() );
                        return "";
                    }
                } ).ok()
                .endEntity()
                .endEntities()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

    public static String getBase64Image( String photoDirectory, String filename ) throws IOException {
        return encoder.encodeToString( Files.readAllBytes( new File( photoDirectory, filename ).toPath() ) );
    }

}
