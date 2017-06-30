package com.dataloom.integrations.iowacity;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;
import retrofit2.Retrofit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class JohnsonCountyMugshots {
    private static final Logger                      logger                    = LoggerFactory
            .getLogger( JohnsonCountyMugshots.class );
    private static final Map<String, String>         imageIdToMni              = new HashMap<>( 130486 );
    private static final Base64.Encoder              encoder                   = Base64.getEncoder();
    public static        String                      SUBJECTS_ENTITY_SET_NAME  = "jcsubjects";
    public static        FullQualifiedName           SUBJECTS_ENTITY_SET_TYPE  = new FullQualifiedName( "nc.PersonType" );
    public static        FullQualifiedName           PERSON_XREF_FQN           = new FullQualifiedName(
            "publicsafety.xref" );
    public static        FullQualifiedName           SUBJECTS_ENTITY_SET_KEY_1 = PERSON_XREF_FQN;
    public static        String                      SUBJECTS_ALIAS            = "subjects";
    public static        FullQualifiedName           MUG_SHOT_FQN              = new FullQualifiedName(
            "publicsafety.mugshot" );
    public static        RetrofitFactory.Environment environment               = RetrofitFactory.Environment.STAGING;

    public static void main( String[] args ) throws InterruptedException {
        if ( args.length < 5 ) {
            System.err.println( "Expected: <photoCSV> <smartIndexCSV> <photoDir> <jwtToken>" );
        }
        final String extractedPhotosCSV = args[ 1 ];
        final String smartIndexCSV = args[ 2 ];
        final String photoDirectory = args[ 3 ];
        final String jwtToken = args[ 4 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );
        UUID MugShot = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        MUG_SHOT_FQN,
                        "Mug Shot",
                        Optional.of( "An image for a subject." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Binary,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( MugShot == null ) {
            MugShot = edm.getPropertyTypeId( MUG_SHOT_FQN.getNamespace(), MUG_SHOT_FQN.getName() );
        }

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

        //Create index for MNIs
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

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( SUBJECTS_ALIAS )
                .ofType( SUBJECTS_ENTITY_SET_TYPE )
                .to( SUBJECTS_ENTITY_SET_NAME )
                .key( SUBJECTS_ENTITY_SET_KEY_1 )
//                .useCurrentSync()
                .addProperty( PERSON_XREF_FQN )
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
                    return mni;
                } )
                .ok()
                .addProperty( MUG_SHOT_FQN )
                .value( row -> {
                    String photoFile = row.getAs( "ExtractFilePath" );
                    try {
                        return getBase64Image( photoDirectory, photoFile );
                    } catch ( IOException e ) {
                        logger.error( "Unable to open photo at: ",
                                new File( photoDirectory, photoFile ).getAbsolutePath() );
                        return "";
                    }
                } ).ok().ok()
                .ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

    public static String getBase64Image( String photoDirectory, String filename ) throws IOException {
        return encoder.encodeToString( Files.readAllBytes( new File( photoDirectory, filename ).toPath() ) );
    }

}
