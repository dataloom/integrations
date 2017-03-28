package com.dataloom.integrations.dataintegration;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.UUID;

public class DataIntegration {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( DataIntegration.class );

    // Set your Entity Set Name, Type, and Primary Key
    public static String ENTITY_SET_NAME = "openflightsdata";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "sample.openflightsdata" );
    public static FullQualifiedName ENTITY_SET_KEY = new FullQualifiedName( "aviation.id" );

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String path = args[ 0 ];
        final String username = args[ 1 ];
        final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        final String jwtToken = MissionControl.getIdToken( username, password );
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Dataset<Row> payload = sparkSession
            .read()
            .format( "com.databricks.spark.csv" )
            .option( "header", "true" )
            .load( path );

        Flight flight = Flight.newFlight()
            .addEntity( ENTITY_SET_TYPE )
                .to( ENTITY_SET_NAME )
                .key( ENTITY_SET_KEY )
                .addProperty( new FullQualifiedName( "aviation.name" ) )
                    .value( row -> row.getAs( " Name" ) ).ok()
                .addProperty( new FullQualifiedName( "aviation.id" ) )
                    .value( row -> row.getAs( "Airport ID" ) ).ok()
                .addProperty( new FullQualifiedName( "aviation.iata" ) )
                    .value( row -> row.getAs( " IATA" ) ).ok()
                .addProperty( new FullQualifiedName( "location.longitude" ) )
                    .value( row -> row.getAs( " Longitude" ) ).ok()
                .addProperty( new FullQualifiedName( "location.latitude" ) )
                    .value( row -> row.getAs( " Latitude" ) ).ok()
                .addProperty( new FullQualifiedName( "general.altitude" ) )
                    .value( row -> row.getAs( " Altitude" ) ).ok()
                .addProperty( new FullQualifiedName( "general.city" ) )
                    .value( row -> row.getAs( " City" ) ).ok()
                .addProperty( new FullQualifiedName( "general.country" ) )
                    .value( row -> row.getAs( " Country" ) ).ok()
                .addProperty( new FullQualifiedName( "aviation.icao" ) )
                    .value( row -> row.getAs( " ICAO" ) ).ok()
            .ok()
        .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
}
