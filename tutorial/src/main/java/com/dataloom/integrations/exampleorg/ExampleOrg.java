// This should match file location of ExampleOrg.java file
// src/.../com/dataloom/integrations/exampleorg/ExampleOrg.java
package com.dataloom.integrations.exampleorg;

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

// Change ExampleOrg.class if you changed the organization name
// Logger is used to output useful debugging messages to the console
public class ExampleOrg {
    private static final Logger logger = LoggerFactory.getLogger( ExampleOrg.class );

    // SET YOUR ENTITY NAME, TYPE, and PRIMARY KEY
    public static String ENTITY_SET_NAME = "crimeindex";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "publicsafety.crimeindex" );
    public static FullQualifiedName ENTITY_SET_KEY = new FullQualifiedName( "general.city" ); // or use "general.guid"

    // SET YOUR PROPERTY TYPES
    public static FullQualifiedName PT_YEAR = new FullQualifiedName( "general.year" );
    public static FullQualifiedName PT_INDEX = new FullQualifiedName( "publicsafety.crimeindexranking" );
    public static FullQualifiedName PT_CITY = new FullQualifiedName( "general.city" );
    public static FullQualifiedName PT_RATE = new FullQualifiedName( "publicsafety.crimerate" );

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
                .addProperty( PT_YEAR )
                    .value(row -> row.getAs( "Year" )).ok()
                .addProperty( PT_INDEX )
                    .value( row -> row.getAs( "Crime Index Ranking" ) ).ok()
                .addProperty( PT_CITY )
                    .value( row -> row.getAs( "City" ) ).ok()
                .addProperty( PT_RATE )
                    .value( row -> row.getAs( "Rate" ) ).ok()
            .ok()
        .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
}
