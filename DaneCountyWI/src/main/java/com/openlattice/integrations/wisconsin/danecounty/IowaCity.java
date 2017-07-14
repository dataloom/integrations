package com.dataloom.integrations.iowacity;

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

public class IowaCity {
    private static final Logger logger          = LoggerFactory.getLogger( IowaCity.class );
    public static        String ENTITY_SET_NAME = "iowajail";

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
       /* final String path = args[ 0 ];
        final String username = args[ 1 ];
        final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        final String jwtToken = MissionControl.getIdToken( username, password );
        ;

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );
        payload = payload.sample( false, .10 );
        // @formatter:off
        Flight flight = Flight.newFlight()
                .addEntity( new FullQualifiedName( "publicsafety.jaildata" ) )
                    .to( ENTITY_SET_NAME )
                    .key( new FullQualifiedName( "general.guid" ) )
                    .addProperty( new FullQualifiedName( "general.guid" ) )
                        .value( row -> UUID.randomUUID() )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.firstname" ) )
                        .value( row -> getFirstName( row.getAs( "Name" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.lastname" ) )
                        .value( row -> getLastName( row.getAs( "Name" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.dob" ) )
                        .value( row -> row.getAs( "Date of Birth" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "publicsafety.datebooked" ) )
                        .value( row -> row.getAs( "Date Booked" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "publicsafety.datereleased" ) )
                        .value( row -> row.getAs( "Date Released" ) )
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    public static String getFirstName( Object obj ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[ 1 ].trim();
    }

    public static String getLastName( Object obj ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[ 0 ].trim();
    }*/
       }
}