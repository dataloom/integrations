package com.dataloom.integrations.slc;

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
        final String path = args[ 0 ];
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

        Flight flight = Flight.newFlight()
                .addEntity().to( ENTITY_SET_NAME ).as( new FullQualifiedName( "publicsafety.jaildata" ) )
                .key( new FullQualifiedName( "general.guid" ) )
                .addProperty()
                .value( row -> UUID.randomUUID() )
                .as( new FullQualifiedName( "general.guid" ) )
                .ok()
                .addProperty()
                .value( row -> getFirstName( row.getAs( "Name" ) ) )
                .as( new FullQualifiedName( "general.firstname" ) )
                .ok()
                .addProperty().value( row -> getLastName( row.getAs( "Name" ) ) )
                .as( new FullQualifiedName( "general.lastname" ) )
                .ok()
                .addProperty()
                .value( row -> row.getAs( "Date of Birth" ) )
                .as( new FullQualifiedName( "general.dob" ) )
                .ok()
                .addProperty()
                .value( row -> row.getAs( "Date Booked" ) )
                .as( new FullQualifiedName( "publicsafety.datebooked" ) )
                .ok()
                .addProperty().value( row -> row.getAs( "Date Released" ) )
                .as( new FullQualifiedName( "publicsafety.datereleased" ) )
                .ok()
                .ok()
                .done();

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
    }
}