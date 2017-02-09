package com.dataloom.integrations.slc;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

import com.clearspring.analytics.util.Preconditions;
import com.dataloom.client.RetrofitFactory.Environment;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.util.UUID;

public class IowaCity {
    private static final Logger            logger            = LoggerFactory.getLogger( IowaCity.class );
    public static        String            ENTITY_SET_NAME           = "iowajail";

    public static void main( String[] args ) throws InterruptedException {

        final String path;
        final String username;
        final String password;

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        if( args.length==3 ){
            File f = new File( path );
            if( f.exists() && !f.isDirectory() ) {
                path = args[ 0 ];
            }
            username = args[1];
            password = args[2];
        }

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .addEntity().to( ENTITY_SET_NAME).as( new FullQualifiedName( "publicsafety.jaildata") )
                .key(new FullQualifiedName( "general.guid" )   )
                .addProperty().value( row -> UUID.randomUUID() ).as( new FullQualifiedName( "general.guid" ) ).ok()
                .addProperty().value( row -> getFirstName(  row.getAs( "Name" ) ) ).as( new FullQualifiedName( "general.firstname" )  ).ok()
                .addProperty().value( row -> getLastName ( row.getAs( "Name" ) ) ).as( new FullQualifiedName( "general.lastname" )  ).ok()
                .addProperty().value( row -> row.getAs( "Date of Birth" ) ).as( new FullQualifiedName( "general.dob" )  ).ok()
                .addProperty().value( row -> row.getAs( "Date Booked" ) ).as( new FullQualifiedName( "publicsafety.datebooked" )  ).ok()
                .addProperty().value( row -> row.getAs( "Date Released" ) ).as( new FullQualifiedName( "publicsafety.datereleased" )  ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    public static String getFirstName( Object obj  ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[1];
    }

    public static String getLastName( Object obj  ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[0];
    }
}