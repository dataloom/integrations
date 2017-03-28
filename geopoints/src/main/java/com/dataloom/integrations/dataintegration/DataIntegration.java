package com.dataloom.integrations.dataintegration;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

import com.dataloom.client.RetrofitFactory;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataIntegration {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( DataIntegration.class );

    // Set your Entity Set Name, Type, and Primary Key
    public static String            ENTITY_SET_NAME = "sfsampleaddresses";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "sample.addresses" );
    public static FullQualifiedName ENTITY_SET_KEY  = new FullQualifiedName( "iowastate.escene15" );

    public static void main( String[] args ) throws InterruptedException {

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
                .addProperty( new FullQualifiedName( "iowastate.escene15" ) )
                .value( row -> get_geo( row.getAs( "NUMBER" ),
                        row.getAs( "STREET" ),
                        row.getAs( "UNIT" ),
                        row.getAs( "CITY" ),
                        row.getAs( "POSTCODE" ) ).getFormattedAddress() ).ok()
                .addProperty( new FullQualifiedName( "iowastate.escene11" ) )
                .value( row -> get_geo( row.getAs( "NUMBER" ),
                        row.getAs( "STREET" ),
                        row.getAs( "UNIT" ),
                        row.getAs( "CITY" ),
                        row.getAs( "POSTCODE" ) ) ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    public static GeocodedAddress get_geo( Object num, Object street, Object unit, Object city, Object postcode ) {
        String n = num != null ? num.toString() : "";
        String s = street != null ? street.toString() : "";
        String u = unit != null ? unit.toString() : "";
        String c = city != null ? city.toString() : "";
        String p = postcode != null ? postcode.toString() : "";
        String address = n + " " + s + " " + u + ", " + c + " " + p;
        GeocodedAddress add = new GeocodedAddress( address );
        return ( add );
    }
}
