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
    public static String            ENTITY_SET_NAME = "testmysql";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "sample.mysql" );
    public static FullQualifiedName ENTITY_SET_KEY1 = new FullQualifiedName( "general.firstname" );
    public static FullQualifiedName ENTITY_SET_KEY2 = new FullQualifiedName( "general.lastname" );
    public static FullQualifiedName ENTITY_SET_KEY3 = new FullQualifiedName( "general.dob" );

    public static void main( String[] args ) throws InterruptedException {

        final String path = args[ 0 ];
        final String username = args[ 1 ];
        final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        final String jwtToken = MissionControl.getIdToken( username, password );
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Dataset<Row> payload = sparkSession.read()
                .format( "jdbc" )
                .option( "url", "jdbc:mysql://[host]:[port]/[database]" )
                .option( "driver", "com.mysql.jdbc.Driver" )
                .option( "dbtable", "people" )
                .option( "user", "root" )
                .option( "password", "mysqlpassword" )
                .load();

        Flight flight = Flight.newFlight()
                .addEntity( ENTITY_SET_TYPE )
                .to( ENTITY_SET_NAME )
                .key( ENTITY_SET_KEY1, ENTITY_SET_KEY2, ENTITY_SET_KEY3 )
                .addProperty( new FullQualifiedName( "general.firstname" ) )
                .value( row -> row.getAs( "first_name" ) ).ok()
                .addProperty( new FullQualifiedName( "general.uuid" ) )
                .value( row -> row.getAs( "uuid" ) ).ok()
                .addProperty( new FullQualifiedName( "general.dob" ) )
                .value( row -> standardizeDate( row.getAs( "dob" ) ) ).ok()
                .addProperty( new FullQualifiedName( "general.ssn" ) )
                .value( row -> row.getAs( "ssn" ) ).ok()
                .addProperty( new FullQualifiedName( "general.ethnicity" ) )
                .value( row -> row.getAs( "ethnicity" ) ).ok()
                .addProperty( new FullQualifiedName( "general.gender" ) )
                .value( row -> row.getAs( "sex" ) ).ok()
                .addProperty( new FullQualifiedName( "general.race" ) )
                .value( row -> row.getAs( "race" ) ).ok()
                .addProperty( new FullQualifiedName( "general.lastname" ) )
                .value( row -> row.getAs( "last_name" ) ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( RetrofitFactory.Environment.LOCAL, jwtToken );
        shuttle.launch( flight, payload );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
    public static String standardizeDate( Object myDate ) {
        if ( myDate != null && !myDate.equals( "" ) ) {
            FormattedDateTime date = new FormattedDateTime( myDate.toString(), null, "yyyy-MM-dd", null );
            return date.getDateTime();
        }
        return null;
    }
}
