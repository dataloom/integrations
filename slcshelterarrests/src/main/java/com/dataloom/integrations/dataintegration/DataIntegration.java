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

public class DataIntegration {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( DataIntegration.class );

    // Set your Entity Set Name, Type, and Primary Key
    public static String            ENTITY_SET_NAME = "slcshelterarrest";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "slc.slcshelterarrest" );
    public static FullQualifiedName ENTITY_SET_KEY  = new FullQualifiedName( "general.personid" );

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
                .addProperty( new FullQualifiedName( "publicsafety.case" ) )
                .value( row -> row.getAs( "case" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.occdatetime" ) )
                .value( row -> standardizeDateTime( row.getAs( "occ year" ), row.getAs( "time occ" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.adult3-juv103" ) )
                .value( row -> row.getAs( "3-adult,103-juv" ) ).ok()
                .addProperty( new FullQualifiedName( "general.lastname" ) )
                .value( row -> row.getAs( "last name" ) ).ok()
                .addProperty( new FullQualifiedName( "general.firstname" ) )
                .value( row -> row.getAs( "first name" ) ).ok()
                .addProperty( new FullQualifiedName( "general.middlename" ) )
                .value( row -> row.getAs( "middle" ) ).ok()
                .addProperty( new FullQualifiedName( "general.personid" ) )
                .value( row -> row.getAs( "person id no" ) ).ok()
                .addProperty( new FullQualifiedName( "general.homeaddress" ) )
                .value( row -> row.getAs( "address" ) ).ok()
                .addProperty( new FullQualifiedName( "general.city" ) )
                .value( row -> row.getAs( "city" ) ).ok()
                .addProperty( new FullQualifiedName( "general.gender" ) )
                .value( row -> row.getAs( "sex" ) ).ok()
                .addProperty( new FullQualifiedName( "general.race" ) )
                .value( row -> row.getAs( "race" ) ).ok()
                .addProperty( new FullQualifiedName( "general.dob" ) )
                .value( row -> standardizeDateTime( row.getAs( "dob" ), null ) ).ok()
                .addProperty( new FullQualifiedName( "general.weekday" ) )
                .value( row -> row.getAs( "day of week" ) ).ok()
                .addProperty( new FullQualifiedName( "general.address" ) )
                .value( row -> row.getAs( "location" ) ).ok()
                .addProperty( new FullQualifiedName( "general.apartment" ) )
                .value( row -> row.getAs( "apartment" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.pdzonestr" ) )
                .value( row -> row.getAs( "pd zone" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.pdbeat" ) )
                .value( row -> row.getAs( "pd beat" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.ucr" ) )
                .value( row -> row.getAs( "ucr" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.ucrext" ) )
                .value( row -> row.getAs( "ucr ext" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.ncicdes" ) )
                .value( row -> row.getAs( "ncic des" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.ncicexpdesc" ) )
                .value( row -> row.getAs( "ncic exp desc" ) ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW

    public static String paddedTime( String time ) {
        int original = Integer.parseInt( time );
        String padded = String.format( "%04d", original );
        return padded;
    }

    public static String standardizeDateTime( Object myDate, Object myTime ) {
        if ( myDate != null && myTime == null ) {
            FormattedDateTime date = new FormattedDateTime( myDate.toString(), null, "MM/dd/yyyy", null );
            return date.getDateTime();
        }
        if ( myDate == null && myTime != null ) {
            FormattedDateTime time = new FormattedDateTime( null, paddedTime( myTime.toString() ), null, "HHmm" );
            return time.getDateTime();
        }
        if ( myDate != null && myTime != null ) {
            FormattedDateTime dateTime = new FormattedDateTime( myDate.toString(),
                    paddedTime( myTime.toString() ),
                    "MM/dd/yyyy",
                    "HHmm" );
            return dateTime.getDateTime();
        }
        return null;
    }
}
