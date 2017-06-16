package com.dataloom.integrations.iowacity;

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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JohnsonCountyIowa {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( JohnsonCountyIowa.class );

    // Set your Entity Set Name, Type, and Primary Key
    public static String            ENTITY_SET_NAME = "johnsoncountyiowajail";
    public static FullQualifiedName ENTITY_SET_TYPE = new FullQualifiedName( "publicsafety.johnsoncountyiowajail" );
    public static FullQualifiedName ENTITY_SET_KEY1 = new FullQualifiedName( "general.firstname" );
    public static FullQualifiedName ENTITY_SET_KEY2 = new FullQualifiedName( "general.lastname" );
    public static FullQualifiedName ENTITY_SET_KEY3 = new FullQualifiedName( "general.dob" );

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String path = args[ 0 ];
        //final String username = args[ 1 ];
        //final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        final String jwtToken = args[ 1 ];//MissionControl.getIdToken( username, password );
        final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "booking" )
                .ofType( ENTITY_SET_TYPE )
                .to( ENTITY_SET_NAME )
                .key( ENTITY_SET_KEY1, ENTITY_SET_KEY2, ENTITY_SET_KEY3 )
                .addProperty( new FullQualifiedName( "publicsafety.datereleased" ) )
                .value( row -> standardizeDate( row.getAs( "Date Released" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.datebooked" ) )
                .value( row -> standardizeDate( row.getAs( "Date Booked" ) ) ).ok()
                .addProperty( new FullQualifiedName( "general.firstname" ) )
                .value( row -> getFirstName( row.getAs( "Name" ) ) ).ok()
                .addProperty( new FullQualifiedName( "general.middlename" ) )
                .value( row -> getMiddleName( row.getAs( "Name" ) ) ).ok()
                .addProperty( new FullQualifiedName( "general.lastname" ) )
                .value( row -> getLastName( row.getAs( "Name" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.offenserelease" ) )
                .value( row -> standardizeDate( row.getAs( "Offense Release Date" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.offensestart" ) )
                .value( row -> standardizeDate( row.getAs( "Offense Start Date" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.sentence" ) )
                .value( row -> row.getAs( "Sentence" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.sentencedays" ) )
                .value( row -> row.getAs( "Sentence Days" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.statutenumber" ) )
                .value( row -> row.getAs( "Statute No" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.chargedescription" ) )
                .value( row -> row.getAs( "Charge Desc" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.severity" ) )
                .value( row -> row.getAs( "Severity" ) ).ok()
                .addProperty( new FullQualifiedName( "general.race" ) )
                .value( row -> row.getAs( "Race" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.jailid64" ) )
                .value( row -> row.getAs( "Jail ID" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.chargingagency" ) )
                .value( row -> row.getAs( "Charging Agency" ) ).ok()
                .addProperty( new FullQualifiedName( "general.dob" ) )
                .value( row -> standardizeDate( row.getAs( "Date of Birth" ) ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.bondtype" ) )
                .value( row -> row.getAs( "Bond Type" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.reasonheldcodestring" ) )
                .value( row -> row.getAs( "Reason Held Code" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.offensedate" ) )
                .value( row -> standardizeDate( row.getAs( "Date of Offense" ) ) ).ok()
                .addProperty( new FullQualifiedName( "general.ethnicity" ) )
                .value( row -> row.getAs( "Ethnicity" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.arrestingagency" ) )
                .value( row -> row.getAs( "Arresting Agency" ) ).ok()
                .addProperty( new FullQualifiedName( "general.gender" ) )
                .value( row -> row.getAs( "Sex" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.courtcasenumber" ) )
                .value( row -> row.getAs( "Court Case No" ) ).ok()
                .addProperty( new FullQualifiedName( "general.homeaddress" ) )
                .value( row -> row.getAs( "Offender Address" ) ).ok()
                .addProperty( new FullQualifiedName( "general.city" ) )
                .value( row -> row.getAs( "Offender Address City" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.howreleased" ) )
                .value( row -> row.getAs( "How Released" ) ).ok()
                .ok()
                .ok()
                .done();
        Map<Flight, Dataset<Row>> flights = new HashMap<>(  );
        flights.put( flight, payload );
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
    public static String standardizeDate( Object myDate ) {
        if ( myDate != null ) {
            FormattedDateTime date = new FormattedDateTime( myDate.toString(), null, "dd-MMM-yy", null );
            return date.getDateTime();
        }
        return null;
    }

    public static String getFirstName( Object name ) {
        String n = name.toString().replace( ", ", "," );
        String[] names = n.split( "," );
        String[] remainingName = names[ 1 ].split( " " );
        return remainingName[ 0 ].trim() == null ? null : remainingName[ 0 ].trim();
    }

    public static String getMiddleName( Object name ) {
        String n = name.toString().replace( ", ", "," );
        String[] names = n.split( "," );
        String[] remainingName = names[ 1 ].split( " " );
        String[] copy = Arrays.copyOfRange( remainingName, 1, remainingName.length );
        String middle = String.join( " ", copy ).trim();
        return remainingName.length < 2 ? null : middle;
    }

    public static String getLastName( Object name ) {
        String n = name.toString().replace( ", ", "," );
        String[] names = n.split( "," );
        return names[ 0 ].trim() == null ? null : names[ 0 ].trim();
    }
}
