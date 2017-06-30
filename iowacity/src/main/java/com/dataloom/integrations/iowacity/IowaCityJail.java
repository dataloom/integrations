package com.dataloom.integrations.iowacity;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Pattern;

public class IowaCityJail {
    private static final SparkSession sparkSession;
    private static final Logger                  logger            = LoggerFactory.getLogger( IowaCityJail.class );
    private static final Auth0                   auth0             = new Auth0(
            "PTmyExdBckHAiyOjh4w2MqSIUGWWEdf8",
            "loom.auth0.com" );
    private static final AuthenticationAPIClient client            = auth0.newAuthenticationAPIClient();
    public static        String                  ES_NAME           = "iowacityjailbookings";
    public static        FullQualifiedName       ES_TYPE_JAILDATA  = new FullQualifiedName(
            "publicsafety",
            "bookings" );
    public static        FullQualifiedName       CFS_NUMBER_FQN    = new FullQualifiedName( "iowa", "cfsnumber" );
    public static        FullQualifiedName       FIRST_NAME_FQN    = new FullQualifiedName( "general", "firstname" );
    public static        FullQualifiedName       LAST_NAME_FQN     = new FullQualifiedName( "general", "lastname" );
    public static        FullQualifiedName       DOB_FQN           = new FullQualifiedName( "general",
            "dob" );       // date
    public static        FullQualifiedName       DATE_BOOKED_FQN   = new FullQualifiedName(
            "publicsafety",
            "datebooked" );                                                                                           // Date
    public static        FullQualifiedName       DATE_RELEASED_FQN = new FullQualifiedName(
            "publicsafety",
            "datereleased" );                                                                                         // Date
    public static        FullQualifiedName       GUID_FQN          = new FullQualifiedName( "general", "guid" );
    public static        DateTimeFormatter       jailDataFormatter = DateTimeFormat.forPattern( "dd-MMM-yy" );
    private static       Pattern                 p                 = Pattern.compile( ".*\\n*.*\\n*\\((.+),(.+)\\)" );
    private static Environment environment = Environment.PRODUCTION;

    static {
        sparkSession = MissionControl.getSparkSession();
        //        AuthenticationRequest request = client.login( "support@kryptnostic.com", "abracadabra" )
        //                .setConnection( "Tests" )
        //                .setScope( "openid email nickname roles user_id" );
        //        jwtToken = request.execute().getIdToken();
    }

    public static void main( String[] args ) throws InterruptedException {
        // jwtToken = args[ 0 ];
        final String path = args[ 1] ;
        final String jwtToken = args[ 2 ];
        logger.info( "Using the following idToken: Bearer {}", jwtToken );
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        UUID firstName = edm
                .createPropertyType( new PropertyType( Optional.absent(),
                        FIRST_NAME_FQN,
                        "First Name",
                        Optional.of(
                                "First Name" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( firstName == null ) {
            firstName = edm.getPropertyTypeId( FIRST_NAME_FQN.getNamespace(), FIRST_NAME_FQN.getName() );
        }
        UUID lastName = edm
                .createPropertyType( new PropertyType( Optional.absent(),
                        LAST_NAME_FQN,
                        "Last Name",
                        Optional.of(
                                "Last Name" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( lastName == null ) {
            lastName = edm.getPropertyTypeId( LAST_NAME_FQN.getNamespace(), LAST_NAME_FQN.getName() );
        }
        UUID dob = edm.createPropertyType( new PropertyType( DOB_FQN, "Date Of Birth", Optional.of(
                "Date of Birth" ), ImmutableSet.of(), EdmPrimitiveTypeKind.Date ) );
        if ( dob == null ) {
            dob = edm.getPropertyTypeId( DOB_FQN.getNamespace(), DOB_FQN.getName() );
        }
        UUID dateBooked = edm.createPropertyType( new PropertyType( DATE_BOOKED_FQN, "Date Booked", Optional.of(
                "Date Booked into Jail" ), ImmutableSet.of(), EdmPrimitiveTypeKind.Date ) );
        if ( dateBooked == null ) {
            dateBooked = edm.getPropertyTypeId( DATE_BOOKED_FQN.getNamespace(), DATE_BOOKED_FQN.getName() );
        }
        UUID dateReleased = edm.createPropertyType( new PropertyType( DATE_RELEASED_FQN, "Date Released", Optional.of(
                "Date Released from Jail" ), ImmutableSet.of(), EdmPrimitiveTypeKind.Date ) );
        if ( dateReleased == null ) {
            dateReleased = edm.getPropertyTypeId( DATE_RELEASED_FQN.getNamespace(), DATE_RELEASED_FQN.getName() );
        }

        UUID etId = edm.createEntityType( new EntityType(
                ES_TYPE_JAILDATA,
                "Jail Booking",
                "Jail Booking",
                ImmutableSet.of(),
                Sets.newLinkedHashSet(
                        ImmutableSet.of(
                                firstName,
                                lastName,
                                dob,
                                dateBooked,
                                dateReleased ) ),
                Sets.newLinkedHashSet( ImmutableSet.of(
                        firstName,
                        lastName,
                        dob,
                        dateBooked,
                        dateReleased ) ),
                Optional.absent(),
                Optional.absent()
        ) );
        if ( etId == null ) {
            etId = edm.getEntityTypeId(
                    ES_TYPE_JAILDATA.getNamespace(), ES_TYPE_JAILDATA.getName() );
        }

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                etId,
                ES_NAME,
                "Iowa City Jail Bookings",
                Optional.of(
                        "Jail Bookings from Iowa City" ),
                Sets.newHashSet( "matthew@thedataloom.com", "david-schwindt@iowacity.org" ) ) ) );

        /*
         * Get the dataset.
         */
        // UUID entitySetId = edm.getEntitySetId( "slcstolencars2012" );
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "person" ).to( ES_NAME ).ofType( ES_TYPE_JAILDATA )
                .key( new FullQualifiedName( "general.firstname" ),
                        new FullQualifiedName( "general.lastname" ),
                        new FullQualifiedName( "general.dob" ),
                        new FullQualifiedName( "publicsafety.datebooked" ),
                        new FullQualifiedName( "publicsafety.datereleased" )
                )
                .addProperty( new FullQualifiedName( "general.firstname" ) )
                .value( row -> getFirstName( row.getAs( "Name" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "general.lastname" ) )
                .value( row -> getLastName( row.getAs( "Name" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "general.dob" ) )
                .value( row -> row.getAs( "Date of Birth" ) == null ?
                        null :
                        LocalDate.parse( row.getAs( "Date of Birth" ), jailDataFormatter ).toString() )
                .ok()
                .addProperty( new FullQualifiedName( "publicsafety.datebooked" ) )
                .value( row -> row.getAs( "Date Booked" ) )
                .ok()
                .addProperty( new FullQualifiedName( "publicsafety.datereleased" ) )
                .value( row -> row.getAs( "Date Released" ) )
                .ok()
                .ok()
                .ok()
                .done();
        Map<Flight, Dataset<Row>> flights = new HashMap<>(  );
        flights.put( flight, payload );
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
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