package com.dataloom.integrations.iowacity;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.request.AuthenticationRequest;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.commons.lang3.StringUtils;
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

import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IowaCityCallsForService {
    private static final SparkSession sparkSession;
    private static final Logger                  logger            = LoggerFactory
            .getLogger( IowaCityCallsForService.class );
    private static final Auth0                   auth0             = new Auth0(
            "PTmyExdBckHAiyOjh4w2MqSIUGWWEdf8",
            "loom.auth0.com" );
    private static final AuthenticationAPIClient client            = auth0.newAuthenticationAPIClient();
    public static        String                  ES_NAME           = "iowacitycfs";
    public static        FullQualifiedName       ES_TYPE_CFS       = new FullQualifiedName( "publicsafety",
            "callforservice" );
    public static        FullQualifiedName       CFS_NUMBER_FQN    = new FullQualifiedName( "iowa", "cfsnumber" );
    public static        FullQualifiedName       FIRST_NAME_FQN    = new FullQualifiedName( "general", "firstname" );
    public static        FullQualifiedName       LAST_NAME_FQN     = new FullQualifiedName( "general", "lastname" );
    public static        FullQualifiedName       DOB_FQN           = new FullQualifiedName( "general", "dob" );
    public static        FullQualifiedName       INVOLVEMENT_FQN   = new FullQualifiedName( "iowa", "persontype" );
    public static        FullQualifiedName       ADDRESS_FQN       = new FullQualifiedName( "general", "address" );
    public static        FullQualifiedName       TYPE_OF_CALL_FQN  = new FullQualifiedName( "iowa", "cfstype" );
    public static        FullQualifiedName       RESOLUTION_FQN    = new FullQualifiedName( "iowa", "cfsresolution" );
    public static        DateTimeFormatter       cfsDataFormatter  = DateTimeFormat.forPattern( "MM/dd/yy" );
    private static String jwtToken;
    private static Environment environment = Environment.PRODUCTION;
    // For splitting up into first and last name.
    private static Pattern     p           = Pattern.compile( "(.+),[ \t]+(.+)" );

    static {
        sparkSession = SparkSession.builder()
                .master( "local[4]" )
                .appName( "test" )
                .getOrCreate();
        AuthenticationRequest request = client.login( "support@kryptnostic.com", "abracadabra" )
                .setConnection( "Tests" )
                .setScope( "openid email nickname roles user_id" );
        jwtToken = request.execute().getIdToken();

        //        jwtToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImhvY2h1bmdAa3J5cHRub3N0aWMuY29tIiwiZW1haWxfdmVyaWZpZWQiOnRydWUsImFwcF9tZXRhZGF0YSI6eyJyb2xlcyI6WyJ1c2VyIiwiYWRtaW4iLCJBdXRoZW50aWNhdGVkVXNlciJdfSwibmlja25hbWUiOiJob2NodW5nIiwicm9sZXMiOlsidXNlciIsImFkbWluIiwiQXV0aGVudGljYXRlZFVzZXIiXSwidXNlcl9pZCI6Imdvb2dsZS1vYXV0aDJ8MTEzOTE0MjkyNjQyNzY2ODgyOTk0IiwiaXNzIjoiaHR0cHM6Ly9sb29tLmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDExMzkxNDI5MjY0Mjc2Njg4Mjk5NCIsImF1ZCI6Ikt2d3NFVGFVeHVYVmpXMmNtejJMYmRxWFFCZFlzNndIIiwiZXhwIjoxNDg3MzcyODI5LCJpYXQiOjE0ODczMzY4Mjl9.BgMdydUdVivvnKVoIvBJ66MgGNg6vAcsiLDS9YSeQYg";
        logger.info( "Using the following idToken: Bearer {}", jwtToken );
    }

    public static void main( String[] args ) throws InterruptedException {
        String path = args[ 0 ];
        jwtToken = args[ 1 ];

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );
        UUID cfsNumber = edm.createPropertyType( new PropertyType( CFS_NUMBER_FQN, "CFS Number", Optional.of(
                "Call For Service Case Number" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        if ( cfsNumber == null ) {
            cfsNumber = edm.getPropertyTypeId( CFS_NUMBER_FQN.getNamespace(), CFS_NUMBER_FQN.getName() );
        }
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
        UUID involvement = edm.createPropertyType( new PropertyType( INVOLVEMENT_FQN, "Involvement", Optional.of(
                "" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        if ( involvement == null ) {
            involvement = edm.getPropertyTypeId( INVOLVEMENT_FQN.getNamespace(), INVOLVEMENT_FQN.getName() );
        }
        UUID typeOfCall = edm.createPropertyType( new PropertyType( TYPE_OF_CALL_FQN, "Type of Call", Optional.of(
                "Type of the call for service" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        if ( typeOfCall == null ) {
            typeOfCall = edm.getPropertyTypeId( TYPE_OF_CALL_FQN.getNamespace(), TYPE_OF_CALL_FQN.getName() );
        }
        UUID resolution = edm.createPropertyType( new PropertyType( RESOLUTION_FQN, "Resolution", Optional.of(
                "Resolution of the call for service" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        if ( resolution == null ) {
            resolution = edm.getPropertyTypeId( RESOLUTION_FQN.getNamespace(), RESOLUTION_FQN.getName() );
        }
        UUID address = edm.createPropertyType( new PropertyType( Optional.absent(),
                ADDRESS_FQN,
                "Address",
                Optional.of(
                        "Address of the incident" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String,
                Optional.of( true ),
                Optional.absent() ) );
        if ( address == null ) {
            address = edm.getPropertyTypeId( ADDRESS_FQN.getNamespace(), ADDRESS_FQN.getName() );
        }
        Set<UUID> properties = ImmutableSet.of(
                cfsNumber,
                firstName,
                lastName,
                dob,
                involvement,
                resolution,
                typeOfCall,
                address );
        UUID cfsId = edm.createEntityType( new EntityType(
                ES_TYPE_CFS,
                "Call For Service",
                "Call For Service",
                ImmutableSet.of(),
                properties,
                properties ) );
        if ( cfsId == null ) {
            cfsId = edm.getEntityTypeId( ES_TYPE_CFS.getNamespace(), ES_TYPE_CFS.getName() );
        }

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                cfsId,
                ES_NAME,
                "Iowa City Police Department Calls For Service",
                Optional.of(
                        "" ) ) ) );

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
                .addEntity().to( ES_NAME ).as( ES_TYPE_CFS )
                .key( CFS_NUMBER_FQN,
                        FIRST_NAME_FQN,
                        LAST_NAME_FQN,
                        DOB_FQN,
                        INVOLVEMENT_FQN,
                        ADDRESS_FQN,
                        TYPE_OF_CALL_FQN,
                        RESOLUTION_FQN )
                .addProperty().value( row -> row.getAs( "CALL FOR SERVICE" ) ).as( CFS_NUMBER_FQN ).ok()
                .addProperty().value( IowaCityCallsForService::getFirstName ).as( FIRST_NAME_FQN ).ok()
                .addProperty().value( IowaCityCallsForService::getLastName ).as( LAST_NAME_FQN ).ok()
                .addProperty().value( row -> row.getAs( "DOB" ) == null ?
                        null :
                        LocalDate.parse( row.getAs( "DOB" ), cfsDataFormatter ).toString() )
                .as( DOB_FQN ).ok()
                .addProperty().value( row -> row.getAs( "PERSON TYPE" ) ).as( INVOLVEMENT_FQN ).ok()
                .addProperty().value( row -> row.getAs( "ADDRESS" ) ).as( ADDRESS_FQN ).ok()
                .addProperty().value( row -> row.getAs( "TYPE OF CALL" ) ).as( TYPE_OF_CALL_FQN ).ok()
                .addProperty().value( row -> row.getAs( "CLEARED BY" ) ).as( RESOLUTION_FQN ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flight, payload );
    }

    public static String getFirstName( Row row ) {
        String name = row.getAs( "NAME" );
        if ( StringUtils.isBlank( name ) ) {
            return null;
        }
        Matcher m = p.matcher( name );
        if ( !m.matches() ) {
            return null;
        }
        return (String) m.group( 2 );
    }

    public static String getLastName( Row row ) {
        String name = row.getAs( "NAME" );
        if ( StringUtils.isBlank( name ) ) {
            return null;
        }
        Matcher m = p.matcher( name );
        if ( !m.matches() ) {
            return null;
        }
        return (String) m.group( 1 );
    }

}