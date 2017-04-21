package com.dataloom.integrations.slc;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.request.AuthenticationRequest;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.io.File;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SlcStolenCars {
    private static final SparkSession sparkSession;
    private static final Logger            logger   = LoggerFactory.getLogger( SlcStolenCars.class );
    private static final Auth0                   auth0  = new Auth0(
            "PTmyExdBckHAiyOjh4w2MqSIUGWWEdf8",
            "loom.auth0.com" );
    private static final AuthenticationAPIClient client = auth0.newAuthenticationAPIClient();
    public static        String            ES_NAME  = "slcstolencars2012";
    public static        FullQualifiedName ES_TYPE  = new FullQualifiedName( "publicsafety", "stolencars" );
    private static       FullQualifiedName CASE_FQN = new FullQualifiedName( "publicsafety", "case" );
    private static       FullQualifiedName OC_FQN   = new FullQualifiedName( "publicsafety", "offensecode" );
    private static       FullQualifiedName OD_FQN   = new FullQualifiedName( "publicsafety", "offensedescription" );
    private static       FullQualifiedName RD_FQN   = new FullQualifiedName( "publicsafety", "reportdate" );
    private static       FullQualifiedName OCC_FQN  = new FullQualifiedName( "publicsafety", "occdate" );
    private static       FullQualifiedName DOW_FQN  = new FullQualifiedName( "general", "dayofweek" );
    private static       FullQualifiedName ADDR_FQN = new FullQualifiedName( "general", "address" );
    private static       FullQualifiedName LAT_FQN  = new FullQualifiedName( "location", "latitude" );
    private static       FullQualifiedName LON_FQN  = new FullQualifiedName( "location", "longitude" );
    private static Pattern p = Pattern.compile( ".*\\n*.*\\n*\\((.+),(.+)\\)" );
    private static String jwtToken;

    static {
        sparkSession = SparkSession.builder()
                .master( "local[8]" )
                .appName( "test" )
                .getOrCreate();

        AuthenticationRequest request = client.login( "support@kryptnostic.com", "abracadabra" )
                .setConnection( "Tests" )
                .setScope( "openid email nickname roles user_id" );
        jwtToken = request.execute().getIdToken();
    }

    public static void main( String[] args ) throws InterruptedException {
        jwtToken = args[ 0 ];
        logger.info( "Using the following idToken: Bearer {}", jwtToken );
        String path = new File( args[ 1 ] ).getAbsolutePath();
        Retrofit retrofit = RetrofitFactory.newClient( Environment.PRODUCTION, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        UUID caseId = edm.createPropertyType( new PropertyType( CASE_FQN, "Case #", Optional.of(
                "The case it was filed under" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        UUID ocId = edm
                .createPropertyType( new PropertyType(
                        OC_FQN,
                        "Offense Code",
                        Optional.of( "The code of the offense" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String ) );
        UUID odId = edm.createPropertyType( new PropertyType(
                OD_FQN,
                "Offense Description",
                Optional.of( "The description of the offense." ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String ) );
        UUID rdId = edm.createPropertyType( new PropertyType(
                RD_FQN,
                "Report Date",
                Optional.of( "The day the car was reported stolen" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String ) );
        UUID occId = edm.createPropertyType( new PropertyType( OCC_FQN, "Offense Code Commited Date", Optional.of(
                "I'm not really sure what this means." ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        UUID dowId = edm.createPropertyType( new PropertyType(
                DOW_FQN,
                "Day of Week",
                Optional.of( "Day of week car was stolen" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Int16 ) );
        UUID addrId = edm.createPropertyType( new PropertyType(
                ADDR_FQN,
                "Day of Week",
                Optional.of( "Address the car was stolen from." ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String ) );
        UUID latId = edm.createPropertyType( new PropertyType(
                LAT_FQN,
                "Latitude",
                Optional.of( "" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.Double ) );
        UUID lonId = edm.createPropertyType(
                new PropertyType(
                        LON_FQN,
                        "Longitude",
                        Optional.of( "Longitude" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double ) );

        caseId = edm.getPropertyTypeId( CASE_FQN.getNamespace(), CASE_FQN.getName() );
        ocId = edm.getPropertyTypeId( OC_FQN.getNamespace(), OC_FQN.getName() );
        odId = edm.getPropertyTypeId( OD_FQN.getNamespace(), OD_FQN.getName() );
        rdId = edm.getPropertyTypeId( RD_FQN.getNamespace(), RD_FQN.getName() );
        occId = edm.getPropertyTypeId( OCC_FQN.getNamespace(), OCC_FQN.getName() );
        dowId = edm.getPropertyTypeId( DOW_FQN.getNamespace(), DOW_FQN.getName() );
        addrId = edm.getPropertyTypeId( ADDR_FQN.getNamespace(), ADDR_FQN.getName() );
        latId = edm.getPropertyTypeId( LAT_FQN.getNamespace(), LAT_FQN.getName() );
        lonId = edm.getPropertyTypeId( LON_FQN.getNamespace(), LON_FQN.getName() );

        UUID esId = edm.getEntityTypeId(
                ES_TYPE.getNamespace(), ES_TYPE.getName() );

        //        edm.createEntitySets( ImmutableSet.of( new EntitySet(
        //                esId,
        //                ES_NAME,
        //                "Salt Lake Lake City Stolen Cars (2012)",
        //                Optional.of(
        //                        "Cars stolen in Salt Lake City in 2012." ) ) ) );

        /*
         * Get the dataset.
         */
        // UUID entitySetId = edm.getEntitySetId( "slcstolencars2012" );
        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );
        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();
        // @formatter:off
        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "stolencar" )
                    .ofType( ES_TYPE )
                    .to( ES_NAME )
                    .key( CASE_FQN )
                    .addProperty( CASE_FQN ).value( row -> row.getAs( "CASE" ) ).ok()
                    .addProperty( OC_FQN ).value( row -> row.getAs( "OFFENSE CODE" ) ).ok()
                    .addProperty( OD_FQN ).value( row -> row.getAs( "OFFENSE DESCRIPTION" ) ).ok()
                    .addProperty( RD_FQN ).value( row -> row.getAs( "REPORT DATE" ) ).ok()
                    .addProperty( OCC_FQN ).value( row -> row.getAs( "OCC DATE" ) ).ok()
                    .addProperty( DOW_FQN )
                        .value( row -> {
                            try {
                                System.out.println( "Trying to read INT: " + row.getAs( "DAY OF WEEK" ) );
                                return Integer.parseInt( row.getAs( "DAY OF WEEK" ) );
                            } catch ( NumberFormatException e ) {
                                System.err.println( "Failed to read INT: " + row.getAs( "DAY OF WEEK" ) );
                                return null;
                            }
                        } )
                        .ok()
                    .addProperty( ADDR_FQN ).value( row -> row.getAs( "LOCATION" ) ).ok()
                    .addProperty( LAT_FQN ).value( SlcStolenCars::getLat ).ok()
                    .addProperty( LON_FQN ).value( SlcStolenCars::getLon ).ok()
                    .ok()
                .ok()
                .done();
        // @formatter:on
        flights.put( flight, payload );
        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );

    }

    public static double getLat( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        m.matches();
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 1 ) );
    }

    public static double getLon( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 2 ) );
    }
}
