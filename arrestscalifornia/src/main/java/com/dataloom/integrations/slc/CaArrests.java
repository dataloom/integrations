package com.dataloom.integrations.slc;

import java.io.File;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.request.AuthenticationRequest;
import com.dataloom.authorization.Ace;
import com.dataloom.authorization.Acl;
import com.dataloom.authorization.AclData;
import com.dataloom.authorization.Action;
import com.dataloom.authorization.Permission;
import com.dataloom.authorization.PermissionsApi;
import com.dataloom.authorization.Principal;
import com.dataloom.authorization.PrincipalType;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.Shuttle;

import retrofit2.Retrofit;

/**
 * This is the integration for CA Arrest Records from 1980 - 2015 (35 years of data).
 *
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 *         <p>
 *         The CSV fields are:
 *         "year","county","gender","race","age","offense_level","offense_code","disposition"
 */
public class CaArrests {
    private static final SparkSession sparkSession;
    private static final Logger                  logger  = LoggerFactory.getLogger( CaArrests.class );
    private static final Auth0                   auth0   = new Auth0(
            "PTmyExdBckHAiyOjh4w2MqSIUGWWEdf8",
            "loom.auth0.com" );
    private static final AuthenticationAPIClient client  = auth0.newAuthenticationAPIClient();
    public static        String                  ES_NAME = "caarrests";

    public static FullQualifiedName ES_TYPE = new FullQualifiedName( "publicsafety", "arrests" );

    public static FullQualifiedName GUID   = new FullQualifiedName( "general", "guid" );
    public static FullQualifiedName YEAR   = new FullQualifiedName( "general", "year" );
    public static FullQualifiedName COUNTY = new FullQualifiedName( "general", "county" );
    public static FullQualifiedName GENDER = new FullQualifiedName( "general", "gender" );
    public static FullQualifiedName RACE   = new FullQualifiedName( "general", "race" );
    public static FullQualifiedName AGE    = new FullQualifiedName( "general", "approximate_age" );
    public static FullQualifiedName OL     = new FullQualifiedName( "publicsafety", "offense_level" );

    private static FullQualifiedName CASE_FQN  = new FullQualifiedName( "publicsafety", "case" );
    private static FullQualifiedName DISPO_FQN = new FullQualifiedName( "publicsafety", "disposition" );
    private static FullQualifiedName OC_FQN    = new FullQualifiedName( "publicsafety", "offensecode" );
    private static FullQualifiedName OD_FQN    = new FullQualifiedName( "publicsafety", "offensedescription" );
    private static FullQualifiedName RD_FQN    = new FullQualifiedName( "publicsafety", "reportdate" );
    private static FullQualifiedName OCC_FQN   = new FullQualifiedName( "publicsafety", "occdate" );
    private static FullQualifiedName DOW_FQN   = new FullQualifiedName( "general", "dayofweek" );
    private static FullQualifiedName ADDR_FQN  = new FullQualifiedName( "general", "address" );
    private static FullQualifiedName LAT_FQN   = new FullQualifiedName( "location", "latitude" );
    private static Pattern           p         = Pattern.compile( ".*\\n*.*\\n*\\((.+),(.+)\\)" );
    private static FullQualifiedName LON_FQN   = new FullQualifiedName( "location", "longitude" );
    private static String jwtToken;

    static {
        sparkSession = SparkSession.builder()
                .master( "local[4]" )
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
        Function<PropertyType, UUID> createOrGet = createProperty( edm );

        Function<EntityType, UUID> createOrGetEntity = createEntity( edm );

        UUID guidId = createOrGet.apply( new PropertyType( GUID, "Guid", Optional.of(
                "A 128-bit unique identifier" ), ImmutableSet.of(), EdmPrimitiveTypeKind.Guid ) );

        UUID yearId = createOrGet.apply( new PropertyType( YEAR, "Year", Optional.of(
                "A Gregorian calendar year." ), ImmutableSet.of(), EdmPrimitiveTypeKind.Int16 ) );

        UUID countyId = createOrGet.apply( new PropertyType( COUNTY, "County", Optional.of(
                "A County in the United States" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );

        UUID genderId = createOrGet.apply( new PropertyType( GENDER, "Gender", Optional.of(
                "Gender of subject" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );

        UUID raceid = createOrGet.apply( new PropertyType( RACE, "Race", Optional.of(
                "Race of subject" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );

        UUID ageId = createOrGet.apply( new PropertyType( AGE, "Approximate Age", Optional.of(
                "Number of years since subject was created" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );

        UUID olId = createOrGet.apply( new PropertyType( OL, "Offense Level", Optional.of(
                "Offense Level" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );

        UUID ocId = createOrGet.apply( new PropertyType(
                OC_FQN,
                "Offense Code",
                Optional.of( "The code of the offense" ),
                ImmutableSet.of(),
                EdmPrimitiveTypeKind.String ) );

        UUID dispoId = createOrGet.apply( new PropertyType( DISPO_FQN, "Disposition", Optional.of(
                "Action taken" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
        UUID esId = createOrGetEntity.apply( new EntityType(
                ES_TYPE,
                "Arrest Records",
                "Deidentified arrest records",
                ImmutableSet.of(),
                ImmutableSet.of( guidId ),
                ImmutableSet.of( guidId, yearId, countyId, genderId, raceid, ageId, olId, ocId, dispoId ) ) );

        Set<EntitySet> entitySets = ImmutableSet.of( new EntitySet(
                esId,
                ES_NAME,
                "California Arrest Records 1985 - 2015",
                Optional.of(
                        "California law enforcement agencies report to the CA Department of Justice information on felony arrests and misdemeanor arrests occurring within the state. This Monthly Arrest and Citation Register data includes details of the arrest (date, offense, arrest), basic demographic information of the individual arrested, and limited information on the law enforcement disposition of the arrest. This file contains thirty-five years (1980-2015) of Arrest data." ) ) );
        Map<String, UUID> entitySetIds = edm.createEntitySets(entitySets);

        /*
         * Get the dataset.
         */
        UUID entitySetId;
        if( entitySetIds == null ) {
            entitySetId = edm.getEntitySetId( ES_NAME );
        } else {
            entitySetId = entitySetIds.values().iterator().next();
        }

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        // @formatter:off
        Flight flight = Flight.newFlight()
                .addEntity( ES_TYPE )
                    .to( ES_NAME )
                    .key( GUID )
                    .addProperty( GUID ).value( row -> UUID.randomUUID() ).ok()
                    .addProperty( YEAR ).value( row -> row.getAs( "year" ) ).ok()
                    .addProperty( COUNTY ).value( row -> row.getAs( "county" ) ).ok()
                    .addProperty( GENDER ).value( row -> row.getAs( "gender" ) ).ok()
                    .addProperty( RACE ).value( row -> row.getAs( "race" ) ).ok()
                    .addProperty( AGE ).value( row -> row.getAs( "age" ) ).ok()
                    .addProperty( OL ).value( row -> row.getAs( "offense_level" ) ).ok()
                    .addProperty( OC_FQN ).value( row -> row.getAs( "offense_code" ) ).ok()
                    .addProperty( DISPO_FQN ).value( row -> row.getAs( "disposition" ) ).ok()
                    .ok()
                .done();
        // @formatter:on

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flight, payload );
        PermissionsApi permissions = retrofit.create( PermissionsApi.class );

        Acl acl = new Acl( ImmutableList.of( entitySetId ),
                ImmutableList.of(
                        new Ace(
                                new Principal(
                                        PrincipalType.ROLE,
                                        "AuthenticatedUser" ),
                                EnumSet.of( Permission.READ )
                        ) ) );
        permissions.updateAcl(new AclData( acl, Action.ADD ) );
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

    public static Function<PropertyType, UUID> createProperty( EdmApi edm ) {
        return propertyType -> Optional.fromNullable( edm.createPropertyType( propertyType ) ).or( () -> edm
                .getPropertyTypeId( propertyType.getType().getNamespace(), propertyType.getType().getName() ) );
    }

    public static Function<EntityType, UUID> createEntity( EdmApi edm ) {
        return entityType -> Optional.fromNullable( edm.createEntityType( entityType ) ).or( () -> edm
                .getEntityTypeId( entityType.getType().getNamespace(), entityType.getType().getName() ) );
    }
}
