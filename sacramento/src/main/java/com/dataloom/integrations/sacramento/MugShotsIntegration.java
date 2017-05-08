package com.dataloom.integrations.sacramento;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;

import com.auth0.Auth0;
import com.auth0.authentication.AuthenticationAPIClient;
import com.auth0.request.AuthenticationRequest;
import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import retrofit2.Retrofit;

public class MugShotsIntegration {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( MugShotsIntegration.class );

    // ENTITIES
    public static String            PEOPLE_ENTITY_SET_NAME  = "sacramentopeople";
    public static FullQualifiedName PEOPLE_ENTITY_SET_TYPE  = new FullQualifiedName( "nc.PersonType" );
    public static FullQualifiedName PEOPLE_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            PEOPLE_ALIAS            = "people";

    // PROPERTIES
    public static FullQualifiedName GANG_CODE_FQN              = new FullQualifiedName( "publicsafety.GangCode" );
    public static FullQualifiedName GANG_NAME_FQN              = new FullQualifiedName( "publicsafety.GangName" );
    public static FullQualifiedName XREF_ID_FQN                = new FullQualifiedName( "publicsafety.xref" );
    public static FullQualifiedName NAME_GIVEN_FQN             = new FullQualifiedName( "nc.PersonGivenName" );
    public static FullQualifiedName NAME_MIDDLE_FQN            = new FullQualifiedName( "nc.PersonMiddleName" );
    public static FullQualifiedName NAME_SUR_FQN               = new FullQualifiedName( "nc.PersonSurName" );
    public static FullQualifiedName SEX_FQN                    = new FullQualifiedName( "nc.PersonSex" );
    public static FullQualifiedName RACE_FQN                   = new FullQualifiedName( "nc.PersonRace" );
    public static FullQualifiedName HEIGHT_FQN                 = new FullQualifiedName( "nc.PersonHeightMeasure" );
    public static FullQualifiedName WEIGHT_FQN                 = new FullQualifiedName( "nc.PersonWeightMeasure" );
    public static FullQualifiedName HAIRCOLOR_FQN              = new FullQualifiedName( "nc.PersonHairColorText" );
    public static FullQualifiedName EYECOLOR_FQN               = new FullQualifiedName( "nc.PersonEyeColorText" );
    public static FullQualifiedName BIRTH_CITY_FQN             = new FullQualifiedName( "nc.LocationCityName" );
    public static FullQualifiedName BIRTH_STATE_FQN            = new FullQualifiedName( "nc.LocationStateName" );
    public static FullQualifiedName BIRTH_COUNTRY_FQN          = new FullQualifiedName( "nc.LocationCountryName" );
    public static FullQualifiedName BIRTH_DATE_FQN             = new FullQualifiedName( "nc.PersonBirthDate" );
    public static FullQualifiedName ADDRESS_STREETNUMBER_FQN   = new FullQualifiedName( "nc.StreetNumberText" );
    public static FullQualifiedName ADDRESS_STREETNAME_FQN     = new FullQualifiedName( "nc.StreetName" );
    public static FullQualifiedName ADDRESS_STREETSUFFIX_FQN   = new FullQualifiedName( "nc.StreetCategoryText" );
    public static FullQualifiedName ADDRESS_STREETUNIT_FQN     = new FullQualifiedName( "nc.AddressSecondaryUnitText" );
    public static FullQualifiedName ADDRESS_CITY_FQN           = new FullQualifiedName( "nc.LocationCityName" );
    public static FullQualifiedName ADDRESS_STATE_FQN          = new FullQualifiedName( "nc.LocationStateName" );
    public static FullQualifiedName ADDRESS_ZIP_FQN            = new FullQualifiedName( "nc.LocationPostalCode" );
    public static FullQualifiedName HASACTIVEFELONYWARRANT_FQN = new FullQualifiedName( "j.WarrantLevelText" );
    public static FullQualifiedName ADDRESS_COMMENT_FQN         = new FullQualifiedName( "general.addresscomment" );
    public static FullQualifiedName MUG_SHOT_FQN                = new FullQualifiedName( "publicsafety.mugshot" );

    public static void main( String[] args ) throws InterruptedException {

        String path = new File( MugShotsIntegration.class.getClassLoader().getResource( "webmugs.csv" ).getPath() )
                .getAbsolutePath();

        //        final String username = "replace me with email username";
        //        final String password = "replace me with password";
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //        final String jwtToken = MissionControl.getIdToken( username, password );
        final String jwtToken = args[1];
        logger.info( "Using the following idToken: Bearer {}", jwtToken );
        
        /*
         * 
         * CREATE EDM OBJECTS
         * 
         */
        
        /*
         * PROPERTY TYPES
         */
        Retrofit retrofit = RetrofitFactory.newClient( Environment.PRODUCTION, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        UUID MugShot = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        MUG_SHOT_FQN,
                        "Mug Shot",
                        Optional.of( "An image for a subject." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Binary,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( MugShot == null ) {
            MugShot = edm.getPropertyTypeId( MUG_SHOT_FQN.getNamespace(), MUG_SHOT_FQN.getName() );
        }

        UUID Gang_Code = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GANG_CODE_FQN,
                        "Gang Code",
                        Optional.of( "An abbreviation, acronym, or code for an gang name." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Gang_Code == null ) {
            Gang_Code = edm.getPropertyTypeId( GANG_CODE_FQN.getNamespace(), GANG_CODE_FQN.getName() );
        }

        UUID Gang_Name = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GANG_NAME_FQN,
                        "Gang Name",
                        Optional.of( "A name of an gang." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Gang_Name == null ) {
            Gang_Name = edm.getPropertyTypeId( GANG_NAME_FQN.getNamespace(), GANG_NAME_FQN.getName() );
        }

        UUID xref = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        XREF_ID_FQN,
                        "XREF ID",
                        Optional.of( "An identifier." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( xref == null ) {
            xref = edm.getPropertyTypeId( XREF_ID_FQN.getNamespace(), XREF_ID_FQN.getName() );
        }

        UUID Name_Given = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NAME_GIVEN_FQN,
                        "Person Given Name",
                        Optional.of( "A first name of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Name_Given == null ) {
            Name_Given = edm.getPropertyTypeId( NAME_GIVEN_FQN.getNamespace(), NAME_GIVEN_FQN.getName() );
        }

        UUID Name_Middle = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NAME_MIDDLE_FQN,
                        "Person Middle Name",
                        Optional.of( "A middle name of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Name_Middle == null ) {
            Name_Middle = edm.getPropertyTypeId( NAME_MIDDLE_FQN.getNamespace(), NAME_MIDDLE_FQN.getName() );
        }

        UUID Name_Sur = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NAME_SUR_FQN,
                        "Person Sur Name",
                        Optional.of( "An original last name or surname of a person before changed by marriage." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Name_Sur == null ) {
            Name_Sur = edm.getPropertyTypeId( NAME_SUR_FQN.getNamespace(), NAME_SUR_FQN.getName() );
        }

        UUID Sex = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SEX_FQN,
                        "Person Sex",
                        Optional.of( "A data concept for a gender or sex of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Sex == null ) {
            Sex = edm.getPropertyTypeId( SEX_FQN.getNamespace(), SEX_FQN.getName() );
        }

        UUID Race = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RACE_FQN,
                        "Person Race",
                        Optional.of(
                                "A data concept for a classification of a person based on factors such as geographical locations and genetics." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Race == null ) {
            Race = edm.getPropertyTypeId( RACE_FQN.getNamespace(), RACE_FQN.getName() );
        }

        UUID Height = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HEIGHT_FQN,
                        "Person Height Measure",
                        Optional.of( "A measurement of the height of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Height == null ) {
            Height = edm.getPropertyTypeId( HEIGHT_FQN.getNamespace(), HEIGHT_FQN.getName() );
        }

        UUID Weight = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        WEIGHT_FQN,
                        "Person Weight Measure",
                        Optional.of( "A measurement of the weight of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Weight == null ) {
            Weight = edm.getPropertyTypeId( WEIGHT_FQN.getNamespace(), WEIGHT_FQN.getName() );
        }

        UUID HairColor = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HAIRCOLOR_FQN,
                        "Person Hair Color",
                        Optional.of( "A data concept for a color of the hair of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( HairColor == null ) {
            HairColor = edm.getPropertyTypeId( HAIRCOLOR_FQN.getNamespace(), HAIRCOLOR_FQN.getName() );
        }

        UUID EyeColor = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        EYECOLOR_FQN,
                        "Person Eye Color",
                        Optional.of( "A data concept for a color of the eyes of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( EyeColor == null ) {
            EyeColor = edm.getPropertyTypeId( EYECOLOR_FQN.getNamespace(), EYECOLOR_FQN.getName() );
        }

        UUID Birth_City = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BIRTH_CITY_FQN,
                        "Location City Name",
                        Optional.of( "A name of a city or town." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Birth_City == null ) {
            Birth_City = edm.getPropertyTypeId( BIRTH_CITY_FQN.getNamespace(), BIRTH_CITY_FQN.getName() );
        }

        UUID Birth_State = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BIRTH_STATE_FQN,
                        "Location State Name",
                        Optional.of(
                                "A name of a state, commonwealth, province, or other such geopolitical subdivision of a country." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Birth_State == null ) {
            Birth_State = edm.getPropertyTypeId( BIRTH_STATE_FQN.getNamespace(), BIRTH_STATE_FQN.getName() );
        }

        UUID Birth_Country = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BIRTH_COUNTRY_FQN,
                        "Location Country Name",
                        Optional.of(
                                "A name of a country, territory, dependency, or other such geopolitical subdivision of a location." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Birth_Country == null ) {
            Birth_Country = edm.getPropertyTypeId( BIRTH_COUNTRY_FQN.getNamespace(), BIRTH_COUNTRY_FQN.getName() );
        }

        UUID Birth_Date = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BIRTH_DATE_FQN,
                        "Person Birth Date",
                        Optional.of( "A date a person was born." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Birth_Date == null ) {
            Birth_Date = edm.getPropertyTypeId( BIRTH_DATE_FQN.getNamespace(), BIRTH_DATE_FQN.getName() );
        }

        UUID Address_StreetNumber = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_STREETNUMBER_FQN,
                        "Street Number",
                        Optional.of( "A number that identifies a particular unit or location within a street." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_StreetNumber == null ) {
            Address_StreetNumber = edm
                    .getPropertyTypeId( ADDRESS_STREETNUMBER_FQN.getNamespace(), ADDRESS_STREETNUMBER_FQN.getName() );
        }

        UUID Address_StreetName = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_STREETNAME_FQN,
                        "Street Name",
                        Optional.of( "A name of a street." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_StreetName == null ) {
            Address_StreetName = edm
                    .getPropertyTypeId( ADDRESS_STREETNAME_FQN.getNamespace(), ADDRESS_STREETNAME_FQN.getName() );
        }

        UUID Address_StreetSuffix = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_STREETSUFFIX_FQN,
                        "Street Category",
                        Optional.of( "A kind of street." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_StreetSuffix == null ) {
            Address_StreetSuffix = edm
                    .getPropertyTypeId( ADDRESS_STREETSUFFIX_FQN.getNamespace(), ADDRESS_STREETSUFFIX_FQN.getName() );
        }

        UUID Address_StreetUnit = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_STREETUNIT_FQN,
                        "Unit",
                        Optional.of( "A particular unit within a larger unit or grouping at a location." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_StreetUnit == null ) {
            Address_StreetUnit = edm
                    .getPropertyTypeId( ADDRESS_STREETUNIT_FQN.getNamespace(), ADDRESS_STREETUNIT_FQN.getName() );
        }

        UUID Address_City = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_CITY_FQN,
                        "Location City Name",
                        Optional.of( "A name of a city or town." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_City == null ) {
            Address_City = edm.getPropertyTypeId( ADDRESS_CITY_FQN.getNamespace(), ADDRESS_CITY_FQN.getName() );
        }

        UUID Address_State = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_STATE_FQN,
                        "Location State Name",
                        Optional.of(
                                "A name of a state, commonwealth, province, or other such geopolitical subdivision of a country." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_State == null ) {
            Address_State = edm.getPropertyTypeId( ADDRESS_STATE_FQN.getNamespace(), ADDRESS_STATE_FQN.getName() );
        }

        UUID Address_Zip = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_ZIP_FQN,
                        "Location Postal Code",
                        Optional.of( "An identifier of a post office-assigned zone for an address." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Address_Zip == null ) {
            Address_Zip = edm.getPropertyTypeId( ADDRESS_ZIP_FQN.getNamespace(), ADDRESS_ZIP_FQN.getName() );
        }

        UUID hasActiveFelonyWarrant = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HASACTIVEFELONYWARRANT_FQN,
                        "Warrant Level",
                        Optional.of( "An offense level associated with a warrant to be served." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( hasActiveFelonyWarrant == null ) {
            hasActiveFelonyWarrant = edm.getPropertyTypeId( HASACTIVEFELONYWARRANT_FQN.getNamespace(),
                    HASACTIVEFELONYWARRANT_FQN.getName() );
        }

        UUID addressComment = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADDRESS_COMMENT_FQN,
                        "Address Comment",
                        Optional.of( "A comment about an address" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( addressComment == null ) {
            addressComment = edm.getPropertyTypeId( ADDRESS_COMMENT_FQN.getNamespace(), ADDRESS_COMMENT_FQN.getName() );
        }


        
        /*
         * ENTITY TYPES
         */

        // ENTITY TYPES

        LinkedHashSet<UUID> peopleKey = new LinkedHashSet<UUID>();
        peopleKey.add( xref );

        LinkedHashSet<UUID> peopleProperties = new LinkedHashSet<UUID>();
        peopleProperties.add( xref );
        peopleProperties.add( Name_Given );
        peopleProperties.add( Name_Middle );
        peopleProperties.add( Name_Sur );
        peopleProperties.add( Sex );
        peopleProperties.add( Race );
        peopleProperties.add( Height );
        peopleProperties.add( Weight );
        peopleProperties.add( HairColor );
        peopleProperties.add( EyeColor );
        peopleProperties.add( Birth_Date );
        peopleProperties.add( MugShot );

        UUID peopleType = edm.createEntityType( new EntityType(
                PEOPLE_ENTITY_SET_TYPE,
                "Person Type",
                "Entity type for a person",
                ImmutableSet.of(),
                peopleKey,
                peopleProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( peopleType == null ) {
            peopleType = edm.getEntityTypeId(
                    PEOPLE_ENTITY_SET_TYPE.getNamespace(), PEOPLE_ENTITY_SET_TYPE.getName() );
        }
        
        /*
         * ENTITY SETS
         */

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                peopleType,
                PEOPLE_ENTITY_SET_NAME,
                "Gang Members",
                Optional.of( "Sacramento Gang Members KPF" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );


        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( PEOPLE_ALIAS )
                .ofType( PEOPLE_ENTITY_SET_TYPE )
                .to( PEOPLE_ENTITY_SET_NAME )
                .key( PEOPLE_ENTITY_SET_KEY_1 )
                .useCurrentSync()

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "XREF" ) ).ok()
                .addProperty( MUG_SHOT_FQN )
                .value( row -> row.getAs( "mugshot" ) ).ok().ok()

                .ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );
    }

}
