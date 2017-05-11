package com.dataloom.integrations.sacramento;

import java.awt.*;
import java.io.File;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;
import com.sun.org.apache.regexp.internal.RE;
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

public class CadCalls {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( CadCalls.class );

    // ENTITIES
    public static String            PEOPLE_ENTITY_SET_NAME  = "sacramentopeople";
    public static FullQualifiedName PEOPLE_ENTITY_SET_TYPE  = new FullQualifiedName( "nc.PersonType" );
    public static FullQualifiedName PEOPLE_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            PEOPLE_ALIAS            = "people";

    public static String            CAD_CALLS_ENTITY_SET_NAME   = "sacramentocad";
    public static FullQualifiedName CAD_CALLS_ENTITY_SET_TYPE   = new FullQualifiedName( "j.ServiceCallType" );
    public static FullQualifiedName CAD_CALLS_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "publicsafety.CadEventNumber" );
    public static String            CAD_CALLS_ALIAS             = "calls";

    public static String            LOCATIONS_ENTITY_SET_NAME   = "sacramentocadlocation";
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE       = new FullQualifiedName( "general.LocationType" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "nc.StreetNumberText" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_2 = new FullQualifiedName( "nc.StreetName" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_3 = new FullQualifiedName( "nc.StreetCategoryText" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_4 = new FullQualifiedName( "nc.AddressSecondaryUnitText" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_5 = new FullQualifiedName( "nc.LocationCityName" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_6 = new FullQualifiedName( "nc.LocationStateName" );
    public static FullQualifiedName LOCATIONS_ENTITY_TYPE_KEY_7 = new FullQualifiedName( "nc.LocationPostalCode" );
    public static String            LOCATIONS_ALIAS             = "locations";



    // ASSOCIATIONS
    public static String            WAS_RELATED_ENTITY_SET_NAME      = "sacramentowasrelated";
    public static FullQualifiedName WAS_RELATED_ENTITY_SET_TYPE      = new FullQualifiedName( "publicsafety.RelatedPersonServiceCallAssociation" );
    public static FullQualifiedName WAS_RELATED_ENTITY_SET_KEY_1     = new FullQualifiedName( "publicsafety.xref" );
    public static String            WAS_RELATED_ENTITY_SET_ALIAS     = "wasrelatedsubject";



    public static String            OCCURRED_AT_ENTITY_SET_NAME       = "sacramentooccuredat";
    public static FullQualifiedName OCCURRED_AT_ENTITY_SET_TYPE       = new FullQualifiedName( "j.ServiceCallLocationAssociationType2" );
    public static FullQualifiedName OCCURRED_AT_ENTITY_SET_KEY_1      = new FullQualifiedName( "publicsafety.CadEventNumber" );
    public static String            OCCURRED_AT_ALIAS                 = "occurredat";


    public static String            WAS_SUBJECT_IN_ENTITY_SET_NAME    = "sacramentowasmainsubject";
    public static FullQualifiedName WAS_SUBJECT_IN_ENTITY_SET_TYPE    = new FullQualifiedName( "publicsafety.PersonServiceCallAssociation" );
    public static FullQualifiedName WAS_SUBJECT_IN_ENTITY_SET_KEY_1   = new FullQualifiedName( "publicsafety.xref" );
    public static String            WAS_SUBJECT_IN_ALIAS              = "wasmainsubject";



    // PROPERTIES
    public static FullQualifiedName EVENT_NUMBER_FQN           = new FullQualifiedName( "publicsafety.CadEventNumber" );
    public static FullQualifiedName FINAL_CALL_TYPE_FQN            = new FullQualifiedName( "j.ServiceCallCategoryText" );
    public static FullQualifiedName HOW_RECEIVED_FQN            = new FullQualifiedName( "j.ServiceCallMechanismText" );

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
        String path = new File( CadCalls.class.getClassLoader().getResource( "CADAddressFix.csv" ).getPath() )
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

        UUID FinalCallType = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        FINAL_CALL_TYPE_FQN,
                        "Service Call Category",
                        Optional.of( "A kind of service call as determined at the time of call receipt." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( FinalCallType == null ) {
            FinalCallType = edm.getPropertyTypeId( FINAL_CALL_TYPE_FQN.getNamespace(), FINAL_CALL_TYPE_FQN.getName() );
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

        UUID Country = edm
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
        if ( Country == null ) {
            Country = edm.getPropertyTypeId( BIRTH_COUNTRY_FQN.getNamespace(), BIRTH_COUNTRY_FQN.getName() );
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

        UUID EventNumber = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        EVENT_NUMBER_FQN,
                        "Event Number",
                        Optional.of( "An identifier for a CAD Event." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( EventNumber == null ) {
            EventNumber = edm.getPropertyTypeId( EVENT_NUMBER_FQN.getNamespace(),
                    EVENT_NUMBER_FQN.getName() );
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

        UUID HowReceived = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HOW_RECEIVED_FQN,
                        "How Received",
                        Optional.of( "A way in which a call for service is received." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( HowReceived == null ) {
            HowReceived = edm.getPropertyTypeId( HOW_RECEIVED_FQN.getNamespace(), HOW_RECEIVED_FQN.getName() );
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


        LinkedHashSet<UUID> cadKeys = new LinkedHashSet<UUID>();
        cadKeys.add( EventNumber );

        LinkedHashSet<UUID> cadProperties = new LinkedHashSet<UUID>();
        cadProperties.add( EventNumber );
        cadProperties.add( HowReceived );
        cadProperties.add( FinalCallType );


        UUID cadType = edm.createEntityType( new EntityType(
                CAD_CALLS_ENTITY_SET_TYPE,
                "Service Call",
                "Entity type for a service call",
                ImmutableSet.of(),
                cadKeys,
                cadProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( cadType == null ) {
            cadType = edm.getEntityTypeId(
                    CAD_CALLS_ENTITY_SET_TYPE.getNamespace(), CAD_CALLS_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> locationsKey = new LinkedHashSet<UUID>();
        locationsKey.add( Address_StreetNumber );
        locationsKey.add( Address_StreetName );
        locationsKey.add( Address_StreetSuffix );
        locationsKey.add( Address_StreetUnit );
        locationsKey.add( Address_City );
        locationsKey.add( Address_State );
        locationsKey.add( Address_Zip );

        LinkedHashSet<UUID> locationsProperties = new LinkedHashSet<UUID>();
        locationsProperties.add( Address_StreetNumber );
        locationsProperties.add( Address_StreetName );
        locationsProperties.add( Address_StreetSuffix );
        locationsProperties.add( Address_StreetUnit );
        locationsProperties.add( Address_City );
        locationsProperties.add( Address_State );
        locationsProperties.add( Address_Zip );
        locationsProperties.add( Country );
        locationsProperties.add( addressComment );

        UUID locationsType = edm.createEntityType( new EntityType(
                LOCATIONS_ENTITY_TYPE,
                "Location Type",
                "Entity type for a location",
                ImmutableSet.of(),
                locationsKey,
                locationsProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( locationsType == null ) {
            locationsType = edm.getEntityTypeId(
                    LOCATIONS_ENTITY_TYPE.getNamespace(), LOCATIONS_ENTITY_TYPE.getName() );
        }




        // ASSOCIATIONS

        LinkedHashSet<UUID> wasRelatedKey = new LinkedHashSet<UUID>();
        wasRelatedKey.add( xref );

        LinkedHashSet<UUID> wasRelatedProperties = new LinkedHashSet<UUID>();
        wasRelatedProperties.add( xref );

        LinkedHashSet<UUID> wasRelatedSource = new LinkedHashSet<UUID>();
        wasRelatedSource.add( peopleType );
        LinkedHashSet<UUID> wasRelatedDestination = new LinkedHashSet<UUID>();
        wasRelatedDestination.add( cadType );

        UUID wasRelatedType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                WAS_RELATED_ENTITY_SET_TYPE,
                "was related subject in",
                "person was related subject in event association type",
                ImmutableSet.of(),
                wasRelatedKey,
                wasRelatedProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                wasRelatedSource,
                wasRelatedDestination,
                false
        ) );
        if ( wasRelatedType == null ) {
            wasRelatedType = edm.getEntityTypeId( WAS_RELATED_ENTITY_SET_TYPE.getNamespace(), WAS_RELATED_ENTITY_SET_TYPE.getName() );
        }


        LinkedHashSet<UUID> wasMainSubjectKey = new LinkedHashSet<UUID>();
        wasMainSubjectKey.add( xref );

        LinkedHashSet<UUID> wasMainSubjectProperties = new LinkedHashSet<UUID>();
        wasMainSubjectProperties.add( xref );

        LinkedHashSet<UUID> wasMainSubjectSource = new LinkedHashSet<UUID>();
        wasMainSubjectSource.add( peopleType );
        LinkedHashSet<UUID> wasMainSubjectDestination = new LinkedHashSet<UUID>();
        wasMainSubjectDestination.add( cadType );

        UUID wasMainSubjectType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                WAS_SUBJECT_IN_ENTITY_SET_TYPE,
                "was main subject in",
                "person was main subject in event association type",
                ImmutableSet.of(),
                wasMainSubjectKey,
                wasMainSubjectProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                wasMainSubjectSource,
                wasMainSubjectDestination,
                false
        ) );
        if ( wasMainSubjectType == null ) {
            wasMainSubjectType = edm.getEntityTypeId(
                    WAS_SUBJECT_IN_ENTITY_SET_TYPE.getNamespace(), WAS_SUBJECT_IN_ENTITY_SET_TYPE.getName() );
        }


        LinkedHashSet<UUID> occurredAtKey = new LinkedHashSet<UUID>();
        occurredAtKey.add( EventNumber );

        LinkedHashSet<UUID> occurredAtProperties = new LinkedHashSet<UUID>();
        occurredAtProperties.add( EventNumber );

        LinkedHashSet<UUID> occurredAtSource = new LinkedHashSet<UUID>();
        occurredAtSource.add( cadType );
        LinkedHashSet<UUID> occurredAtDestination = new LinkedHashSet<UUID>();
        occurredAtDestination.add( locationsType );

        UUID occurredAtType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        OCCURRED_AT_ENTITY_SET_TYPE,
                        "occurred at",
                        "call for service occurred at location association type",
                        ImmutableSet.of(),
                        occurredAtKey,
                        occurredAtProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                occurredAtSource,
                occurredAtDestination,
                false
        ) );
        if ( occurredAtType == null ) {
            occurredAtType = edm.getEntityTypeId(
                    OCCURRED_AT_ENTITY_SET_TYPE.getNamespace(), OCCURRED_AT_ENTITY_SET_TYPE.getName() );
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


        // NEW entity sets

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                cadType,
                CAD_CALLS_ENTITY_SET_NAME,
                "CAD Calls",
                Optional.of( "Sacramento CAD Calls" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                locationsType,
                LOCATIONS_ENTITY_SET_NAME,
                "CAD Call Locations",
                Optional.of( "Sacramento CAD Call Locations" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        // NEW association sets

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                wasRelatedType,
                WAS_RELATED_ENTITY_SET_NAME,
                "was related subject in",
                Optional.of( "---- person was a related subject in cad call association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                wasMainSubjectType,
                WAS_SUBJECT_IN_ENTITY_SET_NAME,
                "was main subject in",
                Optional.of( "---- person was main subject in cad call association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                occurredAtType,
                OCCURRED_AT_ENTITY_SET_NAME,
                "occurred at",
                Optional.of( "---- call event occurred at location association ----" ),
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
                .value( row -> row.getAs( "XREF" ) ).ok().ok()

                .addEntity( "people2" )
                .ofType( PEOPLE_ENTITY_SET_TYPE )
                .to( PEOPLE_ENTITY_SET_NAME )
                .key( PEOPLE_ENTITY_SET_KEY_1 )
                .useCurrentSync()

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "RelatedXREFs" ) ).ok().ok()

                .addEntity( CAD_CALLS_ALIAS )
                .ofType( CAD_CALLS_ENTITY_SET_TYPE )
                .to( CAD_CALLS_ENTITY_SET_NAME )
                .key( CAD_CALLS_ENTITY_TYPE_KEY_1 )
                .useCurrentSync()

                .addProperty( EVENT_NUMBER_FQN )
                .value( row -> row.getAs( "EventNumber" ) ).ok()
                .addProperty( HOW_RECEIVED_FQN )
                .value( row -> getHowReceived( row.getAs( "HowReceived" ) ) ).ok()
                .addProperty( FINAL_CALL_TYPE_FQN )
                .value( row -> row.getAs( "FinalCallType" ) ).ok().ok()

                .addEntity( LOCATIONS_ALIAS )
                .ofType( LOCATIONS_ENTITY_TYPE )
                .to( LOCATIONS_ENTITY_SET_NAME )
                .key( LOCATIONS_ENTITY_TYPE_KEY_1,
                        LOCATIONS_ENTITY_TYPE_KEY_2,
                        LOCATIONS_ENTITY_TYPE_KEY_3,
                        LOCATIONS_ENTITY_TYPE_KEY_4,
                        LOCATIONS_ENTITY_TYPE_KEY_5,
                        LOCATIONS_ENTITY_TYPE_KEY_6,
                        LOCATIONS_ENTITY_TYPE_KEY_7 )
                .useCurrentSync()

                .addProperty( ADDRESS_COMMENT_FQN )
                .value( row -> row.getAs( "PlaceName" ) ).ok()
                .addProperty( ADDRESS_STREETNUMBER_FQN )
                .value( row -> getStreetNumber( row.getAs( "LocationAddress" ) ) ).ok()
                .addProperty( ADDRESS_STREETNAME_FQN )
                .value( row -> getStreetName( row.getAs( "LocationAddress" ) ) ).ok()
                .addProperty( ADDRESS_STREETSUFFIX_FQN )
                .value( row -> getStreetSuffix( row.getAs( "LocationAddress" ) )  ).ok()
                .addProperty( ADDRESS_STREETUNIT_FQN )
                .value( row -> row.getAs( "LocationAptNo" ) ).ok()


                .ok().ok()
                .createAssociations()

                .addAssociation( WAS_RELATED_ENTITY_SET_ALIAS )
                .ofType( WAS_RELATED_ENTITY_SET_TYPE )
                .to( WAS_RELATED_ENTITY_SET_NAME )
                .key( WAS_RELATED_ENTITY_SET_KEY_1 )
                .fromEntity( "people2" )
                .toEntity( CAD_CALLS_ALIAS )
                .useCurrentSync()

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "RelatedXREFs" ) ).ok().ok()


                .addAssociation( OCCURRED_AT_ALIAS )
                .ofType( OCCURRED_AT_ENTITY_SET_TYPE )
                .to( OCCURRED_AT_ENTITY_SET_NAME )
                .key( OCCURRED_AT_ENTITY_SET_KEY_1 )
                .fromEntity( CAD_CALLS_ALIAS )
                .toEntity( LOCATIONS_ALIAS )
                .useCurrentSync()

                .addProperty( EVENT_NUMBER_FQN )
                .value( row -> row.getAs( "EventNumber" ) ).ok().ok()

                .addAssociation( WAS_SUBJECT_IN_ALIAS )
                .ofType( WAS_SUBJECT_IN_ENTITY_SET_TYPE )
                .to( WAS_SUBJECT_IN_ENTITY_SET_NAME )
                .key( WAS_SUBJECT_IN_ENTITY_SET_KEY_1)
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( CAD_CALLS_ALIAS )
                .useCurrentSync()

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "XREF" ) ).ok()

                .ok().ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW


    public static String getStreetNumber( Object input ) {
        if ( input != null ) {
            String mystr = input.toString();
            if ( !mystr.contains( "/" ) && !mystr.contains( "[" ) ) {
                String[] words = mystr.split( " " );
                //            logger.info("words: {}", words[0]);
                if ( Character.isDigit( words[ 0 ].charAt( 0 ) ) ) {
                    return words[ 0 ];
                }
            } else if ( !mystr.contains( "/" ) && mystr.contains( "[" ) ) {
                String[] words = mystr.split( "\\[" )[ 0 ].split( " " );
                if ( Character.isDigit( words[ 0 ].charAt( 0 ) ) ) {
                    return words[ 0 ];
                }
            }
            return null;
        }
        return null;
    }

    public static String getStreetName( Object input ) {
        if (input != null) {
            String mystr = input.toString();
            if ( mystr.contains("VARIOUS") ||  mystr.contains("HOMELESS") || mystr.contains("LAT:") || mystr.contains("TRANSIENTS") || mystr.contains( "/" )) {
                return mystr;
            }
            String[] r = mystr.split(" ");
            if (r.length > 1) {
                r = (String[]) Arrays.copyOfRange(r, 1, r.length);
            }
            if (Character.isDigit(mystr.split(" ")[0].charAt(0))) {
                return String.join(" ", r);
            } else {
                return mystr;
            }
        }
        return null;
    }

    public static String getStreetSuffix( Object input ) {
        if (input != null) {
            String mystr = input.toString();
            if ( mystr.contains("VARIOUS") ||  mystr.contains("HOMELESS") || mystr.contains("LAT:") || mystr.contains("TRANSIENTS") || mystr.contains( "/" )) {
                return null;
            }
            String[] r = mystr.split( " " );
            String last = r[ r.length - 1 ]; // last word
            return last;
        }
        return null;
    }

    public static String getHowReceived( Object input ) {
        if (input != null) {
            String val = input.toString();
            if (val.equals("9")) {
                return "E911 SYSTEM";
            } else if (val.equals("A") ) {
                return "ALARM COMPANY";
            } else if (val.equals("D")) {
                return "OFFICER REQUEST";
            } else if (val.equals("O")) {
                return "OUTSIDE AGENCY";
            } else if (val.equals("Q")) {
                return "RECURRING CALL";
            } else if (val.equals("S")) {
                return "SERVICE CENTER / STATION HOUSE";
            } else if (val.equals("T")) {
                return "TELEPHONE";
            } else if (val.equals("V")) {
                return "VEHICLE";
            }
            return null;
        }
        return null;
    }

}
