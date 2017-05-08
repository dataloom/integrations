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

public class GangData {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( GangData.class );

    // ENTITIES
    public static String            PEOPLE_ENTITY_SET_NAME  = "sacramentopeople";
    public static FullQualifiedName PEOPLE_ENTITY_SET_TYPE  = new FullQualifiedName( "nc.PersonType" );
    public static FullQualifiedName PEOPLE_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            PEOPLE_ALIAS            = "people";

    public static String            GANGS_ENTITY_SET_NAME   = "sacramentogangs";
    public static FullQualifiedName GANGS_ENTITY_SET_TYPE   = new FullQualifiedName( "publicsafety.GangType" );
    public static FullQualifiedName GANGS_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "publicsafety.GangCode" );
    public static String            GANGS_ALIAS             = "gangs";

    public static String            BIRTHS_ENTITY_SET_NAME   = "sacramentobirths";
    public static FullQualifiedName BIRTHS_ENTITY_SET_TYPE   = new FullQualifiedName( "general.LocationType" );
    public static FullQualifiedName BIRTHS_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "nc.LocationCityName" );
    public static FullQualifiedName BIRTHS_ENTITY_TYPE_KEY_2 = new FullQualifiedName( "nc.LocationStateName" );
    public static String            BIRTHS_ALIAS             = "births";

    public static String            HOMES_ENTITY_SET_NAME   = "sacramentohomes";
    public static FullQualifiedName HOMES_ENTITY_TYPE       = new FullQualifiedName( "general.LocationType" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "nc.StreetNumberText" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_2 = new FullQualifiedName( "nc.StreetName" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_3 = new FullQualifiedName( "nc.StreetCategoryText" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_4 = new FullQualifiedName( "nc.AddressSecondaryUnitText" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_5 = new FullQualifiedName( "nc.LocationCityName" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_6 = new FullQualifiedName( "nc.LocationStateName" );
    public static FullQualifiedName HOMES_ENTITY_TYPE_KEY_7 = new FullQualifiedName( "nc.LocationPostalCode" );
    public static String            HOMES_ALIAS             = "homes";

    public static String            WARRANTS_ENTITY_SET_NAME  = "sacramentowarrants";
    public static FullQualifiedName WARRANTS_ENTITY_SET_TYPE  = new FullQualifiedName( "j.WarrantType" );
    public static FullQualifiedName WARRANTS_ENTITY_SET_KEY_1 = new FullQualifiedName( "j.WarrantLevelText" );
    public static String            WARRANTS_ALIAS            = "warrants";

    // ASSOCIATIONS
    public static String            MEMBER_OF_ENTITY_SET_NAME      = "sacramentoismemberof";
    public static FullQualifiedName MEMBER_OF_ENTITY_SET_TYPE      = new FullQualifiedName( "j.PersonGangAssociation" );
    public static FullQualifiedName MEMBER_OF_ENTITY_SET_KEY_1     = new FullQualifiedName( "publicsafety.xref" );
    public static String            MEMBER_OF_ENTITY_SET_ALIAS     = "ismember";



    public static String            LIVED_IN_ENTITY_SET_NAME       = "sacramentohaslivedat";
    public static FullQualifiedName LIVED_IN_ENTITY_SET_TYPE       = new FullQualifiedName( "nc.PersonResidenceAssociation" );
    public static FullQualifiedName LIVED_IN_ENTITY_SET_KEY_1      = new FullQualifiedName( "publicsafety.xref" );
    public static String            LIVED_IN_ALIAS                 = "livedin";


    public static String            WAS_BORN_IN_ENTITY_SET_NAME    = "sacramentowasbornin";
    public static FullQualifiedName WAS_BORN_IN_ENTITY_SET_TYPE    = new FullQualifiedName( "j.PersonBirthLocationAssociation" );
    public static FullQualifiedName WAS_BORN_IN_ENTITY_SET_KEY_1   = new FullQualifiedName( "publicsafety.xref" );
    public static String            WAS_BORN_IN_ALIAS              = "wasbornin";


    public static String            HAS_FELONY_IN_ENTITY_SET_NAME  = "sacramentohasactivefelonywarrant";
    public static FullQualifiedName HAS_FELONY_IN_ENTITY_SET_TYPE  = new FullQualifiedName( "j.ActivityWarrantAssociation" );
    public static FullQualifiedName HAS_FELONY_IN_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            HAS_FELONY_ALIAS               = "hasactivefelonywarrant";

    public static String            HAS_MEMBERS_ENTITY_SET_NAME  = "sacramentoganghasmembers";
    public static FullQualifiedName HAS_MEMBERS_ENTITY_SET_TYPE  = new FullQualifiedName( "publicsafety.GangPersonAssociation" );
    public static FullQualifiedName HAS_MEMBERS_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.GangCode" );
    public static String            HAS_MEMBERS_ALIAS               = "hasmembers";

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
        String path = new File( GangData.class.getClassLoader().getResource( "GangMemberNullFix.csv" ).getPath() )
                .getAbsolutePath();

        //        final String username = "replace me with email username";
        //        final String password = "replace me with password";
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //        final String jwtToken = MissionControl.getIdToken( username, password );
        final String jwtToken = "replace me with jwtToken";
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



        LinkedHashSet<UUID> gangsKey = new LinkedHashSet<UUID>();
        gangsKey.add( Gang_Code );

        LinkedHashSet<UUID> gangsProperties = new LinkedHashSet<UUID>();
        gangsProperties.add( Gang_Code );
        gangsProperties.add( Gang_Name );

        UUID gangsType = edm.createEntityType( new EntityType(
                GANGS_ENTITY_SET_TYPE,
                "Gang Type",
                "Entity type for a gang",
                ImmutableSet.of(),
                gangsKey,
                gangsProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( gangsType == null ) {
            gangsType = edm.getEntityTypeId(
                    GANGS_ENTITY_SET_TYPE.getNamespace(), GANGS_ENTITY_SET_TYPE.getName() );
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
        locationsProperties.add( Birth_Country );
        locationsProperties.add( Address_Zip );
        locationsProperties.add( addressComment );

        UUID locationsType = edm.createEntityType( new EntityType(
                HOMES_ENTITY_TYPE,
                "Location Type",
                "Entity type for a location",
                ImmutableSet.of(),
                locationsKey,
                locationsProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( locationsType == null ) {
            locationsType = edm.getEntityTypeId(
                    HOMES_ENTITY_TYPE.getNamespace(), HOMES_ENTITY_TYPE.getName() );
        }




        LinkedHashSet<UUID> warrantsKey = new LinkedHashSet<UUID>();
        warrantsKey.add( hasActiveFelonyWarrant );

        LinkedHashSet<UUID> warrantsProperties = new LinkedHashSet<UUID>();
        warrantsProperties.add( hasActiveFelonyWarrant );

        UUID warrantsType = edm.createEntityType( new EntityType(
                WARRANTS_ENTITY_SET_TYPE,
                "Warrant Type",
                "Entity type for a warrant",
                ImmutableSet.of(),
                warrantsKey,
                warrantsProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( warrantsType == null ) {
            warrantsType = edm.getEntityTypeId(
                    WARRANTS_ENTITY_SET_TYPE.getNamespace(), WARRANTS_ENTITY_SET_TYPE.getName() );
        }




        // ASSOCIATIONS

        LinkedHashSet<UUID> ismemberKey = new LinkedHashSet<UUID>();
        ismemberKey.add( xref );

        LinkedHashSet<UUID> ismemberProperties = new LinkedHashSet<UUID>();
        ismemberProperties.add( xref );

        LinkedHashSet<UUID> ismemberSource = new LinkedHashSet<UUID>();
        ismemberSource.add( peopleType );
        LinkedHashSet<UUID> ismemberDestination = new LinkedHashSet<UUID>();
        ismemberDestination.add( gangsType );

        UUID ismemberType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                MEMBER_OF_ENTITY_SET_TYPE,
                "is a member of",
                "person is a member of gang association type",
                ImmutableSet.of(),
                ismemberKey,
                ismemberProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                ismemberSource,
                ismemberDestination,
                false
        ) );
        if ( ismemberType == null ) {
            ismemberType = edm.getEntityTypeId(
                    MEMBER_OF_ENTITY_SET_TYPE.getNamespace(), MEMBER_OF_ENTITY_SET_TYPE.getName() );
        }



        LinkedHashSet<UUID> livedinKey = new LinkedHashSet<UUID>();
        livedinKey.add( xref );

        LinkedHashSet<UUID> livedinProperties = new LinkedHashSet<UUID>();
        livedinProperties.add( xref );

        LinkedHashSet<UUID> livedinSource = new LinkedHashSet<UUID>();
        livedinSource.add( peopleType );
        LinkedHashSet<UUID> livedinDestination = new LinkedHashSet<UUID>();
        livedinDestination.add( locationsType );

        UUID livedinType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                LIVED_IN_ENTITY_SET_TYPE,
                "has lived at",
                "person has lived at location association type",
                ImmutableSet.of(),
                livedinKey,
                livedinProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                livedinSource,
                livedinDestination,
                false
        ) );
        if ( livedinType == null ) {
            livedinType = edm.getEntityTypeId(
                    LIVED_IN_ENTITY_SET_TYPE.getNamespace(), LIVED_IN_ENTITY_SET_TYPE.getName() );
        }



        LinkedHashSet<UUID> wasborninKey = new LinkedHashSet<UUID>();
        wasborninKey.add( xref );

        LinkedHashSet<UUID> wasborninProperties = new LinkedHashSet<UUID>();
        wasborninProperties.add( xref );

        LinkedHashSet<UUID> wasborninSource = new LinkedHashSet<UUID>();
        wasborninSource.add( peopleType );
        LinkedHashSet<UUID> wasborninDestination = new LinkedHashSet<UUID>();
        wasborninDestination.add( locationsType );

        UUID wasborninType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                WAS_BORN_IN_ENTITY_SET_TYPE,
                "was born in",
                "person was born in location association type",
                ImmutableSet.of(),
                wasborninKey,
                wasborninProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                wasborninSource,
                wasborninDestination,
                false
        ) );
        if ( wasborninType == null ) {
            wasborninType = edm.getEntityTypeId(
                    WAS_BORN_IN_ENTITY_SET_TYPE.getNamespace(), WAS_BORN_IN_ENTITY_SET_TYPE.getName() );
        }




        LinkedHashSet<UUID> hasactivefelonywarrantKey = new LinkedHashSet<UUID>();
        hasactivefelonywarrantKey.add( xref );

        LinkedHashSet<UUID> hasactivefelonywarrantProperties = new LinkedHashSet<UUID>();
        hasactivefelonywarrantProperties.add( xref );

        LinkedHashSet<UUID> hasactivefelonywarrantSource = new LinkedHashSet<UUID>();
        hasactivefelonywarrantSource.add( peopleType );
        LinkedHashSet<UUID> hasactivefelonywarrantDestination = new LinkedHashSet<UUID>();
        hasactivefelonywarrantDestination.add( warrantsType );

        UUID hasactivefelonywarrantType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                HAS_FELONY_IN_ENTITY_SET_TYPE,
                "has active warrant",
                "person has active warrant association type",
                ImmutableSet.of(),
                hasactivefelonywarrantKey,
                hasactivefelonywarrantProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                hasactivefelonywarrantSource,
                hasactivefelonywarrantDestination,
                false
        ) );
        if ( hasactivefelonywarrantType == null ) {
            hasactivefelonywarrantType = edm.getEntityTypeId(
                    HAS_FELONY_IN_ENTITY_SET_TYPE.getNamespace(), HAS_FELONY_IN_ENTITY_SET_TYPE.getName() );
        }


        LinkedHashSet<UUID> hasmembersKey = new LinkedHashSet<UUID>();
        hasmembersKey.add( Gang_Code );

        LinkedHashSet<UUID> hasmembersProperties = new LinkedHashSet<UUID>();
        hasmembersProperties.add( Gang_Code );

        LinkedHashSet<UUID> hasmembersSource = new LinkedHashSet<UUID>();
        hasmembersSource.add( gangsType );
        LinkedHashSet<UUID> hasmembersDestination = new LinkedHashSet<UUID>();
        hasmembersDestination.add( peopleType );

        UUID hasmembersType = edm.createAssociationType( new AssociationType( Optional.of( new EntityType(
                HAS_MEMBERS_ENTITY_SET_TYPE,
                "has members",
                "gang has people as members association type",
                ImmutableSet.of(),
                hasmembersKey,
                hasmembersProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.AssociationType ) ) ),
                hasmembersSource,
                hasmembersDestination,
                false
        ) );
        if ( hasmembersType == null ) {
            hasmembersType = edm.getEntityTypeId(
                    HAS_MEMBERS_ENTITY_SET_TYPE.getNamespace(), HAS_MEMBERS_ENTITY_SET_TYPE.getName() );
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

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                gangsType,
                GANGS_ENTITY_SET_NAME,
                "Gangs",
                Optional.of( "Sacramento Gangs" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                locationsType,
                BIRTHS_ENTITY_SET_NAME,
                "Birth Places",
                Optional.of( "Sacramento Gang Member Birth Locations" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                locationsType,
                HOMES_ENTITY_SET_NAME,
                "Homes",
                Optional.of( "Sacramento Gang Member Home Locations" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );
        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                warrantsType,
                WARRANTS_ENTITY_SET_NAME,
                "Warrants",
                Optional.of( "Active Warrants for Gang Members" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                ismemberType,
                MEMBER_OF_ENTITY_SET_NAME,
                "is a member of",
                Optional.of( "---- person is a member of gang association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                wasborninType,
                WAS_BORN_IN_ENTITY_SET_NAME,
                "was born in",
                Optional.of( "---- person was born in location association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                hasactivefelonywarrantType,
                HAS_FELONY_IN_ENTITY_SET_NAME,
                "has warrant status",
                Optional.of( "---- person has warrant status association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                livedinType,
                LIVED_IN_ENTITY_SET_NAME,
                "has lived at",
                Optional.of( "---- person has lived at location association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                hasmembersType,
                HAS_MEMBERS_ENTITY_SET_NAME,
                "has member",
                Optional.of( "---- gang has person as member association ----" ),
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

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "xref" ) ).ok()
                .addProperty( NAME_GIVEN_FQN )
                .value( row -> row.getAs( "Name_Given" ) ).ok()
                .addProperty( NAME_MIDDLE_FQN )
                .value( row -> row.getAs( "Name_Middle" ) ).ok()
                .addProperty( NAME_SUR_FQN )
                .value( row -> row.getAs( "Name_Sur" ) ).ok()
                .addProperty( SEX_FQN )
                .value( row -> row.getAs( "Sex" ) ).ok()
                .addProperty( RACE_FQN )
                .value( row -> row.getAs( "Race" ) ).ok()
                .addProperty( HEIGHT_FQN )
                .value( row -> row.getAs( "Height" ) ).ok()
                .addProperty( WEIGHT_FQN )
                .value( row -> row.getAs( "Weight" ) ).ok()
                .addProperty( HAIRCOLOR_FQN )
                .value( row -> row.getAs( "HairColor" ) ).ok()
                .addProperty( EYECOLOR_FQN )
                .value( row -> row.getAs( "EyeColor" ) ).ok()
                .addProperty( BIRTH_DATE_FQN )
                .value( row -> row.getAs( "Birth_Date" ) ).ok().ok()

                .addEntity( GANGS_ALIAS )
                .ofType( GANGS_ENTITY_SET_TYPE )
                .to( GANGS_ENTITY_SET_NAME )
                .key( GANGS_ENTITY_TYPE_KEY_1 )

                .addProperty( GANG_CODE_FQN )
                .value( row -> row.getAs( "Gang_Code" ) ).ok()
                .addProperty( GANG_NAME_FQN )
                .value( row -> row.getAs( "Gang_Name" ) ).ok().ok()

                .addEntity( BIRTHS_ALIAS )
                .ofType( BIRTHS_ENTITY_SET_TYPE )
                .to( BIRTHS_ENTITY_SET_NAME)
                .key( BIRTHS_ENTITY_TYPE_KEY_1, BIRTHS_ENTITY_TYPE_KEY_2 )

                .addProperty( BIRTH_CITY_FQN )
                .value( row -> row.getAs( "Birth_City" ) ).ok()
                .addProperty( BIRTH_STATE_FQN )
                .value( row -> row.getAs( "Birth_State" ) ).ok()
                .addProperty( BIRTH_COUNTRY_FQN )
                .value( row -> row.getAs( "Birth_Country" ) ).ok().ok()

                .addEntity( HOMES_ALIAS )
                .ofType( HOMES_ENTITY_TYPE )
                .to( HOMES_ENTITY_SET_NAME)
                .key( HOMES_ENTITY_TYPE_KEY_1,
                        HOMES_ENTITY_TYPE_KEY_2,
                        HOMES_ENTITY_TYPE_KEY_3,
                        HOMES_ENTITY_TYPE_KEY_4,
                        HOMES_ENTITY_TYPE_KEY_5,
                        HOMES_ENTITY_TYPE_KEY_6,
                        HOMES_ENTITY_TYPE_KEY_7 )

                .addProperty( ADDRESS_STREETNUMBER_FQN )
                .value( row -> row.getAs( "Address_StreetNumber" ) ).ok()
                .addProperty( ADDRESS_STREETNAME_FQN )
                .value( row -> row.getAs( "Address_StreetName" ) ).ok()
                .addProperty( ADDRESS_STREETSUFFIX_FQN )
                .value( row -> row.getAs( "Address_StreetSuffix" ) ).ok()
                .addProperty( ADDRESS_STREETUNIT_FQN )
                .value( row -> row.getAs( "Address_StreetUnit" ) ).ok()
                .addProperty( ADDRESS_CITY_FQN )
                .value( row -> row.getAs( "Address_City" ) ).ok()
                .addProperty( ADDRESS_STATE_FQN )
                .value( row -> row.getAs( "Address_State" ) ).ok()
                .addProperty( ADDRESS_ZIP_FQN )
                .value( row -> row.getAs( "Address_Zip" ) ).ok().ok()

                .addEntity( WARRANTS_ALIAS )
                .ofType( WARRANTS_ENTITY_SET_TYPE )
                .to( WARRANTS_ENTITY_SET_NAME )
                .key( WARRANTS_ENTITY_SET_KEY_1 )

                .addProperty( HASACTIVEFELONYWARRANT_FQN )
                .value( row -> row.getAs( "hasActiveFelonyWarrant" ) ).ok().ok()

                .ok()
                .createAssociations()

                .addAssociation( HAS_MEMBERS_ALIAS )
                .ofType( HAS_MEMBERS_ENTITY_SET_TYPE )
                .to( HAS_MEMBERS_ENTITY_SET_NAME )
                .key( HAS_MEMBERS_ENTITY_SET_KEY_1 )
                .fromEntity( GANGS_ALIAS )
                .toEntity( PEOPLE_ALIAS )

                .addProperty( GANG_CODE_FQN )
                .value( row -> row.getAs( "Gang_Code" ) ).ok().ok()


                .addAssociation( MEMBER_OF_ENTITY_SET_ALIAS )
                .ofType( MEMBER_OF_ENTITY_SET_TYPE )
                .to( MEMBER_OF_ENTITY_SET_NAME )
                .key( MEMBER_OF_ENTITY_SET_KEY_1 )
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( GANGS_ALIAS )

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "xref" ) ).ok().ok()

                .addAssociation( LIVED_IN_ALIAS )
                .ofType( LIVED_IN_ENTITY_SET_TYPE )
                .to( LIVED_IN_ENTITY_SET_NAME )
                .key( LIVED_IN_ENTITY_SET_KEY_1)
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( HOMES_ALIAS )

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "xref" ) ).ok().ok()

                .addAssociation( WAS_BORN_IN_ALIAS )
                .ofType( WAS_BORN_IN_ENTITY_SET_TYPE )
                .to( WAS_BORN_IN_ENTITY_SET_NAME )
                .key( WAS_BORN_IN_ENTITY_SET_KEY_1 )
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( BIRTHS_ALIAS )

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "xref" ) ).ok().ok()

                .addAssociation( HAS_FELONY_ALIAS )
                .ofType( HAS_FELONY_IN_ENTITY_SET_TYPE )
                .to( HAS_FELONY_IN_ENTITY_SET_NAME )
                .key( HAS_FELONY_IN_ENTITY_SET_KEY_1 )
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( WARRANTS_ALIAS )

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "xref" ) ).ok().ok()

                .ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW

}
