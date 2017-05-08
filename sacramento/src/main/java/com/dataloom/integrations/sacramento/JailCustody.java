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

public class JailCustody {
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger( JailCustody.class );

    // ENTITIES
    public static String            PEOPLE_ENTITY_SET_NAME  = "sacramentopeople";
    public static FullQualifiedName PEOPLE_ENTITY_SET_TYPE  = new FullQualifiedName( "nc.PersonType" );
    public static FullQualifiedName PEOPLE_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            PEOPLE_ALIAS            = "people";

    public static String            BOOKINGS_ENTITY_SET_NAME   = "sacramentojail";
    public static FullQualifiedName BOOKINGS_ENTITY_SET_TYPE   = new FullQualifiedName( "j.BookingType" );
    public static FullQualifiedName BOOKINGS_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "publicsafety.CustodyID" );
    public static String            BOOKINGS_ALIAS             = "bookings";

    public static String            CHARGES_ENTITY_SET_NAME   = "sacramentocharges";
    public static FullQualifiedName CHARGES_ENTITY_SET_TYPE   = new FullQualifiedName( "j.ChargeType" );
    public static FullQualifiedName CHARGES_ENTITY_TYPE_KEY_1 = new FullQualifiedName( "j.ArrestCharge" );
    public static FullQualifiedName CHARGES_ENTITY_TYPE_KEY_2 = new FullQualifiedName( "publicsafety.datereleased2" );
    public static String            CHARGES_ALIAS             = "charges";

    // ASSOCIATIONS
    public static String            WAS_BOOKED_ENTITY_SET_NAME  = "sacramentowasbooked";
    public static FullQualifiedName WAS_BOOKED_ENTITY_SET_TYPE  = new FullQualifiedName( "publicsafety.PersonJailBookingAssociation" );
    public static FullQualifiedName WAS_BOOKED_ENTITY_SET_KEY_1 = new FullQualifiedName( "publicsafety.xref" );
    public static String            WAS_BOOKED_ENTITY_SET_ALIAS = "wasbooked";

    public static String            CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_NAME  = "sacramentoinvolvescharges";
    public static FullQualifiedName CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_TYPE  = new FullQualifiedName( "publicsafety.ChargeAppearsInBookingAssociation" );
    public static FullQualifiedName CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_KEY_1 = new FullQualifiedName( "j.ArrestCharge" );
    public static FullQualifiedName CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_KEY_2 = new FullQualifiedName( "publicsafety.datereleased2" );
    public static String            CHARGE_APPEARS_IN_BOOKING_ALIAS            = "involvescharges";

    // PROPERTIES
    public static FullQualifiedName CUSTODY_ID_FQN           = new FullQualifiedName( "publicsafety.CustodyID" );
    public static FullQualifiedName CUSTODY_EVENT_NUMBER_FQN = new FullQualifiedName( "publicsafety.CustodyEventNumber" );
    public static FullQualifiedName BOOKING_CODE_FQN         = new FullQualifiedName( "j.SubjectBooking" );
    public static FullQualifiedName RELEASE_CODE_FQN         = new FullQualifiedName( "j.BookingRelease" );
    public static FullQualifiedName BOOKING_DATETIME_FQN     = new FullQualifiedName( "publicsafety.datebooked2" );
    public static FullQualifiedName RELEASE_DATETIME_FQN     = new FullQualifiedName( "publicsafety.datereleased2" );
    public static FullQualifiedName CHARGE_SEQUENCE_FQN        = new FullQualifiedName( "j.ChargeSequenceID" );
    public static FullQualifiedName ARREST_CHARGE_FQN          = new FullQualifiedName( "j.ArrestCharge" );
    public static FullQualifiedName TOP_CHARGE_DESCRIPTION_FQN = new FullQualifiedName( "j.ChargeDescriptionText" );
    public static FullQualifiedName DOCKET_FQN                 = new FullQualifiedName( "nc.CaseDocketID" );
    public static FullQualifiedName COURT_FILE_FQN             = new FullQualifiedName( "j.CourtEventCase" );
    public static FullQualifiedName VIOLATION_COUNT_FQN        = new FullQualifiedName( "publicsafety.ViolationCount" );
    public static FullQualifiedName CRIME_REPORT_NUMBER_FQN    = new FullQualifiedName( "publicsafety.CrimeReportNumber" );

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
        String path = new File( JailCustody.class.getClassLoader().getResource( "CustodySpaceFix.csv" )
                .getPath() )
                .getAbsolutePath();

        //        final String username = "replace me with email username";
        //        final String password = "replace me with password";
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //        final String jwtToken = MissionControl.getIdToken( username, password );
        final String jwtToken = args[0];
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

        UUID CustodyID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CUSTODY_ID_FQN,
                        "Custody ID",
                        Optional.of( "An identifier for jail custody." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( CustodyID == null ) {
            CustodyID = edm.getPropertyTypeId( CUSTODY_ID_FQN.getNamespace(), CUSTODY_ID_FQN.getName() );
        }

        UUID CustodyEventNumber = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CUSTODY_EVENT_NUMBER_FQN,
                        "Custody Event Number",
                        Optional.of( "A jail custody event number." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( CustodyEventNumber == null ) {
            CustodyEventNumber = edm.getPropertyTypeId( CUSTODY_EVENT_NUMBER_FQN.getNamespace(), CUSTODY_EVENT_NUMBER_FQN.getName() );
        }

        UUID BookingCode = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOOKING_CODE_FQN,
                        "Booking Code",
                        Optional.of( "A code for a booking event associated with a corrections subject." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( BookingCode == null ) {
            BookingCode = edm.getPropertyTypeId( BOOKING_CODE_FQN.getNamespace(), BOOKING_CODE_FQN.getName() );
        }

        UUID ReleaseCode = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RELEASE_CODE_FQN,
                        "Release Code",
                        Optional.of( "A release of a subject from booking or from booking detention." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ReleaseCode == null ) {
            ReleaseCode = edm.getPropertyTypeId( RELEASE_CODE_FQN.getNamespace(), RELEASE_CODE_FQN.getName() );
        }

        UUID BookingDateTime = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOOKING_DATETIME_FQN,
                        "Booking Date",
                        Optional.of( "A datetime of a booking." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( BookingDateTime == null ) {
            BookingDateTime = edm.getPropertyTypeId( BOOKING_DATETIME_FQN.getNamespace(), BOOKING_DATETIME_FQN.getName() );
        }

        UUID ReleaseDateTime = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RELEASE_DATETIME_FQN,
                        "Release Date",
                        Optional.of( "A datetime of release from a booking or charge." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ReleaseDateTime == null ) {
            ReleaseDateTime = edm.getPropertyTypeId( RELEASE_DATETIME_FQN.getNamespace(), RELEASE_DATETIME_FQN.getName() );
        }

        UUID ChargeSequence = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGE_SEQUENCE_FQN,
                        "Charge Sequence",
                        Optional.of( "A sequentially assigned identifier for charge tracking purposes." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ChargeSequence == null ) {
            ChargeSequence = edm.getPropertyTypeId( CHARGE_SEQUENCE_FQN.getNamespace(), CHARGE_SEQUENCE_FQN.getName() );
        }

        UUID ArrestCharge = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ARREST_CHARGE_FQN,
                        "Arrest Charge",
                        Optional.of( "A formal allegation of a violation of a statute and/or ordinance in association with an arrest." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ArrestCharge == null ) {
            ArrestCharge = edm.getPropertyTypeId( ARREST_CHARGE_FQN.getNamespace(), ARREST_CHARGE_FQN.getName() );
        }

        UUID TopChargeDescription = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TOP_CHARGE_DESCRIPTION_FQN,
                        "Top Charge Description",
                        Optional.of( "Description of the top charge." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( TopChargeDescription == null ) {
            TopChargeDescription = edm.getPropertyTypeId( TOP_CHARGE_DESCRIPTION_FQN.getNamespace(), TOP_CHARGE_DESCRIPTION_FQN.getName() );
        }

        UUID Docket = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        DOCKET_FQN,
                        "Docket",
                        Optional.of( "An identifier used to reference a case docket." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Docket == null ) {
            Docket = edm.getPropertyTypeId( DOCKET_FQN.getNamespace(), DOCKET_FQN.getName() );
        }

        UUID CourtFile = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        COURT_FILE_FQN,
                        "Court File",
                        Optional.of( "A case associated with a court event." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( CourtFile == null ) {
            CourtFile = edm.getPropertyTypeId( COURT_FILE_FQN.getNamespace(), COURT_FILE_FQN.getName() );
        }

        UUID ViolationCount = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        VIOLATION_COUNT_FQN,
                        "Violation Count",
                        Optional.of( "Quantity of violations." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int64,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ViolationCount == null ) {
            ViolationCount = edm.getPropertyTypeId( VIOLATION_COUNT_FQN.getNamespace(), VIOLATION_COUNT_FQN.getName() );
        }

        UUID CrimeReportNumber = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CRIME_REPORT_NUMBER_FQN,
                        "Crime Report Number",
                        Optional.of( "Crime report number." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( CrimeReportNumber == null ) {
            CrimeReportNumber = edm.getPropertyTypeId( CRIME_REPORT_NUMBER_FQN.getNamespace(), CRIME_REPORT_NUMBER_FQN.getName() );
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

        LinkedHashSet<UUID> bookingsKeys = new LinkedHashSet<UUID>();
        bookingsKeys.add( CustodyID );

        LinkedHashSet<UUID> bookingsProperties = new LinkedHashSet<UUID>();
        bookingsProperties.add( CustodyID );
        bookingsProperties.add( CustodyEventNumber );
        bookingsProperties.add( BookingCode );
        bookingsProperties.add( BookingDateTime );

        UUID bookingsType = edm.createEntityType( new EntityType(
                BOOKINGS_ENTITY_SET_TYPE,
                "Jail Custody Bookings",
                "Entity type for a jail booking",
                ImmutableSet.of(),
                bookingsKeys,
                bookingsProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( bookingsType == null ) {
            bookingsType = edm.getEntityTypeId(
                    BOOKINGS_ENTITY_SET_TYPE.getNamespace(), BOOKINGS_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> chargesKey = new LinkedHashSet<UUID>();
        chargesKey.add( ArrestCharge );
        chargesKey.add( ReleaseDateTime );

        LinkedHashSet<UUID> chargesProperties = new LinkedHashSet<UUID>();
        chargesProperties.add( ChargeSequence );
        chargesProperties.add( ReleaseCode );
        chargesProperties.add( ArrestCharge );
        chargesProperties.add( TopChargeDescription );
        chargesProperties.add( Docket );
        chargesProperties.add( CourtFile );
        chargesProperties.add( ViolationCount );
        chargesProperties.add( CrimeReportNumber );
        chargesProperties.add( ReleaseDateTime );


        UUID chargesType = edm.createEntityType( new EntityType(
                CHARGES_ENTITY_SET_TYPE,
                "Charges Type",
                "Entity type for a charge",
                ImmutableSet.of(),
                chargesKey,
                chargesProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( chargesType == null ) {
            chargesType = edm.getEntityTypeId(
                    CHARGES_ENTITY_SET_TYPE.getNamespace(), CHARGES_ENTITY_SET_TYPE.getName() );
        }

        // ASSOCIATIONS

        LinkedHashSet<UUID> wasBookedKey = new LinkedHashSet<UUID>();
        wasBookedKey.add( xref );

        LinkedHashSet<UUID> wasBookedProperties = new LinkedHashSet<UUID>();
        wasBookedProperties.add( xref );

        LinkedHashSet<UUID> wasBookedSource = new LinkedHashSet<UUID>();
        wasBookedSource.add( peopleType );
        LinkedHashSet<UUID> wasBookedDestination = new LinkedHashSet<UUID>();
        wasBookedDestination.add( bookingsType );

        UUID wasBookedType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        WAS_BOOKED_ENTITY_SET_TYPE,
                        "was booked into custody",
                        "person was booked into custody association type",
                        ImmutableSet.of(),
                        wasBookedKey,
                        wasBookedProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                wasBookedSource,
                wasBookedDestination,
                false
        ) );
        if ( wasBookedType == null ) {
            wasBookedType = edm.getEntityTypeId( WAS_BOOKED_ENTITY_SET_TYPE.getNamespace(),
                    WAS_BOOKED_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> chargeAppearsInBookingKey = new LinkedHashSet<UUID>();
        chargeAppearsInBookingKey.add( ArrestCharge );
        chargeAppearsInBookingKey.add( ReleaseDateTime );

        LinkedHashSet<UUID> chargeAppearsInBookingProperties = new LinkedHashSet<UUID>();
        chargeAppearsInBookingProperties.add( ArrestCharge );
        chargeAppearsInBookingProperties.add( ReleaseDateTime );


        LinkedHashSet<UUID> chargeAppearsInBookingSource = new LinkedHashSet<UUID>();
        chargeAppearsInBookingSource.add( chargesType );
        LinkedHashSet<UUID> chargeAppearsInBookingDestination = new LinkedHashSet<UUID>();
        chargeAppearsInBookingDestination.add( bookingsType );

        UUID chargeAppearsInBookingType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_TYPE,
                        "appears in",
                        "charge appears in booking event association type",
                        ImmutableSet.of(),
                        chargeAppearsInBookingKey,
                        chargeAppearsInBookingProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                chargeAppearsInBookingSource,
                chargeAppearsInBookingDestination,
                false
        ) );
        if ( chargeAppearsInBookingType == null ) {
            chargeAppearsInBookingType = edm.getEntityTypeId(
                    CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_TYPE.getNamespace(), CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_TYPE.getName() );
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
                bookingsType,
                BOOKINGS_ENTITY_SET_NAME,
                "Bookings",
                Optional.of( "Sacramento Jail Custody Bookings" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                chargesType,
                CHARGES_ENTITY_SET_NAME,
                "Charges",
                Optional.of( "Sacramento Booking Charges" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        // NEW association sets

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                wasBookedType,
                WAS_BOOKED_ENTITY_SET_NAME,
                "was booked",
                Optional.of( "---- person was booked in jail custody association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                chargeAppearsInBookingType,
                CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_NAME,
                "involved in",
                Optional.of( "---- charge involved in booking association ----" ),
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

                .addEntity( BOOKINGS_ALIAS )
                .ofType( BOOKINGS_ENTITY_SET_TYPE )
                .to( BOOKINGS_ENTITY_SET_NAME )
                .key( BOOKINGS_ENTITY_TYPE_KEY_1 )
                .useCurrentSync()

                .addProperty( CUSTODY_ID_FQN )
                .value( row -> row.getAs( "CustodyID" ) ).ok()
                .addProperty( CUSTODY_EVENT_NUMBER_FQN )
                .value( row -> row.getAs( "Event Number" ) ).ok()
                .addProperty( BOOKING_DATETIME_FQN )
                .value( row -> standardizeDate( row.getAs( "Booking Date Time" ) ) ).ok()
                .addProperty( BOOKING_CODE_FQN )
                .value( row -> row.getAs( "Booking Code" ) ).ok().ok()

                .addEntity( CHARGES_ALIAS )
                .ofType( CHARGES_ENTITY_SET_TYPE )
                .to( CHARGES_ENTITY_SET_NAME )
                .key( CHARGES_ENTITY_TYPE_KEY_1, CHARGES_ENTITY_TYPE_KEY_2 )
                .useCurrentSync()

                .addProperty( CHARGE_SEQUENCE_FQN )
                .value( row -> row.getAs( "Charge Sequence" ) ).ok()
                .addProperty( ARREST_CHARGE_FQN )
                .value( row -> row.getAs( "Arrest Charge" ) ).ok()
                .addProperty( RELEASE_CODE_FQN )
                .value( row -> row.getAs( "Release Code" ) ).ok()
                .addProperty( TOP_CHARGE_DESCRIPTION_FQN )
                .value( row -> row.getAs( "Top Charge Description" ) ).ok()
                .addProperty( DOCKET_FQN )
                .value( row -> row.getAs( "Docket" ) ).ok()
                .addProperty( COURT_FILE_FQN )
                .value( row -> row.getAs( "Court File" ) ).ok()
                .addProperty( VIOLATION_COUNT_FQN )
                .value( row -> row.getAs( "Violation Count" ) ).ok()
                .addProperty( CRIME_REPORT_NUMBER_FQN )
                .value( row -> row.getAs( "CrimeReportNumber" ) ).ok()
                .addProperty( RELEASE_DATETIME_FQN )
                .value( row -> standardizeDate( row.getAs( "Release Date Time" ) ) ).ok()

                .ok().ok()
                .createAssociations()

                .addAssociation( WAS_BOOKED_ENTITY_SET_ALIAS )
                .ofType( WAS_BOOKED_ENTITY_SET_TYPE )
                .to( WAS_BOOKED_ENTITY_SET_NAME )
                .key( WAS_BOOKED_ENTITY_SET_KEY_1 )
                .fromEntity( PEOPLE_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .useCurrentSync()

                .addProperty( XREF_ID_FQN )
                .value( row -> row.getAs( "XREF" ) ).ok().ok()

                .addAssociation( CHARGE_APPEARS_IN_BOOKING_ALIAS )
                .ofType( CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_TYPE )
                .to( CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_NAME )
                .key( CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_KEY_1, CHARGE_APPEARS_IN_BOOKING_ENTITY_SET_KEY_2 )
                .fromEntity( CHARGES_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .useCurrentSync()

                .addProperty( RELEASE_DATETIME_FQN )
                .value( row -> standardizeDate( row.getAs( "Release Date Time" ) ) ).ok()
                .addProperty( ARREST_CHARGE_FQN )
                .value( row -> row.getAs( "Arrest Charge" ) ).ok().ok()
                .ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
    public static String standardizeDate( Object myDate ) {
        if (myDate != null && !myDate.equals("")) {
            String d = myDate.toString();
            String ddate = d.split(" ")[0];
            String dtime = d.split(" ")[1];
            FormattedDateTime date = new FormattedDateTime( ddate, dtime, "MM/dd/yy", "HH:mm");
            return date.getDateTime();
        }
        return null;
    }


}
