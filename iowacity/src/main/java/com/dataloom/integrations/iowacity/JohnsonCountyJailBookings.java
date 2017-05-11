package com.dataloom.integrations.iowacity;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;

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
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.MissionControl;
import com.kryptnostic.shuttle.Shuttle;

import retrofit2.Retrofit;

public class JohnsonCountyJailBookings {
    // Logger is used to output useful debugging messages to the console
    private static final Logger     logger                                            = LoggerFactory
            .getLogger( JohnsonCountyJailBookings.class );

    // PROPERTIES
    public static FullQualifiedName JAIL_RECORD_XREF_FQN                              = new FullQualifiedName(
            "publicsafety.xref" );
    public static FullQualifiedName PERSON_XREF_FQN                                   = new FullQualifiedName(
            "publicsafety.xref" );
    public static FullQualifiedName OFFICER_XREF_FQN                                  = new FullQualifiedName(
            "publicsafety.xref" );

    public static FullQualifiedName JAIL_ID_FQN                                       = new FullQualifiedName(
            "publicsafety.JailID" );
    public static FullQualifiedName ACTUAL_NO_FQN                                     = new FullQualifiedName(
            "publicsafety.ActualNumber" );
    public static FullQualifiedName ARREST_NO_FQN                                     = new FullQualifiedName(
            "j.ArrestSequenceID" );
    public static FullQualifiedName FIRSTNAME_FQN                                     = new FullQualifiedName(
            "nc.PersonGivenName" );
    public static FullQualifiedName MIDDLENAME_FQN                                    = new FullQualifiedName(
            "nc.PersonMiddleName" );
    public static FullQualifiedName LASTNAME_FQN                                      = new FullQualifiedName(
            "nc.PersonSurName" );
    public static FullQualifiedName ALIAS_FQN                                         = new FullQualifiedName(
            "im.PersonNickName" );
    public static FullQualifiedName DATE_IN_FQN                                       = new FullQualifiedName(
            "publicsafety.datebooked2" );
    public static FullQualifiedName DATE_OUT_FQN                                      = new FullQualifiedName(
            "publicsafety.datereleased2" );
    public static FullQualifiedName OFFICER_ID_FQN                                    = new FullQualifiedName(
            "publicsafety.officerID" );
    public static FullQualifiedName DOB_FQN                                           = new FullQualifiedName(
            "nc.PersonBirthDate" );
    public static FullQualifiedName AGE_FQN                                           = new FullQualifiedName(
            "general.Age" );
    public static FullQualifiedName HOW_RELEASED_FQN                                  = new FullQualifiedName(
            "j.BookingRelease" );
    public static FullQualifiedName CAUTION_FQN                                       = new FullQualifiedName(
            "intel.SubjectCautionInformationDescriptionText" );
    public static FullQualifiedName EST_REL_DATE_FQN                                  = new FullQualifiedName(
            "j.IncarcerationProjectedReleaseDate" );
    public static FullQualifiedName CURRENCY_FQN                                      = new FullQualifiedName(
            "publicsafety.CurrencyAmount" );
    public static FullQualifiedName CHANGE_FQN                                        = new FullQualifiedName(
            "publicsafety.ChangeAmount" );
    public static FullQualifiedName CHECKS_FQN                                        = new FullQualifiedName(
            "publicsafety.ChecksAmount" );
    public static FullQualifiedName CALL_ATTORNEY_FQN                                 = new FullQualifiedName(
            "j.BookingTelephoneCall" );
    public static FullQualifiedName RELEASED_TO_FQN                                   = new FullQualifiedName(
            "j.ReleaseToFacility" );
    public static FullQualifiedName REMARKS_FQN                                       = new FullQualifiedName(
            "j.Remark" );
    public static FullQualifiedName COMIT_AUTH_FQN                                    = new FullQualifiedName(
            "j.CommittedToAuthorityText" );
    public static FullQualifiedName TIME_FRAME_FQN                                    = new FullQualifiedName(
            "publicsafety.TimeFrame" );
    public static FullQualifiedName TOTAL_TIME_FQN                                    = new FullQualifiedName(
            "publicsafety.TotalTime" );
    public static FullQualifiedName HELD_AT_FQN                                       = new FullQualifiedName(
            "publicsafety.HeldAt" );
    public static FullQualifiedName ADULT_JUV_WAIVE_FQN                               = new FullQualifiedName(
            "scr.TreatAsAdultIndicator" );
    public static FullQualifiedName JUV_HOLD_AUTH_FQN                                 = new FullQualifiedName(
            "publicsafety.JuvHoldAuth" );
    public static FullQualifiedName PERSON_POST_BAIL_FQN                              = new FullQualifiedName(
            "j.BailingPerson" );
    public static FullQualifiedName ARREST_AGENCY_FQN                                 = new FullQualifiedName(
            "j.ArrestAgency" );
    public static FullQualifiedName BOND_COURT_DATETIME_FQN                           = new FullQualifiedName(
            "j.BailHearingDate" );
    public static FullQualifiedName SEX_FQN                                           = new FullQualifiedName(
            "nc.PersonSex" );
    public static FullQualifiedName RACE_FQN                                          = new FullQualifiedName(
            "nc.PersonRace" );
    public static FullQualifiedName SSA_FQN                                           = new FullQualifiedName(
            "publicsafety.SSA" );
    public static FullQualifiedName SSA_CONVICTION_FQN                                = new FullQualifiedName(
            "publicsafety.SSAConviction" );
    public static FullQualifiedName SSA_STATUS_FQN                                    = new FullQualifiedName(
            "publicsafety.SSAStatus" );
    public static FullQualifiedName ORI_FQN                                           = new FullQualifiedName(
            "publicsafety.ORI" );
    public static FullQualifiedName DIS_SUBMIT_FQN                                    = new FullQualifiedName(
            "publicsafety.DISSubmit" );
    public static FullQualifiedName BALANCE_FQN                                       = new FullQualifiedName(
            "publicsafety.Balance" );
    public static FullQualifiedName STATUS_FQN                                        = new FullQualifiedName(
            "publicsafety.Status" );
    public static FullQualifiedName SID_ST_FQN                                        = new FullQualifiedName(
            "publicsafety.SIDSt" );
    public static FullQualifiedName SID_NO_FQN                                        = new FullQualifiedName(
            "publicsafety.SIDNo" );
    public static FullQualifiedName REASON_CODE_FQN                                   = new FullQualifiedName(
            "scr.DetentionReleaseReasonCategoryCodeType" );
    public static FullQualifiedName ARREST_DATE_FQN                                   = new FullQualifiedName(
            "publicsafety.arrestdate" );
    public static FullQualifiedName CASE_ID_FQN                                       = new FullQualifiedName(
            "j.CaseNumberText" );
    public static FullQualifiedName INCLUDE_FQN                                       = new FullQualifiedName(
            "publicsafety.include" );
    public static FullQualifiedName HEIGHT_FQN                                        = new FullQualifiedName(
            "nc.PersonHeightMeasure" );
    public static FullQualifiedName WEIGHT_FQN                                        = new FullQualifiedName(
            "nc.PersonWeightMeasure" );
    public static FullQualifiedName EYES_FQN                                          = new FullQualifiedName(
            "nc.PersonEyeColorText" );
    public static FullQualifiedName HAIR_FQN                                          = new FullQualifiedName(
            "nc.PersonHairColorText" );
    public static FullQualifiedName TRANSP_AGENCY_FQN                                 = new FullQualifiedName(
            "j.EnforcementOfficialUnit" );
    public static FullQualifiedName BOOKED_ID_FQN                                     = new FullQualifiedName(
            "publicsafety.CustodyID" );
    public static FullQualifiedName PBT_FQN                                           = new FullQualifiedName(
            "publicsafety.PortableBreathTest" );
    public static FullQualifiedName INTOX_FQN                                         = new FullQualifiedName(
            "j.IntoxicationLevelText" );
    public static FullQualifiedName RELEASE_NOTES_FQN                                 = new FullQualifiedName(
            "publicsafety.ReleaseNotes" );
    public static FullQualifiedName ARREST_ID_LONG_FQN                                = new FullQualifiedName(
            "publicsafety.ArrestID" );
    public static FullQualifiedName RELEASE_OFFICER_USERNAME_FQN                      = new FullQualifiedName(
            "nc.SystemUserName" );

    public static FullQualifiedName MUGSHOT_FQN                                       = new FullQualifiedName(
            "publicsafety.mugshot" );
    public static FullQualifiedName OFFICER_CATEGORY_FQN                              = new FullQualifiedName(
            "j.EnforcementOfficialCategoryText" );

    // ENTITIES
    public static String            SUBJECTS_ENTITY_SET_NAME                          = "jcsubjects2";
    public static FullQualifiedName SUBJECTS_ENTITY_SET_TYPE                          = new FullQualifiedName(
            "nc.PersonType2" );
    public static FullQualifiedName SUBJECTS_ENTITY_SET_KEY_1                         = PERSON_XREF_FQN;
    public static String            SUBJECTS_ALIAS                                    = "subjects";

    public static String            BOOKINGS_ENTITY_SET_NAME                          = "jcjailbookings2";
    public static FullQualifiedName BOOKINGS_ENTITY_SET_TYPE                          = new FullQualifiedName(
            "jciowa.JailBookingType2" );
    public static FullQualifiedName BOOKINGS_ENTITY_TYPE_KEY_1                        = JAIL_ID_FQN;
    public static String            BOOKINGS_ALIAS                                    = "bookings";

    public static String            JAIL_RECORDS_ENTITY_SET_NAME                      = "jcjailrecords2";
    public static FullQualifiedName JAIL_RECORDS_ENTITY_SET_TYPE                      = new FullQualifiedName(
            "jciowa.JailRecordType2" );
    public static FullQualifiedName JAIL_RECORDS_ENTITY_TYPE_KEY_1                    = JAIL_RECORD_XREF_FQN;
    public static String            JAIL_RECORDS_ALIAS                                = "jailrecords";

    public static String            OFFICERS_ENTITY_SET_NAME                          = "jcofficers2";
    public static FullQualifiedName OFFICERS_ENTITY_SET_TYPE                          = new FullQualifiedName(
            "j.EnforcementOfficialType2" );
    public static FullQualifiedName OFFICERS_ENTITY_TYPE_KEY_1                        = OFFICER_XREF_FQN;
    public static String            OFFICERS_ALIAS                                    = "officers";

    // ASSOCIATIONS
    public static String            WAS_BOOKED_IN_ENTITY_SET_NAME                     = "jcwasbookedin2";
    public static FullQualifiedName WAS_BOOKED_IN_ENTITY_SET_TYPE                     = new FullQualifiedName(
            "jciowa.PersonJailBookingAssociation2" );
    public static FullQualifiedName WAS_BOOKED_ENTITY_SET_KEY_1                       = PERSON_XREF_FQN;
    public static String            WAS_BOOKED_IN_ENTITY_SET_ALIAS                    = "personwasbookedin";

    public static String            DETAILS_OF_IN_ENTITY_SET_NAME                     = "jcdetailsof2";
    public static FullQualifiedName DETAILS_OF_IN_ENTITY_SET_TYPE                     = new FullQualifiedName(
            "jciowa.DetailsOfAssociation2" );
    public static FullQualifiedName DETAILS_OF_ENTITY_SET_KEY_1                       = JAIL_RECORD_XREF_FQN;
    public static String            DETAILS_OF_IN_ENTITY_SET_ALIAS                    = "detailsof";

    public static String            SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME   = "jcinteractedwith2";
    public static FullQualifiedName SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.OfficerInteractedWithSubject2" );
    public static FullQualifiedName SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 = PERSON_XREF_FQN;
    // public static String SUBJECT_INTERACTED_WITH_OFFICER_ALIAS = "jcinteractedwith";

    public static void main( String[] args ) throws InterruptedException {
        String path = new File(
                JohnsonCountyJailBookings.class.getClassLoader().getResource( "Jail_Record_Formatted.csv" )
                        .getPath() )
                                .getAbsolutePath();

        // final String username = "replace me with email username";
        // final String password = "replace me with password";
        final SparkSession sparkSession = MissionControl.getSparkSession();
        // final String jwtToken = MissionControl.getIdToken( username, password );
        final String jwtToken = "[JWT TOKEN GOES HERE]";
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        /*
         * CREATE EDM OBJECTS
         */

        /*
         * PROPERTY TYPES
         */
        Retrofit retrofit = RetrofitFactory.newClient( Environment.PRODUCTION, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        UUID Jail_Record_XREF = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        JAIL_RECORD_XREF_FQN,
                        "Identifier",
                        Optional.of( "Identifier" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Jail_Record_XREF == null ) {
            Jail_Record_XREF = edm
                    .getPropertyTypeId( JAIL_RECORD_XREF_FQN.getNamespace(), JAIL_RECORD_XREF_FQN.getName() );
        }

        UUID Person_XREF = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        PERSON_XREF_FQN,
                        "Identifier",
                        Optional.of( "Identifier" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Person_XREF == null ) {
            Person_XREF = edm.getPropertyTypeId( PERSON_XREF_FQN.getNamespace(), PERSON_XREF_FQN.getName() );
        }

        UUID Officer_XREF = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        OFFICER_XREF_FQN,
                        "Identifier",
                        Optional.of( "Identifier" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Officer_XREF == null ) {
            Officer_XREF = edm.getPropertyTypeId( OFFICER_XREF_FQN.getNamespace(), OFFICER_XREF_FQN.getName() );
        }

        UUID Jail_ID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        JAIL_ID_FQN,
                        "Jail ID",
                        Optional.of( "Identifier for a jail." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Jail_ID == null ) {
            Jail_ID = edm.getPropertyTypeId( JAIL_ID_FQN.getNamespace(), JAIL_ID_FQN.getName() );
        }

        UUID Actual_No = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ACTUAL_NO_FQN,
                        "Actual Number",
                        Optional.of( "Actual Number" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Actual_No == null ) {
            Actual_No = edm.getPropertyTypeId( ACTUAL_NO_FQN.getNamespace(), ACTUAL_NO_FQN.getName() );
        }

        UUID Arrest_No = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ARREST_NO_FQN,
                        "Arrest Number",
                        Optional.of( "A sequential identifier number assigned to the arrest of a subject." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Arrest_No == null ) {
            Arrest_No = edm.getPropertyTypeId( ARREST_NO_FQN.getNamespace(), ARREST_NO_FQN.getName() );
        }

        UUID First_Name = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        FIRSTNAME_FQN,
                        "Person Given Name",
                        Optional.of( "A first name of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( First_Name == null ) {
            First_Name = edm.getPropertyTypeId( FIRSTNAME_FQN.getNamespace(), FIRSTNAME_FQN.getName() );
        }

        UUID Middle_Name = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        MIDDLENAME_FQN,
                        "Person Middle Name",
                        Optional.of( "A middle name of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Middle_Name == null ) {
            Middle_Name = edm.getPropertyTypeId( MIDDLENAME_FQN.getNamespace(), MIDDLENAME_FQN.getName() );
        }

        UUID Sur_Name = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        LASTNAME_FQN,
                        "Person Sur Name",
                        Optional.of( "An original last name or surname of a person before changed by marriage." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Sur_Name == null ) {
            Sur_Name = edm.getPropertyTypeId( LASTNAME_FQN.getNamespace(), LASTNAME_FQN.getName() );
        }

        UUID Alias_Name = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ALIAS_FQN,
                        "Person Nickname",
                        Optional.of( "A nickname or street name of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.METAPHONE ) ) );
        if ( Alias_Name == null ) {
            Alias_Name = edm.getPropertyTypeId( ALIAS_FQN.getNamespace(), ALIAS_FQN.getName() );
        }

        UUID Date_In = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        DATE_IN_FQN,
                        "Date Booked",
                        Optional.of( "Date subject was booked." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Date_In == null ) {
            Date_In = edm.getPropertyTypeId( DATE_IN_FQN.getNamespace(), DATE_IN_FQN.getName() );
        }

        UUID Date_Out = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        DATE_OUT_FQN,
                        "Date Released",
                        Optional.of( "Date subject was released." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Date_Out == null ) {
            Date_Out = edm.getPropertyTypeId( DATE_OUT_FQN.getNamespace(), DATE_OUT_FQN.getName() );
        }

        UUID Officer_ID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        OFFICER_ID_FQN,
                        "Officer ID",
                        Optional.of( "Identifier for an officer." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Officer_ID == null ) {
            Officer_ID = edm.getPropertyTypeId( DATE_OUT_FQN.getNamespace(), DATE_OUT_FQN.getName() );
        }

        UUID Dob = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        DOB_FQN,
                        "Person Birth Date",
                        Optional.of( "A date a person was born." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Dob == null ) {
            Dob = edm.getPropertyTypeId( DOB_FQN.getNamespace(), DOB_FQN.getName() );
        }

        // UUID Age = edm
        // .createPropertyType( new PropertyType(
        // Optional.absent(),
        // AGE_FQN,
        // "Person Age",
        // Optional.of( "Age of a person." ),
        // ImmutableSet.of(),
        // EdmPrimitiveTypeKind.String,
        // Optional.of( true ),
        // Optional.of( Analyzer.STANDARD ) ) );
        // if ( Age == null ) {
        // Age = edm.getPropertyTypeId( AGE_FQN.getNamespace(), AGE_FQN.getName() );
        // }

        UUID How_Released = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HOW_RELEASED_FQN,
                        "How Released",
                        Optional.of( "A release of a subject from booking or from booking detention." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( How_Released == null ) {
            How_Released = edm.getPropertyTypeId( HOW_RELEASED_FQN.getNamespace(), HOW_RELEASED_FQN.getName() );
        }

        UUID Caution = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CAUTION_FQN,
                        "Caution",
                        Optional.of(
                                "A description of cautions about a persons potential for dangerous behavior when contacted." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Caution == null ) {
            Caution = edm.getPropertyTypeId( CAUTION_FQN.getNamespace(), CAUTION_FQN.getName() );
        }

        UUID Est_Rel_Date = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        EST_REL_DATE_FQN,
                        "Projected Release Date",
                        Optional.of(
                                "A date a subject is anticipated to complete service of final sentence, automatically or manually calculated, based on current sentence information." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Est_Rel_Date == null ) {
            Est_Rel_Date = edm.getPropertyTypeId( EST_REL_DATE_FQN.getNamespace(), EST_REL_DATE_FQN.getName() );
        }

        UUID Currency = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CURRENCY_FQN,
                        "Currency Amount",
                        Optional.of( "Amount in currency siezed at time of booking." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Currency == null ) {
            Currency = edm.getPropertyTypeId( EST_REL_DATE_FQN.getNamespace(), EST_REL_DATE_FQN.getName() );
        }

        UUID Change = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHANGE_FQN,
                        "Change Amount",
                        Optional.of( "Amount in change siezed at time of booking." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Change == null ) {
            Change = edm.getPropertyTypeId( CHANGE_FQN.getNamespace(), CHANGE_FQN.getName() );
        }

        UUID Checks = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHECKS_FQN,
                        "Checks Amount",
                        Optional.of( "Amount in checks siezed at time of booking." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Checks == null ) {
            Checks = edm.getPropertyTypeId( CHECKS_FQN.getNamespace(), CHECKS_FQN.getName() );
        }

        UUID Call_Attorney = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CALL_ATTORNEY_FQN,
                        "Call Attorney",
                        Optional.of( "True if subject called attorney." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Call_Attorney == null ) {
            Call_Attorney = edm.getPropertyTypeId( CALL_ATTORNEY_FQN.getNamespace(), CALL_ATTORNEY_FQN.getName() );
        }

        UUID Released_To = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RELEASED_TO_FQN,
                        "Released To Facility",
                        Optional.of( "An institution from which the subject is to be released." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Released_To == null ) {
            Released_To = edm.getPropertyTypeId( RELEASED_TO_FQN.getNamespace(), RELEASED_TO_FQN.getName() );
        }

        UUID Remarks = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        REMARKS_FQN,
                        "Remark",
                        Optional.of( "An informal comment or observation." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Remarks == null ) {
            Remarks = edm.getPropertyTypeId( REMARKS_FQN.getNamespace(), REMARKS_FQN.getName() );
        }

        UUID Comit_Auth = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        COMIT_AUTH_FQN,
                        "Committed To Authority",
                        Optional.of(
                                "An authority to which a person is remanded into custody as a part of a judgment." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Comit_Auth == null ) {
            Comit_Auth = edm.getPropertyTypeId( COMIT_AUTH_FQN.getNamespace(), COMIT_AUTH_FQN.getName() );
        }

        UUID Time_Frame = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TIME_FRAME_FQN,
                        "Time Frame",
                        Optional.of( "Time Frame" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Time_Frame == null ) {
            Time_Frame = edm.getPropertyTypeId( TIME_FRAME_FQN.getNamespace(), TIME_FRAME_FQN.getName() );
        }

        UUID Total_Time = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TOTAL_TIME_FQN,
                        "Total Time",
                        Optional.of( "Total Time" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Total_Time == null ) {
            Total_Time = edm.getPropertyTypeId( TOTAL_TIME_FQN.getNamespace(), TOTAL_TIME_FQN.getName() );
        }

        UUID Held_At = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HELD_AT_FQN,
                        "Held At",
                        Optional.of( "Authority subject is held at." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Held_At == null ) {
            Held_At = edm.getPropertyTypeId( HELD_AT_FQN.getNamespace(), HELD_AT_FQN.getName() );
        }

        UUID Adult_Juv_Waive = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ADULT_JUV_WAIVE_FQN,
                        "Adult Juv Waive",
                        Optional.of(
                                "Juvenile individual should be treated as an adult in this specific enforcement encounter." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Adult_Juv_Waive == null ) {
            Adult_Juv_Waive = edm
                    .getPropertyTypeId( ADULT_JUV_WAIVE_FQN.getNamespace(), ADULT_JUV_WAIVE_FQN.getName() );
        }

        UUID Juv_Hold_Auth = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        JUV_HOLD_AUTH_FQN,
                        "Juv Hold Auth",
                        Optional.of( "Juv Hold Auth" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Juv_Hold_Auth == null ) {
            Juv_Hold_Auth = edm.getPropertyTypeId( JUV_HOLD_AUTH_FQN.getNamespace(), JUV_HOLD_AUTH_FQN.getName() );
        }

        UUID Person_Post_Bail = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        PERSON_POST_BAIL_FQN,
                        "Person Post Bail",
                        Optional.of( "A person who posted bond for another person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Person_Post_Bail == null ) {
            Person_Post_Bail = edm
                    .getPropertyTypeId( PERSON_POST_BAIL_FQN.getNamespace(), PERSON_POST_BAIL_FQN.getName() );
        }

        UUID Arrest_Agency = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ARREST_AGENCY_FQN,
                        "Arrest Agency",
                        Optional.of( "An agency which employs the arresting official." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Arrest_Agency == null ) {
            Arrest_Agency = edm.getPropertyTypeId( ARREST_AGENCY_FQN.getNamespace(), ARREST_AGENCY_FQN.getName() );
        }

        UUID Bond_Court_Datetime = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOND_COURT_DATETIME_FQN,
                        "Bond Court Datetime",
                        Optional.of( "A date and time of a court hearing to determine the bail to be set." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Bond_Court_Datetime == null ) {
            Bond_Court_Datetime = edm
                    .getPropertyTypeId( BOND_COURT_DATETIME_FQN.getNamespace(), BOND_COURT_DATETIME_FQN.getName() );
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

        UUID ssa = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SSA_FQN,
                        "SSA",
                        Optional.of( "SSA" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ssa == null ) {
            ssa = edm.getPropertyTypeId( SSA_FQN.getNamespace(), SSA_FQN.getName() );
        }

        UUID ssa_Conviction = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SSA_CONVICTION_FQN,
                        "SSA Conviction",
                        Optional.of( "SSA Conviction" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ssa_Conviction == null ) {
            ssa_Conviction = edm.getPropertyTypeId( SSA_CONVICTION_FQN.getNamespace(), SSA_CONVICTION_FQN.getName() );
        }

        UUID ssa_Status = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SSA_STATUS_FQN,
                        "SSA Status",
                        Optional.of( "SSA Status" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ssa_Status == null ) {
            ssa_Status = edm.getPropertyTypeId( SSA_STATUS_FQN.getNamespace(), SSA_STATUS_FQN.getName() );
        }

        UUID ori = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ORI_FQN,
                        "ORI",
                        Optional.of( "ORI" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ori == null ) {
            ori = edm.getPropertyTypeId( ORI_FQN.getNamespace(), ORI_FQN.getName() );
        }

        UUID DIS_Submit = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        DIS_SUBMIT_FQN,
                        "DIS Submit",
                        Optional.of( "DIS Submit" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( DIS_Submit == null ) {
            DIS_Submit = edm.getPropertyTypeId( DIS_SUBMIT_FQN.getNamespace(), DIS_SUBMIT_FQN.getName() );
        }

        UUID Status = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        STATUS_FQN,
                        "Status",
                        Optional.of( "Status" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Status == null ) {
            Status = edm.getPropertyTypeId( STATUS_FQN.getNamespace(), STATUS_FQN.getName() );
        }

        UUID SID_St = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SID_ST_FQN,
                        "SID St",
                        Optional.of( "SID St" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( SID_St == null ) {
            SID_St = edm.getPropertyTypeId( SID_ST_FQN.getNamespace(), SID_ST_FQN.getName() );
        }

        UUID SID_No = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SID_NO_FQN,
                        "SID No",
                        Optional.of( "SID No" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( SID_No == null ) {
            SID_No = edm.getPropertyTypeId( SID_NO_FQN.getNamespace(), SID_NO_FQN.getName() );
        }

        UUID Reason_Code = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        REASON_CODE_FQN,
                        "Reason Code",
                        Optional.of( "A kind of detention release reason." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Reason_Code == null ) {
            Reason_Code = edm.getPropertyTypeId( REASON_CODE_FQN.getNamespace(), REASON_CODE_FQN.getName() );
        }

        UUID Arrest_Date = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ARREST_DATE_FQN,
                        "Arrest Date",
                        Optional.of( "Date of arrest" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Arrest_Date == null ) {
            Arrest_Date = edm.getPropertyTypeId( ARREST_DATE_FQN.getNamespace(), ARREST_DATE_FQN.getName() );
        }

        UUID Case_ID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CASE_ID_FQN,
                        "Case ID",
                        Optional.of(
                                "An identifying number for a case that this activity is a part of, where the case number belongs to the agency that owns the activity information." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Case_ID == null ) {
            Case_ID = edm.getPropertyTypeId( CASE_ID_FQN.getNamespace(), CASE_ID_FQN.getName() );
        }

        UUID Include = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        INCLUDE_FQN,
                        "Include",
                        Optional.of( "Include" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Include == null ) {
            Include = edm.getPropertyTypeId( INCLUDE_FQN.getNamespace(), INCLUDE_FQN.getName() );
        }

        UUID Height = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HEIGHT_FQN,
                        "Person Height Measure",
                        Optional.of( "A measurement of the height of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
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
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Weight == null ) {
            Weight = edm.getPropertyTypeId( WEIGHT_FQN.getNamespace(), WEIGHT_FQN.getName() );
        }

        UUID Hair = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        HAIR_FQN,
                        "Person Hair Color",
                        Optional.of( "A data concept for a color of the hair of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Hair == null ) {
            Hair = edm.getPropertyTypeId( HAIR_FQN.getNamespace(), HAIR_FQN.getName() );
        }

        UUID Eyes = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        EYES_FQN,
                        "Person Eye Color",
                        Optional.of( "A data concept for a color of the eyes of a person." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Eyes == null ) {
            Eyes = edm.getPropertyTypeId( EYES_FQN.getNamespace(), EYES_FQN.getName() );
        }

        UUID Transp_Agency = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TRANSP_AGENCY_FQN,
                        "Agency",
                        Optional.of( "An enforcement unit to which an enforcement officer is assigned." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Transp_Agency == null ) {
            Transp_Agency = edm.getPropertyTypeId( TRANSP_AGENCY_FQN.getNamespace(), TRANSP_AGENCY_FQN.getName() );
        }

        UUID Booked_ID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOOKED_ID_FQN,
                        "Custody ID",
                        Optional.of( "Custody ID." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Booked_ID == null ) {
            Booked_ID = edm.getPropertyTypeId( BOOKED_ID_FQN.getNamespace(), BOOKED_ID_FQN.getName() );
        }

        UUID pbt = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        PBT_FQN,
                        "PBT",
                        Optional.of( "PBT." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( pbt == null ) {
            pbt = edm.getPropertyTypeId( PBT_FQN.getNamespace(), PBT_FQN.getName() );
        }

        UUID Intox = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        INTOX_FQN,
                        "Intox",
                        Optional.of( "Intox." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Intox == null ) {
            Intox = edm.getPropertyTypeId( INTOX_FQN.getNamespace(), INTOX_FQN.getName() );
        }

        UUID Release_Notes = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RELEASE_NOTES_FQN,
                        "Release Notes",
                        Optional.of( "Release Notes." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Release_Notes == null ) {
            Release_Notes = edm.getPropertyTypeId( RELEASE_NOTES_FQN.getNamespace(), RELEASE_NOTES_FQN.getName() );
        }

        UUID Arrest_ID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ARREST_ID_LONG_FQN,
                        "Arrest ID",
                        Optional.of( "Arrest ID." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Arrest_ID == null ) {
            Arrest_ID = edm.getPropertyTypeId( ARREST_ID_LONG_FQN.getNamespace(), ARREST_ID_LONG_FQN.getName() );
        }

        UUID Release_Officer_Username = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        RELEASE_OFFICER_USERNAME_FQN,
                        "Release Officer Username",
                        Optional.of( "Release Officer Username." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Release_Officer_Username == null ) {
            Release_Officer_Username = edm.getPropertyTypeId( RELEASE_OFFICER_USERNAME_FQN.getNamespace(),
                    RELEASE_OFFICER_USERNAME_FQN.getName() );
        }

        UUID MugShot = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        MUGSHOT_FQN,
                        "Mug Shot",
                        Optional.of( "An image for a subject." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Binary,
                        Optional.of( true ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( MugShot == null ) {
            MugShot = edm.getPropertyTypeId( MUGSHOT_FQN.getNamespace(), MUGSHOT_FQN.getName() );
        }

        UUID Officer_Category = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        OFFICER_CATEGORY_FQN,
                        "Officer Category",
                        Optional.of( "A kind of enforcement official." ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Officer_Category == null ) {
            Officer_Category = edm
                    .getPropertyTypeId( OFFICER_CATEGORY_FQN.getNamespace(), OFFICER_CATEGORY_FQN.getName() );
        }

        UUID Balance = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BALANCE_FQN,
                        "Balance",
                        Optional.of( "Balance" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Double,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Balance == null ) {
            Balance = edm.getPropertyTypeId( BALANCE_FQN.getNamespace(), BALANCE_FQN.getName() );
        }

        /*
         * ENTITY TYPES
         */

        // ENTITY TYPES

        LinkedHashSet<UUID> peopleKey = new LinkedHashSet<UUID>();
        peopleKey.add( Person_XREF );

        LinkedHashSet<UUID> peopleProperties = new LinkedHashSet<UUID>();
        peopleProperties.add( Person_XREF );
        peopleProperties.add( First_Name );
        peopleProperties.add( Middle_Name );
        peopleProperties.add( Sur_Name );
        peopleProperties.add( Alias_Name );
        peopleProperties.add( Sex );
        peopleProperties.add( Race );
        peopleProperties.add( Height );
        peopleProperties.add( Weight );
        peopleProperties.add( Hair );
        peopleProperties.add( Eyes );
        peopleProperties.add( Dob );
        peopleProperties.add( MugShot );

        UUID peopleType = edm.createEntityType( new EntityType(
                SUBJECTS_ENTITY_SET_TYPE,
                "Person Type (with Nickname)",
                "Entity type for a person (with Nickname)",
                ImmutableSet.of(),
                peopleKey,
                peopleProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( peopleType == null ) {
            peopleType = edm.getEntityTypeId(
                    SUBJECTS_ENTITY_SET_TYPE.getNamespace(), SUBJECTS_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> jailBookingKey = new LinkedHashSet<UUID>();
        jailBookingKey.add( Jail_ID );

        LinkedHashSet<UUID> jailBookingProperties = new LinkedHashSet<UUID>();
        jailBookingProperties.add( Jail_Record_XREF );
        jailBookingProperties.add( Jail_ID );
        jailBookingProperties.add( Actual_No );
        jailBookingProperties.add( Arrest_No );
        jailBookingProperties.add( Date_In );
        jailBookingProperties.add( Date_Out );
        jailBookingProperties.add( How_Released );
        jailBookingProperties.add( Est_Rel_Date );
        jailBookingProperties.add( Arrest_Agency );
        jailBookingProperties.add( Reason_Code );
        jailBookingProperties.add( Arrest_Date );
        jailBookingProperties.add( Booked_ID );

        UUID jailBookingType = edm.createEntityType( new EntityType(
                BOOKINGS_ENTITY_SET_TYPE,
                "Jail Booking",
                "Entity type for a jail booking",
                ImmutableSet.of(),
                jailBookingKey,
                jailBookingProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( jailBookingType == null ) {
            jailBookingType = edm.getEntityTypeId(
                    BOOKINGS_ENTITY_SET_TYPE.getNamespace(), BOOKINGS_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> jailRecordKey = new LinkedHashSet<UUID>();
        jailRecordKey.add( Jail_Record_XREF );

        LinkedHashSet<UUID> jailRecordProperties = new LinkedHashSet<UUID>();
        jailRecordProperties.add( Jail_Record_XREF );
        jailRecordProperties.add( Caution );
        jailRecordProperties.add( Currency );
        jailRecordProperties.add( Change );
        jailRecordProperties.add( Checks );
        jailRecordProperties.add( Call_Attorney );
        jailRecordProperties.add( Released_To );
        jailRecordProperties.add( Remarks );
        jailRecordProperties.add( Comit_Auth );
        jailRecordProperties.add( Time_Frame );
        jailRecordProperties.add( Total_Time );
        jailRecordProperties.add( Held_At );
        jailRecordProperties.add( Adult_Juv_Waive );
        jailRecordProperties.add( Juv_Hold_Auth );
        jailRecordProperties.add( Person_Post_Bail );
        jailRecordProperties.add( Include );
        jailRecordProperties.add( Release_Notes );
        jailRecordProperties.add( Balance );
        jailRecordProperties.add( Arrest_ID );

        UUID jailRecordType = edm.createEntityType( new EntityType(
                JAIL_RECORDS_ENTITY_SET_TYPE,
                "Jail Record",
                "Entity type for a jail record",
                ImmutableSet.of(),
                jailRecordKey,
                jailRecordProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( jailRecordType == null ) {
            jailRecordType = edm.getEntityTypeId(
                    JAIL_RECORDS_ENTITY_SET_TYPE.getNamespace(), JAIL_RECORDS_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> officerKey = new LinkedHashSet<UUID>();
        officerKey.add( Officer_XREF );

        LinkedHashSet<UUID> officerProperties = new LinkedHashSet<UUID>();
        officerProperties.add( Officer_XREF );
        officerProperties.add( Officer_ID );
        officerProperties.add( First_Name );
        officerProperties.add( Sur_Name );
        officerProperties.add( Officer_Category );
        officerProperties.add( Transp_Agency );
        officerProperties.add( Release_Officer_Username );

        UUID officerType = edm.createEntityType( new EntityType(
                OFFICERS_ENTITY_SET_TYPE,
                "Officer Type",
                "Entity type for an officer",
                ImmutableSet.of(),
                officerKey,
                officerProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( officerType == null ) {
            officerType = edm.getEntityTypeId(
                    OFFICERS_ENTITY_SET_TYPE.getNamespace(), OFFICERS_ENTITY_SET_TYPE.getName() );
        }

        // ASSOCIATIONS

        LinkedHashSet<UUID> detailsOfKey = new LinkedHashSet<UUID>();
        detailsOfKey.add( Person_XREF );

        LinkedHashSet<UUID> detailsOfProperties = new LinkedHashSet<UUID>();
        detailsOfProperties.add( Person_XREF );

        LinkedHashSet<UUID> detailsOfSource = new LinkedHashSet<UUID>();
        detailsOfSource.add( jailRecordType );
        LinkedHashSet<UUID> detailsOfDestination = new LinkedHashSet<UUID>();
        detailsOfDestination.add( jailBookingType );

        UUID detailsOfType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        DETAILS_OF_IN_ENTITY_SET_TYPE,
                        "details of",
                        "jail record details of booking association type",
                        ImmutableSet.of(),
                        detailsOfKey,
                        detailsOfProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                detailsOfSource,
                detailsOfDestination,
                false ) );
        if ( detailsOfType == null ) {
            detailsOfType = edm.getEntityTypeId(
                    DETAILS_OF_IN_ENTITY_SET_TYPE.getNamespace(), DETAILS_OF_IN_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> wasbookedKey = new LinkedHashSet<UUID>();
        wasbookedKey.add( Person_XREF );

        LinkedHashSet<UUID> wasbookedProperties = new LinkedHashSet<UUID>();
        wasbookedProperties.add( Person_XREF );

        LinkedHashSet<UUID> wasbookedSource = new LinkedHashSet<UUID>();
        wasbookedSource.add( peopleType );
        LinkedHashSet<UUID> wasbookedDestination = new LinkedHashSet<UUID>();
        wasbookedDestination.add( jailBookingType );

        UUID wasbookedType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        WAS_BOOKED_IN_ENTITY_SET_TYPE,
                        "was booked",
                        "person was booked in jail record association type",
                        ImmutableSet.of(),
                        wasbookedKey,
                        wasbookedProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                wasbookedSource,
                wasbookedDestination,
                false ) );
        if ( wasbookedType == null ) {
            wasbookedType = edm.getEntityTypeId(
                    WAS_BOOKED_IN_ENTITY_SET_TYPE.getNamespace(), WAS_BOOKED_IN_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> interactedWithKey = new LinkedHashSet<UUID>();
        interactedWithKey.add( Person_XREF );

        LinkedHashSet<UUID> interactedWithProperties = new LinkedHashSet<UUID>();
        interactedWithProperties.add( Person_XREF );

        LinkedHashSet<UUID> interactedWithSource = new LinkedHashSet<UUID>();
        interactedWithSource.add( peopleType );
        LinkedHashSet<UUID> interactedWithDestination = new LinkedHashSet<UUID>();
        interactedWithDestination.add( officerType );

        UUID interactedWithType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE,
                        "interacted with",
                        "jail subject interacted with officer association type",
                        ImmutableSet.of(),
                        interactedWithKey,
                        interactedWithProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                interactedWithSource,
                interactedWithDestination,
                true ) );
        if ( interactedWithType == null ) {
            interactedWithType = edm.getEntityTypeId(
                    SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE.getNamespace(),
                    SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE.getName() );
        }

        /*
         * ENTITY SETS
         */

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                peopleType,
                SUBJECTS_ENTITY_SET_NAME,
                "JC Iowa Jail Subjects",
                Optional.of( "Johnson County Iowa Jail Subjects" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                jailBookingType,
                BOOKINGS_ENTITY_SET_NAME,
                "JC Iowa Jail Bookings",
                Optional.of( "Johnson County Iowa Jail Bookings" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                officerType,
                OFFICERS_ENTITY_SET_NAME,
                "JC Iowa Officers",
                Optional.of( "Johnson County Iowa Officers" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                wasbookedType,
                WAS_BOOKED_IN_ENTITY_SET_NAME,
                "was booked in",
                Optional.of( "---- person was booked in jail association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                interactedWithType,
                SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME,
                "interacted with",
                Optional.of( "---- officer interacted with jail subject association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                jailRecordType,
                JAIL_RECORDS_ENTITY_SET_NAME,
                "JC Iowa Jail Records",
                Optional.of( "Johnson County Jail Records" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                detailsOfType,
                DETAILS_OF_IN_ENTITY_SET_NAME,
                "are details of",
                Optional.of( "---- jail records are details of bookings association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( SUBJECTS_ALIAS )
                .ofType( SUBJECTS_ENTITY_SET_TYPE )
                .to( SUBJECTS_ENTITY_SET_NAME )
                .key( SUBJECTS_ENTITY_SET_KEY_1 )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok()
                .addProperty( FIRSTNAME_FQN )
                .value( row -> getFirstName( row.getAs( "JName" ) ) ).ok()
                .addProperty( MIDDLENAME_FQN )
                .value( row -> getMiddleName( row.getAs( "JName" ) ) ).ok()
                .addProperty( LASTNAME_FQN )
                .value( row -> getLastName( row.getAs( "JName" ) ) ).ok()
                .addProperty( ALIAS_FQN )
                .value( row -> row.getAs( "Alias" ) ).ok()
                .addProperty( DOB_FQN )
                .value( row -> fixDate( row.getAs( "DOB" ) ) ).ok()
                .addProperty( SEX_FQN )
                .value( row -> row.getAs( "Sex" ) ).ok()
                .addProperty( RACE_FQN )
                .value( row -> row.getAs( "Race" ) ).ok()
                .addProperty( HEIGHT_FQN )
                .value( row -> row.getAs( "OHeight" ) ).ok()
                .addProperty( WEIGHT_FQN )
                .value( row -> row.getAs( "OWeight" ) ).ok()
                .addProperty( EYES_FQN )
                .value( row -> row.getAs( "OEyes" ) ).ok()
                .addProperty( HAIR_FQN )
                .value( row -> row.getAs( "OHair" ) ).ok()
                .ok()

                .addEntity( "arrestofficers" )
                .ofType( OFFICERS_ENTITY_SET_TYPE )
                .to( OFFICERS_ENTITY_SET_NAME )
                .key( OFFICERS_ENTITY_TYPE_KEY_1 )

                .addProperty( OFFICER_XREF_FQN )
                .value( row -> row.getAs( "ArrestOfficerID" ) ).ok()
                .addProperty( OFFICER_ID_FQN )
                .value( row -> row.getAs( "AO_ID" ) ).ok()
                .addProperty( OFFICER_CATEGORY_FQN )
                .value( row -> "Arrest" ).ok()
                .addProperty( FIRSTNAME_FQN )
                .value( row -> getFirstName( row.getAs( "AO" ) ) ).ok()
                .addProperty( LASTNAME_FQN )
                .value( row -> getLastName( row.getAs( "AO" ) ) ).ok().ok()

                .addEntity( "searchofficers" )
                .ofType( OFFICERS_ENTITY_SET_TYPE )
                .to( OFFICERS_ENTITY_SET_NAME )
                .key( OFFICERS_ENTITY_TYPE_KEY_1 )

                .addProperty( OFFICER_XREF_FQN )
                .value( row -> row.getAs( "SearchOfficerID" ) ).ok()
                .addProperty( OFFICER_ID_FQN )
                .value( row -> row.getAs( "Search_Officer_ID" ) ).ok()
                .addProperty( OFFICER_CATEGORY_FQN )
                .value( row -> "Search" ).ok()
                .addProperty( FIRSTNAME_FQN )
                .value( row -> getFirstName( row.getAs( "Search_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN )
                .value( row -> getLastName( row.getAs( "Search_Officer" ) ) ).ok().ok()

                .addEntity( "releaseofficers" )
                .ofType( OFFICERS_ENTITY_SET_TYPE )
                .to( OFFICERS_ENTITY_SET_NAME )
                .key( OFFICERS_ENTITY_TYPE_KEY_1 )

                .addProperty( OFFICER_XREF_FQN )
                .value( row -> row.getAs( "RelOfficerID" ) ).ok()
                .addProperty( OFFICER_ID_FQN )
                .value( row -> row.getAs( "Rel_Officer_ID" ) ).ok()
                .addProperty( OFFICER_CATEGORY_FQN )
                .value( row -> "Release" ).ok()
                .addProperty( FIRSTNAME_FQN )
                .value( row -> getFirstName( row.getAs( "Rel_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN )
                .value( row -> getLastName( row.getAs( "Rel_Officer" ) ) ).ok().ok()

                .addEntity( "transportationofficers" )
                .ofType( OFFICERS_ENTITY_SET_TYPE )
                .to( OFFICERS_ENTITY_SET_NAME )
                .key( OFFICERS_ENTITY_TYPE_KEY_1 )

                .addProperty( OFFICER_XREF_FQN )
                .value( row -> row.getAs( "TranspOfficerID" ) ).ok()
                .addProperty( OFFICER_ID_FQN )
                .value( row -> row.getAs( "Transp_Officer_ID" ) ).ok()
                .addProperty( OFFICER_CATEGORY_FQN )
                .value( row -> "Transport" ).ok()
                .addProperty( TRANSP_AGENCY_FQN )
                .value( row -> row.getAs( "Transp_Agency" ) ).ok()
                .addProperty( FIRSTNAME_FQN )
                .value( row -> getFirstName( row.getAs( "Transp_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN )
                .value( row -> getLastName( row.getAs( "Transp_Officer" ) ) ).ok().ok()

                .addEntity( BOOKINGS_ALIAS )
                .ofType( BOOKINGS_ENTITY_SET_TYPE )
                .to( BOOKINGS_ENTITY_SET_NAME )
                .key( BOOKINGS_ENTITY_TYPE_KEY_1 )

                .addProperty( JAIL_ID_FQN )
                .value( row -> row.getAs( "Jail_ID" ) ).ok()
                .addProperty( ACTUAL_NO_FQN )
                .value( row -> row.getAs( "Actual_No" ) ).ok()
                .addProperty( ARREST_NO_FQN )
                .value( row -> row.getAs( "Arrest_No" ) ).ok()
                .addProperty( DATE_IN_FQN )
                .value( row -> fixDate( row.getAs( "Date_In" ) ) ).ok()
                .addProperty( DATE_OUT_FQN )
                .value( row -> fixDate( row.getAs( "Date_Out" ) ) ).ok()
                .addProperty( HOW_RELEASED_FQN )
                .value( row -> row.getAs( "How_Rel" ) ).ok()
                .addProperty( EST_REL_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Est_Rel_Date" ) ) ).ok()
                .addProperty( ARREST_AGENCY_FQN )
                .value( row -> row.getAs( "Arrest_Agency" ) ).ok()
                .addProperty( REASON_CODE_FQN )
                .value( row -> row.getAs( "ReasonCode" ) ).ok()
                .addProperty( ARREST_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Arrest_Date" ) ) ).ok()
                .addProperty( BOOKED_ID_FQN )
                .value( row -> row.getAs( "BookedID" ) ).ok()
                .addProperty( JAIL_RECORD_XREF_FQN )
                .value( row -> row.getAs( "JailRecordId" ) ).ok().ok()

                .addEntity( JAIL_RECORDS_ALIAS )
                .ofType( JAIL_RECORDS_ENTITY_SET_TYPE )
                .to( JAIL_RECORDS_ENTITY_SET_NAME )
                .key( JAIL_RECORDS_ENTITY_TYPE_KEY_1 )

                .addProperty( JAIL_RECORD_XREF_FQN )
                .value( row -> row.getAs( "JailRecordId" ) ).ok()
                .addProperty( CAUTION_FQN )
                .value( row -> row.getAs( "Caution" ) ).ok()
                .addProperty( CURRENCY_FQN )
                .value( row -> row.getAs( "Curr" ) ).ok()
                .addProperty( CHANGE_FQN )
                .value( row -> row.getAs( "Change" ) ).ok()
                .addProperty( CHECKS_FQN )
                .value( row -> row.getAs( "Checks" ) ).ok()
                .addProperty( CALL_ATTORNEY_FQN )
                .value( row -> row.getAs( "Call_Attorney" ) ).ok()
                .addProperty( RELEASED_TO_FQN )
                .value( row -> row.getAs( "Released_To" ) ).ok()
                .addProperty( REMARKS_FQN )
                .value( row -> row.getAs( "Remarks" ) ).ok()
                .addProperty( COMIT_AUTH_FQN )
                .value( row -> row.getAs( "Comit_Auth" ) ).ok()
                .addProperty( TIME_FRAME_FQN )
                .value( row -> row.getAs( "Time_Frame" ) ).ok()
                .addProperty( TOTAL_TIME_FQN )
                .value( row -> row.getAs( "Total_Time" ) ).ok()
                .addProperty( HELD_AT_FQN )
                .value( row -> row.getAs( "Held_At" ) ).ok()
                .addProperty( ADULT_JUV_WAIVE_FQN )
                .value( row -> row.getAs( "Adult_Juv_Waive" ) ).ok()
                .addProperty( JUV_HOLD_AUTH_FQN )
                .value( row -> row.getAs( "Juv_Hold_Auth" ) ).ok()
                .addProperty( PERSON_POST_BAIL_FQN )
                .value( row -> row.getAs( "Person_Post" ) ).ok()
                .addProperty( INCLUDE_FQN )
                .value( row -> row.getAs( "Include" ) ).ok()
                .addProperty( RELEASE_NOTES_FQN )
                .value( row -> row.getAs( "ReleaseNotes" ) ).ok()
                .addProperty( ARREST_ID_LONG_FQN )
                .value( row -> row.getAs( "Arrest_ID" ) ).ok()
                .addProperty( BALANCE_FQN )
                .value( row -> row.getAs( "Balance" ) ).ok().ok()

                .ok()
                .createAssociations()

                .addAssociation( "arrestedby" )
                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "arrestofficers" )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()

                .addAssociation( "searchedby" )
                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "searchofficers" )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()

                .addAssociation( "transportedby" )
                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "transportationofficers" )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()

                .addAssociation( "releasedby" )
                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "releaseofficers" )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()

                .addAssociation( WAS_BOOKED_IN_ENTITY_SET_ALIAS )
                .ofType( WAS_BOOKED_IN_ENTITY_SET_TYPE )
                .to( WAS_BOOKED_IN_ENTITY_SET_NAME )
                .key( WAS_BOOKED_ENTITY_SET_KEY_1 )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( BOOKINGS_ALIAS )

                .addProperty( PERSON_XREF_FQN )
                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()

                .addAssociation( DETAILS_OF_IN_ENTITY_SET_ALIAS )
                .ofType( DETAILS_OF_IN_ENTITY_SET_TYPE )
                .to( DETAILS_OF_IN_ENTITY_SET_NAME )
                .key( DETAILS_OF_ENTITY_SET_KEY_1 )
                .fromEntity( JAIL_RECORDS_ALIAS )
                .toEntity( BOOKINGS_ALIAS )

                .addProperty( JAIL_RECORD_XREF_FQN )
                .value( row -> row.getAs( "JailRecordId" ) ).ok().ok()

                .ok()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( Environment.PRODUCTION, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
    public static String fixDate( Object obj ) {
        if ( obj == null ) {
            return null;

        }
        String checkme = obj.toString();
        if ( checkme.equals( "0.00" ) ) {
            logger.info( "OMG ITS THAT NUMBER -----------------------------" );
            return null;
        }
        if ( obj != null ) {
            String d = obj.toString();
            FormattedDateTime date = new FormattedDateTime( d, null, "MM/dd/yyyy", "HH:mm:ss" );
            return date.getDateTime();
        }
        return null;
    }

    // Custom Functions for Parsing First and Last Names
    public static String getFirstName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            String[] names = name.split( "," );
            if ( names.length > 1 ) {
                if ( names[ 1 ].length() > 1 ) {
                    String fix = names[ 1 ].trim();
                    String[] newnames = fix.split( " " );
                    if ( newnames.length > 1 ) {
                        return newnames[ 0 ].trim();
                    }
                    return names[ 1 ].trim();
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String getLastName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            if ( !name.equals( "" ) ) {
                String[] names = name.split( "," );
                if ( names.length > 0 ) {
                    return names[ 0 ].trim();
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String getMiddleName( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString();
            String[] names = name.split( "," );
            if ( names.length > 1 ) {
                if ( names[ 1 ].length() > 1 ) {
                    String fix = names[ 1 ].trim();
                    String[] newnames = fix.split( " " );
                    if ( newnames.length > 1 ) {
                        return newnames[ 1 ].trim();
                    }
                    return null;
                }
                return null;
            }
            return null;
        }
        return null;
    }

}
