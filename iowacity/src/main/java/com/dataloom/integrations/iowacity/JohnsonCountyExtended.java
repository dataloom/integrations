package com.dataloom.integrations.iowacity;

import com.dataloom.authorization.securable.SecurableObjectType;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.EntitySet;
import com.dataloom.edm.type.Analyzer;
import com.dataloom.edm.type.AssociationType;
import com.dataloom.edm.type.EntityType;
import com.dataloom.edm.type.PropertyType;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import org.apache.olingo.commons.api.edm.EdmPrimitiveTypeKind;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;
import retrofit2.Retrofit;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;

public class JohnsonCountyExtended {
    public static final Environment       environment                                = Environment.STAGING;
    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory
            .getLogger( JohnsonCountyExtended.class );
    // PROPERTIES
    public static FullQualifiedName JAIL_RECORD_XREF_FQN = new FullQualifiedName(
            "publicsafety.xref" );
    public static FullQualifiedName PERSON_XREF_FQN      = new FullQualifiedName(
            "publicsafety.xref" );
    public static FullQualifiedName OFFICER_XREF_FQN     = new FullQualifiedName(
            "publicsafety.xref" );
    public static FullQualifiedName ACTUAL_NO_FQN                = new FullQualifiedName(
            "publicsafety.ActualNumber" );
    public static FullQualifiedName ARREST_NO_FQN                = new FullQualifiedName(
            "j.ArrestSequenceID" );
    public static FullQualifiedName FIRSTNAME_FQN                = new FullQualifiedName(
            "nc.PersonGivenName" );
    public static FullQualifiedName MIDDLENAME_FQN               = new FullQualifiedName(
            "nc.PersonMiddleName" );
    public static FullQualifiedName LASTNAME_FQN                 = new FullQualifiedName(
            "nc.PersonSurName" );
    public static FullQualifiedName ALIAS_FQN                    = new FullQualifiedName(
            "im.PersonNickName" );
    public static FullQualifiedName DATE_IN_FQN                  = new FullQualifiedName(
            "publicsafety.datebooked" );
    public static FullQualifiedName DATE_OUT_FQN                 = new FullQualifiedName(
            "publicsafety.datereleased" );
    public static FullQualifiedName OFFICER_ID_FQN               = new FullQualifiedName(
            "publicsafety.officerID" );
    public static FullQualifiedName DOB_FQN                      = new FullQualifiedName(
            "nc.PersonBirthDate" );
    public static FullQualifiedName HOW_RELEASED_FQN             = new FullQualifiedName(
            "j.BookingRelease" );
    public static FullQualifiedName CAUTION_FQN                  = new FullQualifiedName(
            "intel.SubjectCautionInformationDescriptionText" );
    public static FullQualifiedName EST_REL_DATE_FQN             = new FullQualifiedName(
            "j.IncarcerationProjectedReleaseDate" );
    public static FullQualifiedName CURRENCY_FQN                 = new FullQualifiedName(
            "publicsafety.CurrencyAmount" );
    public static FullQualifiedName CHANGE_FQN                   = new FullQualifiedName(
            "publicsafety.ChangeAmount" );
    public static FullQualifiedName CHECKS_FQN                   = new FullQualifiedName(
            "publicsafety.ChecksAmount" );
    public static FullQualifiedName CALL_ATTORNEY_FQN            = new FullQualifiedName(
            "j.BookingTelephoneCall" );
    public static FullQualifiedName RELEASED_TO_FQN              = new FullQualifiedName(
            "j.ReleaseToFacility" );
    public static FullQualifiedName REMARKS_FQN                  = new FullQualifiedName( "j.Remark" );
    public static FullQualifiedName COMIT_AUTH_FQN               = new FullQualifiedName(
            "j.CommittedToAuthorityText" );
    public static FullQualifiedName TIME_FRAME_FQN               = new FullQualifiedName(
            "publicsafety.TimeFrame" );
    public static FullQualifiedName TOTAL_TIME_FQN               = new FullQualifiedName(
            "publicsafety.TotalTime" );
    public static FullQualifiedName HELD_AT_FQN                  = new FullQualifiedName(
            "publicsafety.HeldAt" );
    public static FullQualifiedName ADULT_JUV_WAIVE_FQN          = new FullQualifiedName(
            "scr.TreatAsAdultIndicator" );
    public static FullQualifiedName JUV_HOLD_AUTH_FQN            = new FullQualifiedName(
            "publicsafety.JuvHoldAuth" );
    public static FullQualifiedName PERSON_POST_BAIL_FQN         = new FullQualifiedName(
            "j.BailingPerson" );
    public static FullQualifiedName ARREST_AGENCY_FQN            = new FullQualifiedName(
            "j.ArrestAgency" );
    public static FullQualifiedName BOND_COURT_DATETIME_FQN      = new FullQualifiedName(
            "j.BailHearingDate" );
    public static FullQualifiedName SEX_FQN                      = new FullQualifiedName(
            "nc.PersonSex" );
    public static FullQualifiedName RACE_FQN                     = new FullQualifiedName(
            "nc.PersonRace" );
    public static FullQualifiedName SSA_FQN                      = new FullQualifiedName(
            "publicsafety.SSA" );
    public static FullQualifiedName SSA_CONVICTION_FQN           = new FullQualifiedName(
            "publicsafety.SSAConviction" );
    public static FullQualifiedName SSA_STATUS_FQN               = new FullQualifiedName(
            "publicsafety.SSAStatus" );
    public static FullQualifiedName ORI_FQN                      = new FullQualifiedName(
            "publicsafety.ORI" );
    public static FullQualifiedName DIS_SUBMIT_FQN               = new FullQualifiedName(
            "publicsafety.DISSubmit" );
    public static FullQualifiedName BALANCE_FQN                  = new FullQualifiedName(
            "publicsafety.Balance" );
    public static FullQualifiedName STATUS_FQN                   = new FullQualifiedName(
            "publicsafety.Status" );
    public static FullQualifiedName SID_ST_FQN                   = new FullQualifiedName(
            "publicsafety.SIDSt" );
    public static FullQualifiedName SID_NO_FQN                   = new FullQualifiedName(
            "publicsafety.SIDNo" );
    public static FullQualifiedName REASON_CODE_FQN              = new FullQualifiedName(
            "scr.DetentionReleaseReasonCategoryCodeType" );
    public static FullQualifiedName ARREST_DATE_FQN              = new FullQualifiedName(
            "publicsafety.arrestdate" );
    public static FullQualifiedName CASE_ID_FQN                  = new FullQualifiedName(
            "j.CaseNumberText" );
    public static FullQualifiedName HEIGHT_FQN                   = new FullQualifiedName(
            "nc.PersonHeightMeasure" );
    public static FullQualifiedName WEIGHT_FQN                   = new FullQualifiedName(
            "nc.PersonWeightMeasure" );
    public static FullQualifiedName EYES_FQN                     = new FullQualifiedName(
            "nc.PersonEyeColorText" );
    public static FullQualifiedName HAIR_FQN                     = new FullQualifiedName(
            "nc.PersonHairColorText" );
    public static FullQualifiedName TRANSP_AGENCY_FQN            = new FullQualifiedName(
            "j.EnforcementOfficialUnit" );
    public static FullQualifiedName BOOKED_ID_FQN                = new FullQualifiedName(
            "publicsafety.CustodyID" );
    public static FullQualifiedName PBT_FQN                      = new FullQualifiedName(
            "publicsafety.PortableBreathTest" );
    public static FullQualifiedName INTOX_FQN                    = new FullQualifiedName(
            "j.IntoxicationLevelText" );
    public static FullQualifiedName RELEASE_NOTES_FQN            = new FullQualifiedName(
            "publicsafety.ReleaseNotes" );
    public static FullQualifiedName ARREST_ID_LONG_FQN           = new FullQualifiedName(
            "publicsafety.ArrestID" );
    public static FullQualifiedName RELEASE_OFFICER_USERNAME_FQN = new FullQualifiedName(
            "nc.SystemUserName" );
    public static FullQualifiedName MUGSHOT_FQN          = new FullQualifiedName(
            "publicsafety.mugshot" );
    public static FullQualifiedName OFFICER_CATEGORY_FQN = new FullQualifiedName(
            "j.EnforcementOfficialCategoryText" );
    // BOOKINGS PROPERTY TYPES
    public static FullQualifiedName JAIL_ID_FQN = new FullQualifiedName(
            "publicsafety.JailID" );
    // CHARGES PROPERTY TYPE
    public static FullQualifiedName CHARGE_START_DATE_FQN   = new FullQualifiedName(
            "publicsafety.OffenseStartDate" );
    public static FullQualifiedName CHARGE_RELEASE_DATE_FQN = new FullQualifiedName(
            "publicsafety.OffenseReleaseDate" );
    public static FullQualifiedName COURT_FQN               = new FullQualifiedName(
            "j.CourtEventCase" );
    public static FullQualifiedName CHARGE_FQN              = new FullQualifiedName(
            "j.ArrestCharge" );
    public static FullQualifiedName INCLUDE_FQN             = new FullQualifiedName(
            "publicsafety.include" );
    public static FullQualifiedName BOND_MET_FQN            = new FullQualifiedName(
            "publicsafety.BondMet" );
    public static FullQualifiedName BOND_FQN                = new FullQualifiedName(
            "j.BailBondAmount" );
    public static FullQualifiedName NOTES_FQN               = new FullQualifiedName(
            "j.ChargeNarrative" );
    public static FullQualifiedName CHARGING_AGENCY_FQN     = new FullQualifiedName(
            "publicsafety.ChargeAgency" );
    public static FullQualifiedName ALT_START_DATE_FQN      = new FullQualifiedName(
            "publicsafety.AlternateStartDate" );
    public static FullQualifiedName NCIC_FQN                = new FullQualifiedName(
            "j.ChargeNCICText" );
    public static FullQualifiedName CHARGE_ID_FQN           = new FullQualifiedName(
            "j.ChargeSequenceID" );
    // OFFENSE PROPERTY TYPE
    public static FullQualifiedName OFFENSE_DATE_FQN     = new FullQualifiedName(
            "publicsafety.offensedate" );
    public static FullQualifiedName ARRESTING_AGENCY_FQN = new FullQualifiedName(
            "j.ArrestAgency" );
    public static FullQualifiedName REASON_HELD_FQN      = new FullQualifiedName(
            "publicsafety.ReasonHeld" );
    public static FullQualifiedName WARRANT_NO_FQN       = new FullQualifiedName(
            "j.ArrestWarrant" );
    public static FullQualifiedName STATE_STATUTE_FQN    = new FullQualifiedName(
            "publicsafety.OffenseViolatedStateStatute" );
    public static FullQualifiedName LOCAL_STATUTE_FQN    = new FullQualifiedName(
            "publicsafety.OffenseViolatedLocalStatute" );
    public static FullQualifiedName SEVERITY_FQN         = new FullQualifiedName(
            "j.OffenseSeverityLevelText" );
    // SENTENCE PROPERTY TYPE
    public static FullQualifiedName SEX_OFF_FQN            = new FullQualifiedName(
            "j.SentenceRegisterSexOffenderIndicator" );
    public static FullQualifiedName TIMESERVED_DAYS_FQN    = new FullQualifiedName(
            "publicsafety.TimeServedDays" );
    public static FullQualifiedName TIMESERVED_HOURS_FQN   = new FullQualifiedName(
            "publicsafety.TimeServedHours" );
    public static FullQualifiedName TIMESERVED_MINUTES_FQN = new FullQualifiedName(
            "publicsafety.TimeServedMinutes" );
    public static FullQualifiedName NO_COUNTS_FQN          = new FullQualifiedName(
            "publicsafety.NoCounts" );
    public static FullQualifiedName SENTENCE_DAYS_FQN      = new FullQualifiedName(
            "publicsafety.SentenceTermDays" );
    public static FullQualifiedName SENTENCE_HOURS_FQN     = new FullQualifiedName(
            "publicsafety.SentenceTermHours" );
    public static FullQualifiedName GTDAYS_FQN             = new FullQualifiedName(
            "publicsafety.GoodTimeDays" );
    public static FullQualifiedName GTHOURS_FQN            = new FullQualifiedName(
            "publicsafety.GoodTimeHours" );
    public static FullQualifiedName GTMINUTES_FQN          = new FullQualifiedName(
            "publicsafety.GoodTimeMinutes" );
    public static FullQualifiedName ENTRY_DATE_FQN         = new FullQualifiedName(
            "j.RegisteredOffenderEntryDate" );
    public static FullQualifiedName GTPCT_FQN              = new FullQualifiedName(
            "publicsafety.GoodTimePCT" );
    public static FullQualifiedName PROBATION_FQN          = new FullQualifiedName(
            "j.SentenceModificationProbationIndicator" );
    public static FullQualifiedName CONCURRENT_FQN         = new FullQualifiedName(
            "publicsafety.Concurrent" );
    public static FullQualifiedName CONSEC_WITH_FQN        = new FullQualifiedName(
            "publicsafety.ConsecWith" );
    // EXISTING ENTITIES (BOOKINGS IS FROM other integration)
    public static String            BOOKINGS_ENTITY_SET_NAME   = "jcjailbookings";
    public static FullQualifiedName BOOKINGS_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.JailBookingType2" );
    public static FullQualifiedName BOOKINGS_ENTITY_TYPE_KEY_1 = JAIL_ID_FQN;
    public static String            BOOKINGS_ALIAS             = "bookings";
    // NEW ENTITIES
    public static String            CHARGES_ENTITY_SET_NAME   = "jciowacharges";
    public static FullQualifiedName CHARGES_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.ChargesType2" );
    public static FullQualifiedName CHARGES_ENTITY_TYPE_KEY_1 = CHARGE_ID_FQN;
    public static String            CHARGES_ALIAS             = "charges";
    public static String            OFFENSES_ENTITY_SET_NAME   = "jciowaoffenses";
    public static FullQualifiedName OFFENSES_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.OffensesType2" );
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_1 = OFFENSE_DATE_FQN;
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_2 = REASON_HELD_FQN;
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_3 = WARRANT_NO_FQN;
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_4 = STATE_STATUTE_FQN;
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_5 = LOCAL_STATUTE_FQN;
    public static FullQualifiedName OFFENSES_ENTITY_TYPE_KEY_6 = SEVERITY_FQN;
    public static String            OFFENSES_ALIAS             = "offenses";
    public static String            SENTENCES_ENTITY_SET_NAME    = "jciowasentences";
    public static FullQualifiedName SENTENCES_ENTITY_SET_TYPE    = new FullQualifiedName(
            "jciowa.SentencesType2" );
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_1  = SEX_OFF_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_2  = TIMESERVED_DAYS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_3  = TIMESERVED_HOURS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_4  = TIMESERVED_MINUTES_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_5  = SENTENCE_DAYS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_6  = SENTENCE_HOURS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_7  = GTDAYS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_8  = GTHOURS_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_9  = GTMINUTES_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_10 = ENTRY_DATE_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_11 = GTPCT_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_12 = PROBATION_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_13 = CONCURRENT_FQN;
    public static FullQualifiedName SENTENCES_ENTITY_TYPE_KEY_14 = CONSEC_WITH_FQN;
    public static String            SENTENCES_ALIAS              = "sentences";
    // NEW ASSOCIATIONS
    public static String            CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_NAME  = "jciowachargesappearin2";
    public static FullQualifiedName CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_TYPE  = new FullQualifiedName(
            "jciowa.ChargesAppearInJailBooking2" );
    public static FullQualifiedName CHARGES_APPEAR_IN_BOOKING_ENTITY_SET_KEY_1    = CHARGE_ID_FQN;
    public static String            CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_ALIAS = "appearsin";
    public static String            OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_NAME   = "jciowaoffenseresultsin2";
    public static FullQualifiedName OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.OffensesResultInCharge2" );
    public static FullQualifiedName OFFENSE_RESULTS_IN_CHARGES_ENTITY_TYPE_KEY_1 = CHARGE_ID_FQN;
    public static String            OFFENSE_RESULTS_IN_CHARGES_ALIAS             = "resultsin";
    public static       String            CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_NAME   = "jciowachargeleadsto2";
    public static       FullQualifiedName CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_TYPE   = new FullQualifiedName(
            "jciowa.ChargesLeadToSentence2" );
    public static       FullQualifiedName CHARGE_LEADS_TO_SENTENCE_ENTITY_TYPE_KEY_1 = CHARGE_ID_FQN;
    public static       String            CHARGE_LEADS_TO_SENTENCE_ALIAS             = "leadsto";

    public static void main( String[] args ) throws InterruptedException {

        if ( args.length < 3 ) {
            System.out.println( "expected: <path> <jwtToken>" );
            return;
        }

        final String path = args[ 1 ];
        final String jwtToken = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        // final String jwtToken = MissionControl.getIdToken( username, password );
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        /*
         * CREATE EDM OBJECTS
         */

        /*
         * PROPERTY TYPES
         */
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        // NEW PROPERTY TYPES

        UUID Charge_Start_Date = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGE_START_DATE_FQN,
                        "Offense Charge Start Date",
                        Optional.of( "Offense Charge Start Date" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Charge_Start_Date == null ) {
            Charge_Start_Date = edm
                    .getPropertyTypeId( CHARGE_START_DATE_FQN.getNamespace(), CHARGE_START_DATE_FQN.getName() );
        }

        UUID Charge_Release_Date = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGE_RELEASE_DATE_FQN,
                        "Offense Charge Release Date",
                        Optional.of( "Offense Charge Release Date" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Charge_Release_Date == null ) {
            Charge_Release_Date = edm
                    .getPropertyTypeId( CHARGE_RELEASE_DATE_FQN.getNamespace(), CHARGE_RELEASE_DATE_FQN.getName() );
        }

        UUID Court = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        COURT_FQN,
                        "Court Case ID",
                        Optional.of( "Court Case ID" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Court == null ) {
            Court = edm
                    .getPropertyTypeId( COURT_FQN.getNamespace(), COURT_FQN.getName() );
        }

        UUID Charge = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGE_FQN,
                        "Arrest Charge",
                        Optional.of( "Arrest Charge" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Charge == null ) {
            Charge = edm
                    .getPropertyTypeId( CHARGE_FQN.getNamespace(), CHARGE_FQN.getName() );
        }

        UUID Concurrent = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CONCURRENT_FQN,
                        "Concurrent Sentence",
                        Optional.of( "Concurrent Sentence" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Concurrent == null ) {
            Concurrent = edm
                    .getPropertyTypeId( CONCURRENT_FQN.getNamespace(), CONCURRENT_FQN.getName() );
        }

        UUID ConsecWith = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CONSEC_WITH_FQN,
                        "Consecutive Sentence",
                        Optional.of( "Consecutive Sentence" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int64,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ConsecWith == null ) {
            ConsecWith = edm
                    .getPropertyTypeId( CONSEC_WITH_FQN.getNamespace(), CONSEC_WITH_FQN.getName() );
        }

        UUID NoCounts = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NO_COUNTS_FQN,
                        "No Counts",
                        Optional.of( "No Counts" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int64,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( NoCounts == null ) {
            NoCounts = edm
                    .getPropertyTypeId( NO_COUNTS_FQN.getNamespace(), NO_COUNTS_FQN.getName() );
        }

        UUID BondMet = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOND_MET_FQN,
                        "Bond Met",
                        Optional.of( "Bond Met" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( BondMet == null ) {
            BondMet = edm
                    .getPropertyTypeId( BOND_MET_FQN.getNamespace(), BOND_MET_FQN.getName() );
        }

        UUID Bond = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        BOND_FQN,
                        "Bond Amount",
                        Optional.of( "Bond Amount" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Bond == null ) {
            Bond = edm
                    .getPropertyTypeId( BOND_FQN.getNamespace(), BOND_FQN.getName() );
        }

        UUID Notes = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NOTES_FQN,
                        "Notes",
                        Optional.of( "Charge Narrative" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Notes == null ) {
            Notes = edm
                    .getPropertyTypeId( NOTES_FQN.getNamespace(), NOTES_FQN.getName() );
        }

        UUID ChargingAgency = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGING_AGENCY_FQN,
                        "Charging Agency",
                        Optional.of( "Charging Agency" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ChargingAgency == null ) {
            ChargingAgency = edm
                    .getPropertyTypeId( CHARGING_AGENCY_FQN.getNamespace(), CHARGING_AGENCY_FQN.getName() );
        }

        UUID AltStartDate = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ALT_START_DATE_FQN,
                        "Alternate Start Date",
                        Optional.of( "Alternate Offense Charge Start Date" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( AltStartDate == null ) {
            AltStartDate = edm
                    .getPropertyTypeId( ALT_START_DATE_FQN.getNamespace(), ALT_START_DATE_FQN.getName() );
        }

        UUID ncic = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        NCIC_FQN,
                        "NCIC",
                        Optional.of( "NCIC Charge Text" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ncic == null ) {
            ncic = edm
                    .getPropertyTypeId( NCIC_FQN.getNamespace(), NCIC_FQN.getName() );
        }

        UUID chargeID = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        CHARGE_ID_FQN,
                        "Charge ID",
                        Optional.of( "Charge ID" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( chargeID == null ) {
            chargeID = edm
                    .getPropertyTypeId( CHARGE_ID_FQN.getNamespace(), CHARGE_ID_FQN.getName() );
        }

        UUID offenseDate = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        OFFENSE_DATE_FQN,
                        "Offense Date",
                        Optional.of( "Offense Date" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( offenseDate == null ) {
            offenseDate = edm
                    .getPropertyTypeId( OFFENSE_DATE_FQN.getNamespace(), OFFENSE_DATE_FQN.getName() );
        }

        UUID ReasonHeld = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        REASON_HELD_FQN,
                        "Reason Held",
                        Optional.of( "Reason Held" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( ReasonHeld == null ) {
            ReasonHeld = edm
                    .getPropertyTypeId( REASON_HELD_FQN.getNamespace(), REASON_HELD_FQN.getName() );
        }

        UUID WarrantNumber = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        WARRANT_NO_FQN,
                        "Warrant Number",
                        Optional.of( "Warrant Number" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( WarrantNumber == null ) {
            WarrantNumber = edm
                    .getPropertyTypeId( WARRANT_NO_FQN.getNamespace(), WARRANT_NO_FQN.getName() );
        }

        UUID StateStatute = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        STATE_STATUTE_FQN,
                        "State Statute",
                        Optional.of( "State Statute" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( StateStatute == null ) {
            StateStatute = edm
                    .getPropertyTypeId( STATE_STATUTE_FQN.getNamespace(), STATE_STATUTE_FQN.getName() );
        }

        UUID LocalStatute = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        LOCAL_STATUTE_FQN,
                        "Local Statute",
                        Optional.of( "Local Statute" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( LocalStatute == null ) {
            LocalStatute = edm
                    .getPropertyTypeId( LOCAL_STATUTE_FQN.getNamespace(), LOCAL_STATUTE_FQN.getName() );
        }

        UUID Severity = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SEVERITY_FQN,
                        "Severity",
                        Optional.of( "Severity" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.String,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Severity == null ) {
            Severity = edm
                    .getPropertyTypeId( SEVERITY_FQN.getNamespace(), SEVERITY_FQN.getName() );
        }

        UUID SexOff = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SEX_OFF_FQN,
                        "Sex Offender",
                        Optional.of( "Sex Offender" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( SexOff == null ) {
            SexOff = edm
                    .getPropertyTypeId( SEX_OFF_FQN.getNamespace(), SEX_OFF_FQN.getName() );
        }

        UUID TimeServedDays = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TIMESERVED_DAYS_FQN,
                        "Time Served Days",
                        Optional.of( "Time Served Days" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( TimeServedDays == null ) {
            TimeServedDays = edm
                    .getPropertyTypeId( TIMESERVED_DAYS_FQN.getNamespace(), TIMESERVED_DAYS_FQN.getName() );
        }

        UUID TimeServedHours = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TIMESERVED_HOURS_FQN,
                        "Time Served Hours",
                        Optional.of( "Time Served Hours" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( TimeServedHours == null ) {
            TimeServedHours = edm
                    .getPropertyTypeId( TIMESERVED_HOURS_FQN.getNamespace(), TIMESERVED_HOURS_FQN.getName() );
        }

        UUID TimeServedMins = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        TIMESERVED_MINUTES_FQN,
                        "Time Served Minutes",
                        Optional.of( "Time Served Minutes" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( TimeServedMins == null ) {
            TimeServedMins = edm
                    .getPropertyTypeId( TIMESERVED_MINUTES_FQN.getNamespace(), TIMESERVED_MINUTES_FQN.getName() );
        }

        UUID SentenceDays = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SENTENCE_DAYS_FQN,
                        "Sentence Days",
                        Optional.of( "Sentence Days" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( SentenceDays == null ) {
            SentenceDays = edm
                    .getPropertyTypeId( SENTENCE_DAYS_FQN.getNamespace(), SENTENCE_DAYS_FQN.getName() );
        }

        UUID SentenceHours = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        SENTENCE_HOURS_FQN,
                        "Sentence Hours",
                        Optional.of( "Sentence Hours" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( SentenceHours == null ) {
            SentenceHours = edm
                    .getPropertyTypeId( SENTENCE_HOURS_FQN.getNamespace(), SENTENCE_HOURS_FQN.getName() );
        }

        UUID GTDays = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GTDAYS_FQN,
                        "Good Time Days",
                        Optional.of( "Good Time Days" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( GTDays == null ) {
            GTDays = edm
                    .getPropertyTypeId( GTDAYS_FQN.getNamespace(), GTDAYS_FQN.getName() );
        }

        UUID GTHours = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GTHOURS_FQN,
                        "Good Time Hours",
                        Optional.of( "Good Time Hours" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( GTHours == null ) {
            GTHours = edm
                    .getPropertyTypeId( GTHOURS_FQN.getNamespace(), GTHOURS_FQN.getName() );
        }

        UUID GTMins = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GTMINUTES_FQN,
                        "Good Time Minutes",
                        Optional.of( "Good Time Minutes" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( GTMins == null ) {
            GTMins = edm
                    .getPropertyTypeId( GTMINUTES_FQN.getNamespace(), GTMINUTES_FQN.getName() );
        }

        UUID EntryDate = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        ENTRY_DATE_FQN,
                        "Entry Date",
                        Optional.of( "Entry Date" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Date,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( EntryDate == null ) {
            EntryDate = edm
                    .getPropertyTypeId( ENTRY_DATE_FQN.getNamespace(), ENTRY_DATE_FQN.getName() );
        }

        UUID GTPct = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        GTPCT_FQN,
                        "Good Time PCT",
                        Optional.of( "Good Time PCT" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( GTPct == null ) {
            GTPct = edm
                    .getPropertyTypeId( GTPCT_FQN.getNamespace(), GTPCT_FQN.getName() );
        }

        UUID Probation = edm
                .createPropertyType( new PropertyType(
                        Optional.absent(),
                        PROBATION_FQN,
                        "Probation",
                        Optional.of( "Probation" ),
                        ImmutableSet.of(),
                        EdmPrimitiveTypeKind.Int32,
                        Optional.of( false ),
                        Optional.of( Analyzer.STANDARD ) ) );
        if ( Probation == null ) {
            Probation = edm
                    .getPropertyTypeId( PROBATION_FQN.getNamespace(), PROBATION_FQN.getName() );
        }

        // EXISITING PROPERTY TYPES

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

        // EXISTING BOOKING TYPE

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

        // CHARGES TYPE

        LinkedHashSet<UUID> chargeKey = new LinkedHashSet<UUID>();
        chargeKey.add( chargeID );

        LinkedHashSet<UUID> chargeProperties = new LinkedHashSet<UUID>();
        chargeProperties.add( chargeID );
        chargeProperties.add( Charge_Start_Date );
        chargeProperties.add( Charge_Release_Date );
        chargeProperties.add( Court );
        chargeProperties.add( Charge );
        chargeProperties.add( Include );
        chargeProperties.add( BondMet );
        chargeProperties.add( Bond );
        chargeProperties.add( Notes );
        chargeProperties.add( ChargingAgency );
        chargeProperties.add( AltStartDate );
        chargeProperties.add( ncic );

        UUID chargeType = edm.createEntityType( new EntityType(
                CHARGES_ENTITY_SET_TYPE,
                "Charge Type",
                "Entity type for an offense charge",
                ImmutableSet.of(),
                chargeKey,
                chargeProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( chargeType == null ) {
            chargeType = edm.getEntityTypeId(
                    CHARGES_ENTITY_SET_TYPE.getNamespace(), CHARGES_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> offenseKey = new LinkedHashSet<UUID>();
        offenseKey.add( offenseDate );
        offenseKey.add( ReasonHeld );
        offenseKey.add( WarrantNumber );
        offenseKey.add( StateStatute );
        offenseKey.add( LocalStatute );
        offenseKey.add( Severity );

        LinkedHashSet<UUID> offenseProperties = new LinkedHashSet<UUID>();
        offenseProperties.add( offenseDate );
        offenseProperties.add( ReasonHeld );
        offenseProperties.add( WarrantNumber );
        offenseProperties.add( StateStatute );
        offenseProperties.add( LocalStatute );
        offenseProperties.add( Severity );

        UUID offenseType = edm.createEntityType( new EntityType(
                OFFENSES_ENTITY_SET_TYPE,
                "Offense Type",
                "Entity type for an offense",
                ImmutableSet.of(),
                offenseKey,
                offenseProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( offenseType == null ) {
            offenseType = edm.getEntityTypeId(
                    OFFENSES_ENTITY_SET_TYPE.getNamespace(), OFFENSES_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> sentenceKey = new LinkedHashSet<UUID>();
        sentenceKey.add( Concurrent );
        sentenceKey.add( SexOff );
        sentenceKey.add( TimeServedDays );
        sentenceKey.add( TimeServedHours );
        sentenceKey.add( TimeServedMins );
        sentenceKey.add( ConsecWith );
        sentenceKey.add( NoCounts );
        sentenceKey.add( SentenceDays );
        sentenceKey.add( SentenceHours );
        sentenceKey.add( GTDays );
        sentenceKey.add( GTHours );
        sentenceKey.add( GTMins );
        sentenceKey.add( EntryDate );
        sentenceKey.add( GTPct );
        sentenceKey.add( Probation );

        LinkedHashSet<UUID> sentenceProperties = new LinkedHashSet<UUID>();
        sentenceProperties.add( Concurrent );
        sentenceProperties.add( SexOff );
        sentenceProperties.add( TimeServedDays );
        sentenceProperties.add( TimeServedHours );
        sentenceProperties.add( TimeServedMins );
        sentenceProperties.add( ConsecWith );
        sentenceProperties.add( NoCounts );
        sentenceProperties.add( SentenceDays );
        sentenceProperties.add( SentenceHours );
        sentenceProperties.add( GTDays );
        sentenceProperties.add( GTHours );
        sentenceProperties.add( GTMins );
        sentenceProperties.add( EntryDate );
        sentenceProperties.add( GTPct );
        sentenceProperties.add( Probation );

        UUID sentenceType = edm.createEntityType( new EntityType(
                SENTENCES_ENTITY_SET_TYPE,
                "Sentence Type",
                "Entity type for an sentence",
                ImmutableSet.of(),
                sentenceKey,
                sentenceProperties,
                Optional.absent(),
                Optional.of( SecurableObjectType.EntityType ) ) );
        if ( sentenceType == null ) {
            sentenceType = edm.getEntityTypeId(
                    SENTENCES_ENTITY_SET_TYPE.getNamespace(), SENTENCES_ENTITY_SET_TYPE.getName() );
        }

        // ASSOCIATIONS

        LinkedHashSet<UUID> appearsInKey = new LinkedHashSet<UUID>();
        appearsInKey.add( chargeID );

        LinkedHashSet<UUID> appearsInProperties = new LinkedHashSet<UUID>();
        appearsInProperties.add( chargeID );

        LinkedHashSet<UUID> appearsInSource = new LinkedHashSet<UUID>();
        appearsInSource.add( chargeType );
        LinkedHashSet<UUID> appearsInDestination = new LinkedHashSet<UUID>();
        appearsInDestination.add( jailBookingType );

        UUID appearsInType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_TYPE,
                        "appears in",
                        "charge appears in booking association type",
                        ImmutableSet.of(),
                        appearsInKey,
                        appearsInProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                appearsInSource,
                appearsInDestination,
                false ) );
        if ( appearsInType == null ) {
            appearsInType = edm.getEntityTypeId(
                    CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_TYPE.getNamespace(),
                    CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> resultInKey = new LinkedHashSet<UUID>();
        resultInKey.add( chargeID );

        LinkedHashSet<UUID> resultInProperties = new LinkedHashSet<UUID>();
        resultInProperties.add( chargeID );

        LinkedHashSet<UUID> resultInSource = new LinkedHashSet<UUID>();
        resultInSource.add( offenseType );
        LinkedHashSet<UUID> resultInDestination = new LinkedHashSet<UUID>();
        resultInDestination.add( chargeType );

        UUID resultInType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_TYPE,
                        "results in",
                        "offense results in charge association type",
                        ImmutableSet.of(),
                        resultInKey,
                        resultInProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                resultInSource,
                resultInDestination,
                false ) );
        if ( resultInType == null ) {
            resultInType = edm.getEntityTypeId(
                    OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_TYPE.getNamespace(),
                    OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_TYPE.getName() );
        }

        LinkedHashSet<UUID> leadsToKey = new LinkedHashSet<UUID>();
        leadsToKey.add( chargeID );

        LinkedHashSet<UUID> leadsToProperties = new LinkedHashSet<UUID>();
        leadsToProperties.add( chargeID );

        LinkedHashSet<UUID> leadsToSource = new LinkedHashSet<UUID>();
        leadsToSource.add( chargeType );
        LinkedHashSet<UUID> leadsToDestination = new LinkedHashSet<UUID>();
        leadsToDestination.add( sentenceType );

        UUID leadsToType = edm.createAssociationType( new AssociationType(
                Optional.of( new EntityType(
                        CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_TYPE,
                        "leads to",
                        "charge leads to sentence association type",
                        ImmutableSet.of(),
                        leadsToKey,
                        leadsToProperties,
                        Optional.absent(),
                        Optional.of( SecurableObjectType.AssociationType ) ) ),
                leadsToSource,
                leadsToDestination,
                false ) );
        if ( leadsToType == null ) {
            leadsToType = edm.getEntityTypeId(
                    CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_TYPE.getNamespace(),
                    CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_TYPE.getName() );
        }

        /*
         * ENTITY SETS
         */

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                jailBookingType,
                BOOKINGS_ENTITY_SET_NAME,
                "JC Iowa Jail Bookings",
                Optional.of( "Johnson County Iowa Jail Bookings" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                chargeType,
                CHARGES_ENTITY_SET_NAME,
                "JC Iowa Charges",
                Optional.of( "Johnson County Iowa Charges" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                sentenceType,
                SENTENCES_ENTITY_SET_NAME,
                "JC Iowa Sentences",
                Optional.of( "Johnson County Iowa Sentences" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                offenseType,
                OFFENSES_ENTITY_SET_NAME,
                "JC Iowa Offenses",
                Optional.of( "Johnson County Offenses" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                leadsToType,
                CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_NAME,
                "leads to",
                Optional.of( "---- charge leads to sentence association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                appearsInType,
                CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_NAME,
                "appear in",
                Optional.of( "---- charges appear in booking association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                resultInType,
                OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_NAME,
                "results in",
                Optional.of( "---- offense results in charge association ----" ),
                ImmutableSet.of( "Loom <help@thedataloom.com>" ) ) ) );

        Map<Flight, Dataset<Row>> flights = Maps.newHashMap();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( BOOKINGS_ALIAS )
                .ofType( BOOKINGS_ENTITY_SET_TYPE )
                .to( BOOKINGS_ENTITY_SET_NAME )
                .key( BOOKINGS_ENTITY_TYPE_KEY_1 )
                .useCurrentSync()

                .addProperty( JAIL_ID_FQN )
                .value( row -> row.getAs( "Jail_ID" ) ).ok()
                .addProperty( HOW_RELEASED_FQN )
                .value( row -> row.getAs( "How_Rel" ) ).ok()
                .addProperty( EST_REL_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Exp_Release_Date" ), "exp release date" ) ).ok()
                .addProperty( ARREST_AGENCY_FQN )
                .value( row -> row.getAs( "Arresting_Agency" ) ).ok()
                .ok()

                .addEntity( CHARGES_ALIAS )
                .ofType( CHARGES_ENTITY_SET_TYPE )
                .to( CHARGES_ENTITY_SET_NAME )
                .key( CHARGES_ENTITY_TYPE_KEY_1 )
                .useCurrentSync()

                .addProperty( CHARGE_ID_FQN )
                .value( row -> row.getAs( "ID" ) ).ok()
                .addProperty( CHARGE_START_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Start_Date" ), "start date" ) ).ok()
                .addProperty( CHARGE_RELEASE_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Release_Date" ), "release date" ) ).ok()
                .addProperty( COURT_FQN )
                .value( row -> row.getAs( "Court" ) ).ok()
                .addProperty( CHARGE_FQN )
                .value( row -> row.getAs( "Charge" ) ).ok()
                .addProperty( INCLUDE_FQN )
                .value( row -> row.getAs( "Include" ) ).ok()
                .addProperty( BOND_MET_FQN )
                .value( row -> row.getAs( "Bond_Met" ) ).ok()
                .addProperty( BOND_FQN )
                .value( row -> row.getAs( "Bond" ) ).ok()
                .addProperty( NOTES_FQN )
                .value( row -> row.getAs( "Notes" ) ).ok()
                .addProperty( CHARGING_AGENCY_FQN )
                .value( row -> row.getAs( "Charging_Agency" ) ).ok()
                .addProperty( ALT_START_DATE_FQN )
                .value( row -> fixDate( row.getAs( "Alt_Start_Date" ), "alt start date" ) ).ok()
                .addProperty( NCIC_FQN )
                .value( row -> row.getAs( "NCIC" ) ).ok().ok()

                .addEntity( OFFENSES_ALIAS )
                .ofType( OFFENSES_ENTITY_SET_TYPE )
                .to( OFFENSES_ENTITY_SET_NAME )
                .key( OFFENSES_ENTITY_TYPE_KEY_1,
                        OFFENSES_ENTITY_TYPE_KEY_2,
                        OFFENSES_ENTITY_TYPE_KEY_3,
                        OFFENSES_ENTITY_TYPE_KEY_4,
                        OFFENSES_ENTITY_TYPE_KEY_5,
                        OFFENSES_ENTITY_TYPE_KEY_6 )
                .useCurrentSync()

                .addProperty( OFFENSE_DATE_FQN )
                .value( row -> row.getAs( "Off_Date" ) ).ok()
                .addProperty( REASON_HELD_FQN )
                .value( row -> row.getAs( "ReasonHeld" ) ).ok()
                .addProperty( WARRANT_NO_FQN )
                .value( row -> row.getAs( "Warr_No" ) ).ok()
                .addProperty( STATE_STATUTE_FQN )
                .value( row -> row.getAs( "State" ) ).ok()
                .addProperty( LOCAL_STATUTE_FQN )
                .value( row -> row.getAs( "Local" ) ).ok()
                .addProperty( SEVERITY_FQN )
                .value( row -> row.getAs( "Severity" ) ).ok().ok()

                .addEntity( SENTENCES_ALIAS )
                .ofType( SENTENCES_ENTITY_SET_TYPE )
                .to( SENTENCES_ENTITY_SET_NAME )
                .key( SENTENCES_ENTITY_TYPE_KEY_1,
                        SENTENCES_ENTITY_TYPE_KEY_2,
                        SENTENCES_ENTITY_TYPE_KEY_3,
                        SENTENCES_ENTITY_TYPE_KEY_4,
                        SENTENCES_ENTITY_TYPE_KEY_5,
                        SENTENCES_ENTITY_TYPE_KEY_6,
                        SENTENCES_ENTITY_TYPE_KEY_7,
                        SENTENCES_ENTITY_TYPE_KEY_8,
                        SENTENCES_ENTITY_TYPE_KEY_9,
                        SENTENCES_ENTITY_TYPE_KEY_10,
                        SENTENCES_ENTITY_TYPE_KEY_11,
                        SENTENCES_ENTITY_TYPE_KEY_12,
                        SENTENCES_ENTITY_TYPE_KEY_13,
                        SENTENCES_ENTITY_TYPE_KEY_14 )
                .useCurrentSync()

                .addProperty( CONCURRENT_FQN )
                .value( row -> row.getAs( "Concurrent" ) ).ok()
                .addProperty( SEX_OFF_FQN )
                .value( row -> row.getAs( "SexOff" ) ).ok()
                .addProperty( TIMESERVED_DAYS_FQN )
                .value( row -> row.getAs( "TSrvdDays" ) ).ok()
                .addProperty( TIMESERVED_HOURS_FQN )
                .value( row -> row.getAs( "TSrvdHrs" ) ).ok()
                .addProperty( TIMESERVED_MINUTES_FQN )
                .value( row -> row.getAs( "TSrvdMins" ) ).ok()
                .addProperty( CONSEC_WITH_FQN )
                .value( row -> row.getAs( "ConsecWith" ) ).ok()
                .addProperty( NO_COUNTS_FQN )
                .value( row -> row.getAs( "NoCounts" ) ).ok()
                .addProperty( SENTENCE_DAYS_FQN )
                .value( row -> row.getAs( "SentenceDays" ) ).ok()
                .addProperty( SENTENCE_HOURS_FQN )
                .value( row -> row.getAs( "SentenceHrs" ) ).ok()
                .addProperty( GTDAYS_FQN )
                .value( row -> row.getAs( "GTDays" ) ).ok()
                .addProperty( GTHOURS_FQN )
                .value( row -> row.getAs( "GTHrs" ) ).ok()
                .addProperty( GTMINUTES_FQN )
                .value( row -> row.getAs( "GTMins" ) ).ok()
                .addProperty( ENTRY_DATE_FQN )
                .value( row -> fixDate( row.getAs( "EntryDate" ), "entry date" ) ).ok()
                .addProperty( GTPCT_FQN )
                .value( row -> row.getAs( "GTPct" ) ).ok()
                .addProperty( PROBATION_FQN )
                .value( row -> row.getAs( "Probation" ) ).ok()
                .ok()

                .ok()
                .createAssociations()

                .addAssociation( CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_ALIAS )
                .ofType( CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_TYPE )
                .to( CHARGES_APPEAR_IN_BOOKING_IN_ENTITY_SET_NAME )
                .key( CHARGES_APPEAR_IN_BOOKING_ENTITY_SET_KEY_1 )
                .fromEntity( CHARGES_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .useCurrentSync()

                .addProperty( CHARGE_ID_FQN )
                .value( row -> row.getAs( "ID" ) ).ok().ok()

                .addAssociation( OFFENSE_RESULTS_IN_CHARGES_ALIAS )
                .ofType( OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_TYPE )
                .to( OFFENSE_RESULTS_IN_CHARGES_ENTITY_SET_NAME )
                .key( OFFENSE_RESULTS_IN_CHARGES_ENTITY_TYPE_KEY_1 )
                .fromEntity( OFFENSES_ALIAS )
                .toEntity( CHARGES_ALIAS )
                .useCurrentSync()

                .addProperty( CHARGE_ID_FQN )
                .value( row -> row.getAs( "ID" ) ).ok().ok()

                .addAssociation( CHARGE_LEADS_TO_SENTENCE_ALIAS )
                .ofType( CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_TYPE )
                .to( CHARGE_LEADS_TO_SENTENCE_ENTITY_SET_NAME )
                .key( CHARGE_LEADS_TO_SENTENCE_ENTITY_TYPE_KEY_1 )
                .fromEntity( CHARGES_ALIAS )
                .toEntity( SENTENCES_ALIAS )
                .useCurrentSync()

                .addProperty( CHARGE_ID_FQN )
                .value( row -> row.getAs( "ID" ) ).ok().ok()

                .ok()

                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

    // CUSTOM FUNCTIONS DEFINED BELOW
    public static String fixDate( Object obj, String from ) {
        if ( obj == null ) {
            return null;

        }
        String checkme = obj.toString();
        if ( checkme.equals( "0.00" ) || checkme.equals( "_x000C_" ) || checkme.equals( "0" )
                || checkme.equals( "5202" ) || checkme.equals( "CNTM" ) || checkme.equals( "-1" ) ) {
            logger.info( "OMG ITS THAT NUMBER -----------------------------" );
            logger.info( from );
            logger.info( checkme );
            return null;
        }
        if ( obj != null ) {
            String d = obj.toString();
            FormattedDateTime date = new FormattedDateTime( d, null, "dd-MMM-yy", "HH:mm:ss" );
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
