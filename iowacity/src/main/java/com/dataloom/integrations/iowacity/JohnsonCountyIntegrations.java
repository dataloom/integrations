package com.dataloom.integrations.iowacity;

import com.openlattice.shuttle.Flight;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created by mtamayo on 6/29/17.
 */
public class JohnsonCountyIntegrations {

    public static void main( String[] args ) throws InterruptedException {
        switch( args[0] ) {
            case "0":
                JohnsonCountyJailBookings.main( args );
                //JohnsonCounty.main( args );
                break;
            case "1":
                JohnsonCountyExtended.main( args );
                break;
            case "2":
                JohnsonCountyMugshots.main( args );

        }
    }

//    public static Flight getPrimaryFlight( SparkSession sparkSession , String path) {
//        Dataset<Row> payload = sparkSession
//                .read()
//                .format( "com.databricks.spark.csv" )
//                .option( "header", "true" )
//                .load( path );
//
//        Flight flight = Flight.newFlight()
//                .createEntities()
//
//                .addEntity( "subjects" )
//                .ofType( "justice.booking" )
//                .to( "jcbookings" )
//                .key( "nc.SubjectIdentification" )
//                .addProperty( "nc.SubjectIdentification" )
//                .value( row -> row.getAs( "MNI_No" ) ).ok()
//                .addProperty( "nc.GivenName" )
//                .value( row -> getFirstName( row.getAs( "JName" ) ) ).ok()
//                .addProperty( "nc.MiddleName" )
//                .value( row -> getMiddleName( row.getAs( "JName" ) ) ).ok()
//                .addProperty( "nc.LastName" )
//                .value( row -> getLastName( row.getAs( "JName" ) ) ).ok()
//                .addProperty( ALIAS_FQN )
//                .value( row -> row.getAs( "Alias" ) ).ok()
//                .addProperty( DOB_FQN )
//                .value( row -> fixDate( row.getAs( "DOB" ) ) ).ok()
//                .addProperty( SEX_FQN )
//                .value( row -> row.getAs( "Sex" ) ).ok()
//                .addProperty( RACE_FQN )
//                .value( row -> row.getAs( "Race" ) ).ok()
//                .addProperty( HEIGHT_FQN )
//                .value( row -> row.getAs( "OHeight" ) ).ok()
//                .addProperty( WEIGHT_FQN )
//                .value( row -> row.getAs( "OWeight" ) ).ok()
//                .addProperty( EYES_FQN )
//                .value( row -> row.getAs( "OEyes" ) ).ok()
//                .addProperty( HAIR_FQN )
//                .value( row -> row.getAs( "OHair" ) ).ok()
//                .ok()
//
//                .addEntity( "arrestofficers" )
//                .ofType( OFFICERS_ENTITY_SET_TYPE )
//                .to( OFFICERS_ENTITY_SET_NAME )
//                .key( OFFICERS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( OFFICER_XREF_FQN )
//                .value( row -> row.getAs( "ArrestOfficerID" ) ).ok()
//                .addProperty( OFFICER_ID_FQN )
//                .value( row -> row.getAs( "AO_ID" ) ).ok()
//                .addProperty( OFFICER_CATEGORY_FQN )
//                .value( row -> "Arrest" ).ok()
//                .addProperty( FIRSTNAME_FQN )
//                .value( row -> getFirstName( row.getAs( "AO" ) ) ).ok()
//                .addProperty( LASTNAME_FQN )
//                .value( row -> getLastName( row.getAs( "AO" ) ) ).ok().ok()
//
//                .addEntity( "searchofficers" )
//                .ofType( OFFICERS_ENTITY_SET_TYPE )
//                .to( OFFICERS_ENTITY_SET_NAME )
//                .key( OFFICERS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( OFFICER_XREF_FQN )
//                .value( row -> row.getAs( "SearchOfficerID" ) ).ok()
//                .addProperty( OFFICER_ID_FQN )
//                .value( row -> row.getAs( "Search_Officer_ID" ) ).ok()
//                .addProperty( OFFICER_CATEGORY_FQN )
//                .value( row -> "Search" ).ok()
//                .addProperty( FIRSTNAME_FQN )
//                .value( row -> getFirstName( row.getAs( "Search_Officer" ) ) ).ok()
//                .addProperty( LASTNAME_FQN )
//                .value( row -> getLastName( row.getAs( "Search_Officer" ) ) ).ok().ok()
//
//                .addEntity( "releaseofficers" )
//                .ofType( OFFICERS_ENTITY_SET_TYPE )
//                .to( OFFICERS_ENTITY_SET_NAME )
//                .key( OFFICERS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( OFFICER_XREF_FQN )
//                .value( row -> row.getAs( "RelOfficerID" ) ).ok()
//                .addProperty( OFFICER_ID_FQN )
//                .value( row -> row.getAs( "Rel_Officer_ID" ) ).ok()
//                .addProperty( OFFICER_CATEGORY_FQN )
//                .value( row -> "Release" ).ok()
//                .addProperty( FIRSTNAME_FQN )
//                .value( row -> getFirstName( row.getAs( "Rel_Officer" ) ) ).ok()
//                .addProperty( LASTNAME_FQN )
//                .value( row -> getLastName( row.getAs( "Rel_Officer" ) ) ).ok().ok()
//
//                .addEntity( "transportationofficers" )
//                .ofType( OFFICERS_ENTITY_SET_TYPE )
//                .to( OFFICERS_ENTITY_SET_NAME )
//                .key( OFFICERS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( OFFICER_XREF_FQN )
//                .value( row -> row.getAs( "TranspOfficerID" ) ).ok()
//                .addProperty( OFFICER_ID_FQN )
//                .value( row -> row.getAs( "Transp_Officer_ID" ) ).ok()
//                .addProperty( OFFICER_CATEGORY_FQN )
//                .value( row -> "Transport" ).ok()
//                .addProperty( TRANSP_AGENCY_FQN )
//                .value( row -> row.getAs( "Transp_Agency" ) ).ok()
//                .addProperty( FIRSTNAME_FQN )
//                .value( row -> getFirstName( row.getAs( "Transp_Officer" ) ) ).ok()
//                .addProperty( LASTNAME_FQN )
//                .value( row -> getLastName( row.getAs( "Transp_Officer" ) ) ).ok().ok()
//
//                .addEntity( BOOKINGS_ALIAS )
//                .ofType( BOOKINGS_ENTITY_SET_TYPE )
//                .to( BOOKINGS_ENTITY_SET_NAME )
//                .key( BOOKINGS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( JAIL_ID_FQN )
//                .value( row -> row.getAs( "Jail_ID" ) ).ok()
//                .addProperty( ACTUAL_NO_FQN )
//                .value( row -> row.getAs( "Actual_No" ) ).ok()
//                .addProperty( ARREST_NO_FQN )
//                .value( row -> row.getAs( "Arrest_No" ) ).ok()
//                .addProperty( DATE_IN_FQN )
//                .value( row -> fixDate( row.getAs( "Date_In" ) ) ).ok()
//                .addProperty( DATE_OUT_FQN )
//                .value( row -> fixDate( row.getAs( "Date_Out" ) ) ).ok()
//                .addProperty( HOW_RELEASED_FQN )
//                .value( row -> row.getAs( "How_Rel" ) ).ok()
//                .addProperty( EST_REL_DATE_FQN )
//                .value( row -> fixDate( row.getAs( "Est_Rel_Date" ) ) ).ok()
//                .addProperty( ARREST_AGENCY_FQN )
//                .value( row -> row.getAs( "Arrest_Agency" ) ).ok()
//                .addProperty( REASON_CODE_FQN )
//                .value( row -> row.getAs( "ReasonCode" ) ).ok()
//                .addProperty( ARREST_DATE_FQN )
//                .value( row -> fixDate( row.getAs( "Arrest_Date" ) ) ).ok()
//                .addProperty( BOOKED_ID_FQN )
//                .value( row -> row.getAs( "BookedID" ) ).ok()
//                .addProperty( JAIL_RECORD_XREF_FQN )
//                .value( row -> row.getAs( "JailRecordId" ) ).ok().ok()
//
//                .addEntity( JAIL_RECORDS_ALIAS )
//                .ofType( JAIL_RECORDS_ENTITY_SET_TYPE )
//                .to( JAIL_RECORDS_ENTITY_SET_NAME )
//                .key( JAIL_RECORDS_ENTITY_TYPE_KEY_1 )
//
//                .addProperty( JAIL_RECORD_XREF_FQN )
//                .value( row -> row.getAs( "JailRecordId" ) ).ok()
//                .addProperty( CAUTION_FQN )
//                .value( row -> row.getAs( "Caution" ) ).ok()
//                .addProperty( CURRENCY_FQN )
//                .value( row -> row.getAs( "Curr" ) ).ok()
//                .addProperty( CHANGE_FQN )
//                .value( row -> row.getAs( "Change" ) ).ok()
//                .addProperty( CHECKS_FQN )
//                .value( row -> row.getAs( "Checks" ) ).ok()
//                .addProperty( CALL_ATTORNEY_FQN )
//                .value( row -> row.getAs( "Call_Attorney" ) ).ok()
//                .addProperty( RELEASED_TO_FQN )
//                .value( row -> row.getAs( "Released_To" ) ).ok()
//                .addProperty( REMARKS_FQN )
//                .value( row -> row.getAs( "Remarks" ) ).ok()
//                .addProperty( COMIT_AUTH_FQN )
//                .value( row -> row.getAs( "Comit_Auth" ) ).ok()
//                .addProperty( TIME_FRAME_FQN )
//                .value( row -> row.getAs( "Time_Frame" ) ).ok()
//                .addProperty( TOTAL_TIME_FQN )
//                .value( row -> row.getAs( "Total_Time" ) ).ok()
//                .addProperty( HELD_AT_FQN )
//                .value( row -> row.getAs( "Held_At" ) ).ok()
//                .addProperty( ADULT_JUV_WAIVE_FQN )
//                .value( row -> row.getAs( "Adult_Juv_Waive" ) ).ok()
//                .addProperty( JUV_HOLD_AUTH_FQN )
//                .value( row -> row.getAs( "Juv_Hold_Auth" ) ).ok()
//                .addProperty( PERSON_POST_BAIL_FQN )
//                .value( row -> row.getAs( "Person_Post" ) ).ok()
//                .addProperty( INCLUDE_FQN )
//                .value( row -> row.getAs( "Include" ) ).ok()
//                .addProperty( RELEASE_NOTES_FQN )
//                .value( row -> row.getAs( "ReleaseNotes" ) ).ok()
//                .addProperty( ARREST_ID_LONG_FQN )
//                .value( row -> row.getAs( "Arrest_ID" ) ).ok()
//                .addProperty( BALANCE_FQN )
//                .value( row -> row.getAs( "Balance" ) ).ok().ok()
//
//                .ok()
//                .createAssociations()
//
//                .addAssociation( "arrestedby" )
//                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
//                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
//                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
//                .fromEntity( SUBJECTS_ALIAS )
//                .toEntity( "arrestofficers" )
//
//                .addProperty( PERSON_XREF_FQN )
//                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()
//
//                .addAssociation( "searchedby" )
//                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
//                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
//                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
//                .fromEntity( SUBJECTS_ALIAS )
//                .toEntity( "searchofficers" )
//
//                .addProperty( PERSON_XREF_FQN )
//                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()
//
//                .addAssociation( "transportedby" )
//                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
//                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
//                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
//                .fromEntity( SUBJECTS_ALIAS )
//                .toEntity( "transportationofficers" )
//
//                .addProperty( PERSON_XREF_FQN )
//                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()
//
//                .addAssociation( "releasedby" )
//                .ofType( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_TYPE )
//                .to( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_SET_NAME )
//                .key( SUBJECT_INTERACTED_WITH_OFFICER_ENTITY_TYPE_KEY_1 )
//                .fromEntity( SUBJECTS_ALIAS )
//                .toEntity( "releaseofficers" )
//
//                .addProperty( PERSON_XREF_FQN )
//                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()
//
//                .addAssociation( WAS_BOOKED_IN_ENTITY_SET_ALIAS )
//                .ofType( WAS_BOOKED_IN_ENTITY_SET_TYPE )
//                .to( WAS_BOOKED_IN_ENTITY_SET_NAME )
//                .key( WAS_BOOKED_ENTITY_SET_KEY_1 )
//                .fromEntity( SUBJECTS_ALIAS )
//                .toEntity( BOOKINGS_ALIAS )
//
//                .addProperty( PERSON_XREF_FQN )
//                .value( row -> row.getAs( "MNI_No" ) ).ok().ok()
//
//                .addAssociation( DETAILS_OF_IN_ENTITY_SET_ALIAS )
//                .ofType( DETAILS_OF_IN_ENTITY_SET_TYPE )
//                .to( DETAILS_OF_IN_ENTITY_SET_NAME )
//                .key( DETAILS_OF_ENTITY_SET_KEY_1 )
//                .fromEntity( JAIL_RECORDS_ALIAS )
//                .toEntity( BOOKINGS_ALIAS )
//
//                .addProperty( JAIL_RECORD_XREF_FQN )
//                .value( row -> row.getAs( "JailRecordId" ) ).ok().ok()
//
//                .ok()
//                .done();
//    }
}
