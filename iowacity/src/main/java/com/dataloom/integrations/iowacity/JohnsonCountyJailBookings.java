package com.dataloom.integrations.iowacity;

import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ACTUAL_NO_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ADULT_JUV_WAIVE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ALIAS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ARREST_AGENCY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ARREST_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ARREST_ID_LONG_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ARREST_NO_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BALANCE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKED_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKED_IN_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKED_IN_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKINGS_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKINGS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CALL_ATTORNEY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CAUTION_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHANGE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHECKS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.COMIT_AUTH_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CURRENCY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.DATE_IN_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.DATE_OUT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.DETAILS_OF_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.DETAILS_OF_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.DOB_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.EST_REL_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.EYES_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.FIRSTNAME_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.HAIR_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.HEIGHT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.HELD_AT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.HOW_RELEASED_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.INCLUDE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.INTERACTED_WITH_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JAIL_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JAIL_RECORDS_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JAIL_RECORDS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JAIL_RECORD_XREF_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JUV_HOLD_AUTH_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.LASTNAME_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.MIDDLENAME_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFICERS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFICER_CATEGORY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFICER_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFICER_XREF_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.PERSON_POST_BAIL_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.PERSON_XREF_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.RACE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.REASON_CODE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.RELEASED_TO_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.RELEASE_NOTES_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.REMARKS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SEX_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.STRING_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SUBJECTS_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SUBJECTS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SUBJECT_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TIME_FRAME_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TOTAL_TIME_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TRANSP_AGENCY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.WEIGHT_FQN;

import java.util.Map;
import java.util.UUID;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.spark_project.guava.collect.Maps;

import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.mappers.ObjectMappers;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;

public class JohnsonCountyJailBookings {
    // Logger is used to output useful debugging messages to the console
    private static final Logger     logger      = LoggerFactory.getLogger( JohnsonCountyJailBookings.class );
    public static final Environment environment = Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException {
        if ( args.length < 3 ) {
            System.out.println( "expected: <path> <jwtToken>" );
            return;
        }
        final String path = args[ 1 ];
        final String jwtToken = args[ 2 ];

        final SparkSession sparkSession = MissionControl.getSparkSession();
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

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

                .addEntity( SUBJECTS_ALIAS )
                .to( SUBJECTS_NAME )
                // .useCurrentSync()
                .addProperty( SUBJECT_ID_FQN, "MNI_No" )
                .addProperty( PERSON_XREF_FQN, "MNI_No" )
                .addProperty( FIRSTNAME_FQN ).value( row -> getFirstName( row.getAs( "JName" ) ) ).ok()
                .addProperty( MIDDLENAME_FQN ).value( row -> getMiddleName( row.getAs( "JName" ) ) ).ok()
                .addProperty( LASTNAME_FQN ).value( row -> getLastName( row.getAs( "JName" ) ) ).ok()
                .addProperty( ALIAS_FQN, "Alias" )
                .addProperty( DOB_FQN ).value( row -> fixDate( row.getAs( "DOB" ) ) ).ok()
                .addProperty( SEX_FQN, "Sex" )
                .addProperty( RACE_FQN, "Race" )
                .addProperty( HEIGHT_FQN, "OHeight" )
                .addProperty( WEIGHT_FQN, "OWeight" )
                .addProperty( EYES_FQN, "OEyes" )
                .addProperty( HAIR_FQN, "OHair" )
                .endEntity()

                .addEntity( "arrestofficers" )
                .to( OFFICERS_NAME )
                .addProperty( OFFICER_XREF_FQN, "ArrestOfficerID" )
                .addProperty( OFFICER_ID_FQN, "AO_ID" )
                .addProperty( OFFICER_CATEGORY_FQN ).value( row -> "Arrest" ).ok()
                .addProperty( FIRSTNAME_FQN ).value( row -> getFirstName( row.getAs( "AO" ) ) ).ok()
                .addProperty( LASTNAME_FQN ).value( row -> getLastName( row.getAs( "AO" ) ) ).ok()
                .endEntity()

                .addEntity( "searchofficers" )
                .to( OFFICERS_NAME )
                .addProperty( OFFICER_XREF_FQN, "SearchOfficerID" )
                .addProperty( OFFICER_ID_FQN, "Search_Officer_ID" )
                .addProperty( OFFICER_CATEGORY_FQN ).value( row -> "Search" ).ok()
                .addProperty( FIRSTNAME_FQN ).value( row -> getFirstName( row.getAs( "Search_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN ).value( row -> getLastName( row.getAs( "Search_Officer" ) ) ).ok()
                .endEntity()

                .addEntity( "releaseofficers" )
                .to( OFFICERS_NAME )
                .addProperty( OFFICER_XREF_FQN, "RelOfficerID" )
                .addProperty( OFFICER_ID_FQN, "Rel_Officer_ID" )
                .addProperty( OFFICER_CATEGORY_FQN ).value( row -> "Release" ).ok()
                .addProperty( FIRSTNAME_FQN ).value( row -> getFirstName( row.getAs( "Rel_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN ).value( row -> getLastName( row.getAs( "Rel_Officer" ) ) ).ok()
                .endEntity()

                .addEntity( "transportationofficers" )
                .to( OFFICERS_NAME )
                .addProperty( OFFICER_XREF_FQN, "TranspOfficerID" )
                .addProperty( OFFICER_ID_FQN, "Transp_Officer_ID" )
                .addProperty( OFFICER_CATEGORY_FQN ).value( row -> "Transport" ).ok()
                .addProperty( TRANSP_AGENCY_FQN ).value( row -> row.getAs( "Transp_Agency" ) ).ok()
                .addProperty( FIRSTNAME_FQN ).value( row -> getFirstName( row.getAs( "Transp_Officer" ) ) ).ok()
                .addProperty( LASTNAME_FQN ).value( row -> getLastName( row.getAs( "Transp_Officer" ) ) ).ok()
                .endEntity()

                .addEntity( BOOKINGS_ALIAS )
                .to( BOOKINGS_NAME )
                .addProperty( JAIL_ID_FQN, "Jail_ID" )
                .addProperty( ACTUAL_NO_FQN, "Actual_No" )
                .addProperty( ARREST_NO_FQN, "Arrest_No" )
                .addProperty( DATE_IN_FQN ).value( row -> fixDate( row.getAs( "Date_In" ) ) ).ok()
                .addProperty( DATE_OUT_FQN ).value( row -> fixDate( row.getAs( "Date_Out" ) ) ).ok()
                .addProperty( HOW_RELEASED_FQN, "How_Rel" )
                .addProperty( EST_REL_DATE_FQN ).value( row -> fixDate( row.getAs( "Est_Rel_Date" ) ) ).ok()
                .addProperty( ARREST_AGENCY_FQN, "Arrest_Agency" )
                .addProperty( REASON_CODE_FQN, "ReasonCode" )
                .addProperty( ARREST_DATE_FQN ).value( row -> fixDate( row.getAs( "Arrest_Date" ) ) ).ok()
                .addProperty( BOOKED_ID_FQN, "BookedID" )
                .addProperty( JAIL_RECORD_XREF_FQN, "JailRecordId" )
                .endEntity()

                .addEntity( JAIL_RECORDS_ALIAS )
                .to( JAIL_RECORDS_NAME )
                .addProperty( JAIL_RECORD_XREF_FQN, "JailRecordId" )
                .addProperty( CAUTION_FQN, "Caution" )
                .addProperty( CURRENCY_FQN, "Curr" )
                .addProperty( CHANGE_FQN, "Change" )
                .addProperty( CHECKS_FQN, "Checks" )
                .addProperty( CALL_ATTORNEY_FQN, "Call_Attorney" )
                .addProperty( RELEASED_TO_FQN, "Released_To" )
                .addProperty( REMARKS_FQN, "Remarks" )
                .addProperty( COMIT_AUTH_FQN, "Comit_Auth" )
                .addProperty( TIME_FRAME_FQN, "Time_Frame" )
                .addProperty( TOTAL_TIME_FQN, "Total_Time" )
                .addProperty( HELD_AT_FQN, "Held_At" )
                .addProperty( ADULT_JUV_WAIVE_FQN, "Adult_Juv_Waive" )
                .addProperty( JUV_HOLD_AUTH_FQN, "Juv_Hold_Auth" )
                .addProperty( PERSON_POST_BAIL_FQN, "Person_Post" )
                .addProperty( INCLUDE_FQN, "Include" )
                .addProperty( RELEASE_NOTES_FQN, "ReleaseNotes" )
                .addProperty( ARREST_ID_LONG_FQN, "Arrest_ID" )
                .addProperty( BALANCE_FQN, "Balance" )
                .endEntity()

                .endEntities()

                .createAssociations()

                .addAssociation( "arrestedby" )
                .to( INTERACTED_WITH_NAME )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "arrestofficers" )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( "searchedby" )
                .to( INTERACTED_WITH_NAME )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "searchofficers" )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( "transportedby" )
                .to( INTERACTED_WITH_NAME )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "transportationofficers" )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( "releasedby" )
                .to( INTERACTED_WITH_NAME )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( "releaseofficers" )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( BOOKED_IN_ALIAS )
                .to( BOOKED_IN_NAME )
                .fromEntity( SUBJECTS_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( DETAILS_OF_ALIAS )
                .to( DETAILS_OF_NAME )
                .fromEntity( JAIL_RECORDS_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .endAssociations()
                .done();

        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
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
