package com.dataloom.integrations.iowacity;

import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ALT_START_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.APPEARS_IN_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.APPEARS_IN_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ARREST_AGENCY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOND_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOND_MET_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKINGS_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.BOOKINGS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_DETAILS_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_DETAILS_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_RELEASE_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGE_START_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CHARGING_AGENCY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CONCURRENT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.CONSEC_WITH_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.COURT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.ENTRY_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.EST_REL_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.GTDAYS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.GTHOURS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.GTMINUTES_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.GTPCT_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.HOW_RELEASED_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.INCLUDE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.JAIL_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.LEADS_TO_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.LEADS_TO_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.LOCAL_STATUTE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.NCIC_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.NOTES_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.NO_COUNTS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFENSES_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFENSES_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.OFFENSE_DATE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.PROBATION_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.REASON_HELD_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.RESULTS_IN_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.RESULTS_IN_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SENTENCES_ALIAS;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SENTENCES_NAME;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SENTENCE_DAYS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SENTENCE_HOURS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SEVERITY_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.SEX_OFF_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.STATE_STATUTE_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.STRING_ID_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TIMESERVED_DAYS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TIMESERVED_HOURS_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.TIMESERVED_MINUTES_FQN;
import static com.dataloom.integrations.iowacity.SharedDataModelConsts.WARRANT_NO_FQN;

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

public class JohnsonCountyExtended {
    public static final Environment environment = Environment.LOCAL;
    private static final Logger     logger      = LoggerFactory.getLogger( JohnsonCountyExtended.class );

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

                .addEntity( BOOKINGS_ALIAS )
                .to( BOOKINGS_NAME )
                .useCurrentSync()
                .addProperty( JAIL_ID_FQN, "Jail_ID" )
                .addProperty( HOW_RELEASED_FQN, "How_Rel" )
                .addProperty( EST_REL_DATE_FQN ).value( row -> fixDate( row.getAs( "Exp_Release_Date" ) ) ).ok()
                .addProperty( ARREST_AGENCY_FQN, "Arresting_Agency" )
                .endEntity()

                .addEntity( CHARGE_DETAILS_ALIAS )
                .to( CHARGE_DETAILS_NAME )
                .useCurrentSync()
                .addProperty( CHARGE_ID_FQN, "ID" )
                .addProperty( COURT_FQN, "Court" )
                .addProperty( CHARGE_FQN, "Charge" )
                .addProperty( INCLUDE_FQN, "Include" )
                .addProperty( BOND_MET_FQN, "Bond_Met" )
                .addProperty( BOND_FQN, "Bond" )
                .addProperty( NOTES_FQN, "Notes" )
                .addProperty( CHARGING_AGENCY_FQN, "Charging_Agency" )
                .addProperty( NCIC_FQN, "NCIC" )
                .addProperty( CHARGE_START_DATE_FQN ).value( row -> fixDate( row.getAs( "Start_Date" ) ) ).ok()
                .addProperty( CHARGE_RELEASE_DATE_FQN ).value( row -> fixDate( row.getAs( "Release_Date" ) ) ).ok()
                .addProperty( ALT_START_DATE_FQN ).value( row -> fixDate( row.getAs( "Alt_Start_Date" ) ) ).ok()
                .endEntity()

                .addEntity( OFFENSES_ALIAS )
                .to( OFFENSES_NAME )
                .useCurrentSync()
                .addProperty( OFFENSE_DATE_FQN ).value( row -> fixDate( row.getAs( "Off_Date" ) ) ).ok()
                .addProperty( REASON_HELD_FQN, "ReasonHeld" )
                .addProperty( WARRANT_NO_FQN, "Warr_No" )
                .addProperty( STATE_STATUTE_FQN, "State" )
                .addProperty( LOCAL_STATUTE_FQN, "Local" )
                .addProperty( SEVERITY_FQN, "Severity" )
                .endEntity()

                .addEntity( SENTENCES_ALIAS )
                .to( SENTENCES_NAME )
                .useCurrentSync()
                .addProperty( CONCURRENT_FQN, "Concurrent" )
                .addProperty( SEX_OFF_FQN, "SexOff" )
                .addProperty( TIMESERVED_DAYS_FQN, "TSrvdDays" )
                .addProperty( TIMESERVED_HOURS_FQN, "TSrvdHrs" )
                .addProperty( TIMESERVED_MINUTES_FQN, "TSrvdMins" )
                .addProperty( CONSEC_WITH_FQN, "ConsecWith" )
                .addProperty( NO_COUNTS_FQN, "NoCounts" )
                .addProperty( SENTENCE_DAYS_FQN, "SentenceDays" )
                .addProperty( SENTENCE_HOURS_FQN, "SentenceHrs" )
                .addProperty( GTDAYS_FQN, "GTDays" )
                .addProperty( GTHOURS_FQN, "GTHrs" )
                .addProperty( GTMINUTES_FQN, "GTMins" )
                .addProperty( ENTRY_DATE_FQN ).value( row -> fixDate( row.getAs( "EntryDate" ) ) ).ok()
                .addProperty( GTPCT_FQN, "GTPct" )
                .addProperty( PROBATION_FQN, "Probation" )
                .endEntity()

                .endEntities()

                .createAssociations()

                .addAssociation( APPEARS_IN_ALIAS )
                .to( APPEARS_IN_NAME )
                .fromEntity( CHARGE_DETAILS_ALIAS )
                .toEntity( BOOKINGS_ALIAS )
                .useCurrentSync()
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( RESULTS_IN_ALIAS )
                .to( RESULTS_IN_NAME )
                .fromEntity( OFFENSES_ALIAS )
                .toEntity( CHARGE_DETAILS_ALIAS )
                .useCurrentSync()
                .addProperty( STRING_ID_FQN ).value( row -> UUID.randomUUID().toString() ).ok()
                .endAssociation()

                .addAssociation( LEADS_TO_ALIAS )
                .to( LEADS_TO_NAME )
                .fromEntity( CHARGE_DETAILS_ALIAS )
                .toEntity( SENTENCES_ALIAS )
                .useCurrentSync()
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
        if ( checkme.equals( "0.00" ) || checkme.equals( "_x000C_" ) || checkme.equals( "0" )
                || checkme.equals( "5202" ) || checkme.equals( "CNTM" ) || checkme.equals( "-1" ) ) {
            logger.info( "OMG ITS THAT NUMBER -----------------------------" );
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
