package com.openlattice.integrations.iowacity.dispatchcenter.flights;

import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsDateTime;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsString;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsUUID;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.CASE_ID_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.CASE_NUM_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.DISPATCH_ID_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.PRIORITY_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.TYPE_ID_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.SystemUserBaseFlight.OFFICER_ID_FQN;
import static org.apache.spark.sql.functions.col;

import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.config.JdbcIntegrationConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;

public class DispatchTypeFlight {

    /*
     * PropertyTypes
     */

    public static FullQualifiedName DISPATCH_TYPE_ID_FQN       = new FullQualifiedName( "ICDC.DispatchTypeId" );          // Int64
    public static FullQualifiedName DISPATCH_DATETIME_FQN      = new FullQualifiedName( "ICDC.DispatchDateTime" );        // DateTimeOffset
    public static FullQualifiedName TIMER_CVD_DATETIME_FQN     = new FullQualifiedName( "ICDC.TimerCvdDateTime" );        // DateTimeOffset
    public static FullQualifiedName TIME_COMP_DATETIME_FQN     = new FullQualifiedName( "ICDC.TimeCompDateTime" );        // DateTimeOffset
    public static FullQualifiedName TIME_ARR_DATETIME_FQN      = new FullQualifiedName( "ICDC.TimeArrDateTime" );         // DateTimeOffset
    public static FullQualifiedName TIME_EN_ROUTE_DATETIME_FQN = new FullQualifiedName( "ICDC.TimeEnRouteDateTime" );     // DateTimeOffset
    public static FullQualifiedName UNIT_FQN                   = new FullQualifiedName( "ICDC.Unit" );                    // String, DispatchFlight.ASSIGNED_OFFICER_FQN
    public static FullQualifiedName CFS_OFFICER_ID_FQN         = new FullQualifiedName( "ICDC.CallForServiceOfficerId" ); // Guid
    public static FullQualifiedName DISPOSITION_FQN            = new FullQualifiedName( "ICDC.Disposition" );             // String
    public static FullQualifiedName TRIP_NUM_FQN               = new FullQualifiedName( "ICDC.TripNumber" );              // String

    /*
     * these columns and their values seem to be references to columns in the other data sets
     */

    // public static FullQualifiedName DISPATCH_ID_FQN   = new FullQualifiedName( "ICDC.DispatchId" );   // DispatchFlight.DISPATCH_ID_FQN
    // public static FullQualifiedName CASE_ID_FQN       = new FullQualifiedName( "ICDC.CaseId" );       // DispatchFlight.CASE_ID_FQN
    // public static FullQualifiedName CASE_NUM_FQN      = new FullQualifiedName( "ICDC.CaseNumber" );   // DispatchFlight.CASE_NUM_FQN
    // public static FullQualifiedName TYPE_ID_FQN       = new FullQualifiedName( "ICDC.TypeId" );       // DispatchFlight.TYPE_ID_FQN
    // public static FullQualifiedName OFFICER_ID_FQN    = new FullQualifiedName( "ICDC.OfficerId" );    // DispatchFlight.ASSIGNED_OFFICER_ID_FQN, SystemUserBaseFlight.OFFICER_ID_FQN
    // public static FullQualifiedName TYPE_PRIORITY_FQN = new FullQualifiedName( "ICDC.TypePriority" ); // DispatchFlight.PRIORITY_FQN

    /*
     * sample data points to these columns having all empty / null / 0 values. skipping for now.
     */

    // public static FullQualifiedName BADGE_NUM_FQN        = new FullQualifiedName( "ICDC.BadgeNumber" );
    // public static FullQualifiedName IS_CODE_3_FQN        = new FullQualifiedName( "ICDC.IsCode3" );
    // public static FullQualifiedName DRIVER_FQN           = new FullQualifiedName( "ICDC.Driver" );
    // public static FullQualifiedName DATA_TRANSFERRED_FQN = new FullQualifiedName( "ICDC.DataTransferred" );
    // public static FullQualifiedName RC_TIME_1_FQN        = new FullQualifiedName( "ICDC.RCTime1" );
    // public static FullQualifiedName RC_TIME_2_FQN        = new FullQualifiedName( "ICDC.RCTime2" );
    // public static FullQualifiedName RC_TIME_3_FQN        = new FullQualifiedName( "ICDC.RCTime3" );
    // public static FullQualifiedName RC_TIME_4_FQN        = new FullQualifiedName( "ICDC.RCTime4" );
    // public static FullQualifiedName RC_TIME_5_FQN        = new FullQualifiedName( "ICDC.RCTime5" );
    // public static FullQualifiedName RC_TIME_6_FQN        = new FullQualifiedName( "ICDC.RCTime6" );
    // public static FullQualifiedName RC_TIME_7_FQN        = new FullQualifiedName( "ICDC.RCTime7" );
    // public static FullQualifiedName RC_TIME_8_FQN        = new FullQualifiedName( "ICDC.RCTime8" );
    // public static FullQualifiedName LEG_DISPATCH_ID_FQN  = new FullQualifiedName( "ICDC.LegacyDispatchId" );

    /*
     * EntityTypes
     */

    public static FullQualifiedName DISPATCH_TYPE_ET_FQN = new FullQualifiedName( "ICDC.DispatchType" );

    /*
     * EntitySets
     */

    public static FullQualifiedName DISPATCH_TYPES_ES_FQN   = new FullQualifiedName( "ICDC.DispatchTypes" );
    public static String            DISPATCH_TYPES_ES_ALIAS = DISPATCH_TYPES_ES_FQN.getFullQualifiedNameAsString();
    public static String            DISPATCH_TYPES_ES_NAME  = "IowaCityDispatchCenter_DispatchTypes";

    private static Dataset<Row> getPayloadFromCsv( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        //        String csvPath = Resources.getResource( "dispatch_type.csv" ).getPath();
        java.sql.Date d = new java.sql.Date( DateTime.now().minusDays( 2 ).toDate().getTime() );
        String query = "select * from dbo.Dispatch_Type where timercvd >-= '" + d.toString() +"'";
        Dataset<Row> payload = sparkSession
                .read()
                .format( "jdbc" )
                .option( "url", config.getUrl() )
                .option( "dbtable", query )
                .option( "password", config.getDbPassword() )
                .option( "user", config.getDbUser() )
                .load();

        return payload;
    }

    public static Map<Flight, Dataset<Row>> getFlight( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        Dataset<Row> payload = getPayloadFromCsv( sparkSession, config );

        // 1. what's the difference between "ICDC.OfficerId" and "ICDC.CallForServiceOfficerId"?
        // 2. is "ICDC.Unit" the same as "ICDC.AssignedOfficer"?

        // @formatter:off
        Flight flight = Flight
            .newFlight()
                .createEntities()
                    .addEntity( DISPATCH_TYPES_ES_ALIAS )
                        .to( DISPATCH_TYPES_ES_NAME )
                        .ofType( DISPATCH_TYPE_ET_FQN )
                        .key( DISPATCH_TYPE_ID_FQN, DISPATCH_ID_FQN )
                        .addProperty( DISPATCH_TYPE_ID_FQN ).value( row -> getAsString( row.getAs( "Dispatch_Type_ID" ) ) ).ok()
                        .addProperty( DISPATCH_ID_FQN ).value( row -> getAsString( row.getAs( "Dis_ID" ) ) ).ok()
                        .addProperty( DISPATCH_DATETIME_FQN ).value( row -> getAsDateTime( row.getAs( "TimeDisp" ) ) ).ok()
                        .addProperty( CASE_ID_FQN ).value( row -> getAsUUID( row.getAs( "Case_ID" ) ) ).ok()
                        .addProperty( CASE_NUM_FQN ).value( row -> getAsString( row.getAs( "Case_Num" ) ) ).ok()
                        .addProperty( TIMER_CVD_DATETIME_FQN ).value( row -> getAsDateTime( row.getAs( "Timercvd" ) ) ).ok()
                        .addProperty( TIME_COMP_DATETIME_FQN ).value( row -> getAsDateTime( row.getAs( "TimeComp" ) ) ).ok()
                        .addProperty( TIME_ARR_DATETIME_FQN ).value( row -> getAsDateTime( row.getAs( "TimeArr" ) ) ).ok()
                        .addProperty( TIME_EN_ROUTE_DATETIME_FQN ).value( row -> getAsDateTime( row.getAs( "TimeEnroute" ) ) ).ok()
                        .addProperty( UNIT_FQN ).value( row -> getAsString( row.getAs( "Unit" ) ) ).ok()
                        .addProperty( PRIORITY_FQN ).value( row -> getAsString( row.getAs( "Type_Priority" ) ) ).ok()
                        .addProperty( CFS_OFFICER_ID_FQN ).value( row -> getAsUUID( row.getAs( "CallForServiceOfficerId" ) ) ).ok()
                        .addProperty( DISPOSITION_FQN ).value( row -> getAsString( row.getAs( "Disposition" ) ) ).ok()
                        .addProperty( TRIP_NUM_FQN ).value( row -> getAsString( row.getAs( "TripNumber" ) ) ).ok()
                        .addProperty( TYPE_ID_FQN ).value( row -> getAsString( row.getAs( "Type_ID" ) ) ).ok()
                        .addProperty( OFFICER_ID_FQN ).value( row -> getAsUUID( row.getAs( "OfficerID" ) ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Map<Flight, Dataset<Row>> result = new HashMap<>( 1 );
        result.put( flight, payload );

        return result;
    }
}
