package com.openlattice.integrations.iowacity.dispatchcenter.flights;

import com.google.common.io.Resources;
import com.openlattice.integrations.iowacity.dispatchcenter.IowaCityDispatchCenter;
import com.openlattice.shuttle.Flight;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getDateTimeJanet;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getDispatchDate;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getDispatchTime;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getInteger;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getString;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getUUID;

public class DispatchFlight {

    private static final Logger logger = LoggerFactory.getLogger( DispatchFlight.class );

    /*
     * PropertyTypes
     */

    public static FullQualifiedName DISPATCH_ID_FQN         = new FullQualifiedName( "ICDC.DispatchId" );
    public static FullQualifiedName DISPATCH_NUM_FQN        = new FullQualifiedName( "ICDC.DispatchNumber" );
    public static FullQualifiedName DISPATCH_DATE_FQN       = new FullQualifiedName( "ICDC.DispatchDate" );
    public static FullQualifiedName DISPATCH_TIME_FQN       = new FullQualifiedName( "ICDC.DispatchTime" );
    public static FullQualifiedName DISPATCH_CASE_FQN       = new FullQualifiedName( "ICDC.DispatchCase" );
    public static FullQualifiedName DISPATCH_ZONE_FQN       = new FullQualifiedName( "ICDC.DispatchZone" );
    public static FullQualifiedName DISPATCH_ORI_FQN        = new FullQualifiedName( "ICDC.DispatchORI" );
    public static FullQualifiedName CASE_ID_FQN             = new FullQualifiedName( "ICDC.CaseId" );
    public static FullQualifiedName CASE_NUM_FQN            = new FullQualifiedName( "ICDC.CaseNumber" );
    public static FullQualifiedName OPERATOR_FQN            = new FullQualifiedName( "ICDC.Operator" );
    public static FullQualifiedName HOW_REPORTED_FQN        = new FullQualifiedName( "ICDC.HowReported" );
    public static FullQualifiedName L_NAME_FQN              = new FullQualifiedName( "ICDC.LName" );
    public static FullQualifiedName L_ADDRESS_FQN           = new FullQualifiedName( "ICDC.LAddress" );
    public static FullQualifiedName L_ADDRESS_APT_FQN       = new FullQualifiedName( "ICDC.LAddressApt" );
    public static FullQualifiedName L_CITY_FQN              = new FullQualifiedName( "ICDC.LCity" );
    public static FullQualifiedName L_STATE_FQN             = new FullQualifiedName( "ICDC.LState" );
    public static FullQualifiedName L_ZIP_FQN               = new FullQualifiedName( "ICDC.LZip" );
    public static FullQualifiedName L_PHONE_FQN             = new FullQualifiedName( "ICDC.LPhone" );
    public static FullQualifiedName CALL_NUM_911_FQN        = new FullQualifiedName( "ICDC.CallNumber911" );
    public static FullQualifiedName CLEARED_BY_FQN          = new FullQualifiedName( "ICDC.ClearedBy" );
    public static FullQualifiedName CLEARED_BY_2_FQN        = new FullQualifiedName( "ICDC.ClearedBy2" );
    public static FullQualifiedName LOCATION_FQN            = new FullQualifiedName( "ICDC.Location" );
    public static FullQualifiedName TRANSFER_IR_FQN         = new FullQualifiedName( "ICDC.TransferIR" );
    public static FullQualifiedName N_ADDRESS_FQN           = new FullQualifiedName( "ICDC.NAddress" );
    public static FullQualifiedName N_CITY_FQN              = new FullQualifiedName( "ICDC.NCity" );
    public static FullQualifiedName N_STATE_FQN             = new FullQualifiedName( "ICDC.NState" );
    public static FullQualifiedName N_ZIP_FQN               = new FullQualifiedName( "ICDC.NZip" );
    public static FullQualifiedName N_PHONE_FQN             = new FullQualifiedName( "ICDC.NPhone" );
    public static FullQualifiedName N_CELL_PHONE_FQN        = new FullQualifiedName( "ICDC.NCellPhone" );
    public static FullQualifiedName ASSIGNED_OFFICER_FQN    = new FullQualifiedName( "ICDC.AssignedOfficer" );
    public static FullQualifiedName TYPE_ID_FQN             = new FullQualifiedName( "ICDC.TypeId" );
    public static FullQualifiedName TYPE_CLASS__FQN         = new FullQualifiedName( "ICDC.TypeClass" );
    public static FullQualifiedName MBI_NO_LOC_FQN          = new FullQualifiedName( "ICDC.MBINumberLocation" );
    public static FullQualifiedName PROQA_FQN               = new FullQualifiedName( "ICDC.PROQA" );
    public static FullQualifiedName PROQA_LEVEL_FQN         = new FullQualifiedName( "ICDC.PROQALevel" );
    public static FullQualifiedName PROQA_TYPE_FQN          = new FullQualifiedName( "ICDC.PROQAType" );
    public static FullQualifiedName MOBILE_STATUS_FQN       = new FullQualifiedName( "ICDC.MobileStatus" );
    public static FullQualifiedName LATITUDE_FQN            = new FullQualifiedName( "ICDC.Latitude" );
    public static FullQualifiedName LONGITUDE_FQN           = new FullQualifiedName( "ICDC.Longitude" );
    public static FullQualifiedName ZONE_ID_FQN             = new FullQualifiedName( "ICDC.ZoneId" );
    public static FullQualifiedName SUB_ZONE_FQN            = new FullQualifiedName( "ICDC.SubZone" );
    public static FullQualifiedName NO_SUB_ZONE_OK_FQN      = new FullQualifiedName( "ICDC.NoSubZoneOK" );
    public static FullQualifiedName ESN_FQN                 = new FullQualifiedName( "ICDC.ESN" );
    public static FullQualifiedName FIRE_DISPATCH_LEVEL_FQN = new FullQualifiedName( "ICDC.FireDispatchLevel" );
    public static FullQualifiedName CFS_FIRE_FQN            = new FullQualifiedName( "ICDC.CFS_Fire" );
    public static FullQualifiedName CFS_EMS_FQN             = new FullQualifiedName( "ICDC.CFS_EMS" );
    public static FullQualifiedName CFS_LEA_FQN             = new FullQualifiedName( "ICDC.CFS_LEA" );
    public static FullQualifiedName INCIDENT_M_ADDR_ID_FQN  = new FullQualifiedName( "ICDC.IncidentMasterAddressId" );
    public static FullQualifiedName FIRE_DISTRICT_FQN       = new FullQualifiedName( "ICDC.FireDistrict" );
    public static FullQualifiedName LINKED_LEA_FQN          = new FullQualifiedName( "ICDC.LinkedLEA" );
    public static FullQualifiedName CALL_FOR_SERVICE_ID_FQN = new FullQualifiedName( "ICDC.CallForServiceId" );
    public static FullQualifiedName ASSIGNED_OFFICER_ID_FQN = new FullQualifiedName( "ICDC.AssignedOfficerId" );
    public static FullQualifiedName PRIORITY_FQN            = new FullQualifiedName( "ICDC.Priority" );
    public static FullQualifiedName CFS_DATE_TIME_JANET_FQN = new FullQualifiedName( "ICDC.CFSDateTimeJanet" );
    public static FullQualifiedName ALERTED_TIME_FQN        = new FullQualifiedName( "ICDC.AlertedTime" );
    public static FullQualifiedName PARENT_DISPATCH_ID_FQN  = new FullQualifiedName( "ICDC.ParentDispatchId" );
    public static FullQualifiedName MEDICAL_ZONE_FQN        = new FullQualifiedName( "ICDC.MedicalZone" );
    public static FullQualifiedName UPSIZE_TS_FQN           = new FullQualifiedName( "ICDC.UpsizeTs" );

    /*
     * sample data points to these columns having all empty / null / 0 values. skipping for now.
     */

    // public static FullQualifiedName DETAILS_FQN                 = new FullQualifiedName( "ICDC.Details" );
    // public static FullQualifiedName MBI_NUM_FQN                 = new FullQualifiedName( "ICDC.MBINumber" );
    // public static FullQualifiedName MNI_NUM_FQN                 = new FullQualifiedName( "ICDC.MNINumber" );
    // public static FullQualifiedName MVI_NUM_FQN                 = new FullQualifiedName( "ICDC.MVINumber" );
    // public static FullQualifiedName DL_NUM_FQN                  = new FullQualifiedName( "ICDC.DLNumber" );
    // public static FullQualifiedName DL_STATE_FQN                = new FullQualifiedName( "ICDC.DLState" );
    // public static FullQualifiedName MAKE_FQN                    = new FullQualifiedName( "ICDC.Make" );
    // public static FullQualifiedName MODEL_FQN                   = new FullQualifiedName( "ICDC.Model" );
    // public static FullQualifiedName SEC_CASE_NUM_FQN            = new FullQualifiedName( "ICDC.SecCaseNumber" );
    // public static FullQualifiedName SEC_ORI_FQN                 = new FullQualifiedName( "ICDC.SecORI" );
    // public static FullQualifiedName MNI_NUM_LOC_FQN             = new FullQualifiedName( "ICDC.MNINumberLocation" );
    // public static FullQualifiedName S_DL_NUM_FQN                = new FullQualifiedName( "ICDC.sDLNumber" );
    // public static FullQualifiedName S_DL_STATE_FQN              = new FullQualifiedName( "ICDC.sDLState" );
    // public static FullQualifiedName VIN_FQN                     = new FullQualifiedName( "ICDC.VIN" );
    // public static FullQualifiedName N_FIRST_FQN                 = new FullQualifiedName( "ICDC.NFirst" );
    // public static FullQualifiedName N_MIDDLE_FQN                = new FullQualifiedName( "ICDC.NMiddle" );
    // public static FullQualifiedName N_LAST_FQN                  = new FullQualifiedName( "ICDC.NLast" );
    // public static FullQualifiedName N_DOB_FQN                   = new FullQualifiedName( "ICDC.NDOB" );
    // public static FullQualifiedName N_ADDRESS_APT_FQN           = new FullQualifiedName( "ICDC.NAddressApt" );
    // public static FullQualifiedName LOCK_DETAILS_FQN            = new FullQualifiedName( "ICDC.LockDetails" );
    // public static FullQualifiedName NCIC_CODE_FQN               = new FullQualifiedName( "ICDC.NCICCode" );
    // public static FullQualifiedName OPERATOR_CALL_TAKER_FQN     = new FullQualifiedName( "ICDC.OperatorCallTaker" );
    // public static FullQualifiedName IS_LOCKED_FQN               = new FullQualifiedName( "ICDC.IsLocked" );
    // public static FullQualifiedName CROSS_STREET_FQN            = new FullQualifiedName( "ICDC.CrossStreet" );
    // public static FullQualifiedName IS_FALSE_ALARM_FQN          = new FullQualifiedName( "ICDC.IsFalseAlarm" );
    // public static FullQualifiedName STACKER_LINK_ID_FQN         = new FullQualifiedName( "ICDC.StackerLinkId" );
    // public static FullQualifiedName MVA_INVESTIGATION_FQN       = new FullQualifiedName( "ICDC.MVAInvestigation" );
    // public static FullQualifiedName MVA_HANDLING_AGENCY_ORI_FQN = new FullQualifiedName( "ICDC.MVAHandlingAgencyORI" );
    // public static FullQualifiedName FIRE_ZONE_FQN               = new FullQualifiedName( "ICDC.FireZone" );
    // public static FullQualifiedName TMP_MBI_NUM_FQN             = new FullQualifiedName( "ICDC.TempMBINumber" );
    // public static FullQualifiedName TMP_MNI_NUM_FQN             = new FullQualifiedName( "ICDC.TempMNINumber" );
    // public static FullQualifiedName TMP_MVI_NUM_FQN             = new FullQualifiedName( "ICDC.TempMVINumber" );
    // public static FullQualifiedName TMP_MBI_NUM_LOC_FQN         = new FullQualifiedName( "ICDC.TempMBINumberLocation" );
    // public static FullQualifiedName TMP_MNI_NUM_LOC_FQN         = new FullQualifiedName( "ICDC.TempMNINumberLocation" );
    // public static FullQualifiedName SCHEDULED_DATE_TIME_FQN     = new FullQualifiedName( "ICDC.ScheduledDateTime" );
    // public static FullQualifiedName LEG_DISPATCH_ID_FQN         = new FullQualifiedName( "ICDC.LegacyDispatchId" );
    // public static FullQualifiedName CONV_DISPATCH_ID_FQN        = new FullQualifiedName( "ICDC.ConvDispatchId" );
    // public static FullQualifiedName CUSTOM_LIST_1_FQN           = new FullQualifiedName( "ICDC.CustomList1" );
    // public static FullQualifiedName CUSTOM_LIST_2_FQN           = new FullQualifiedName( "ICDC.CustomList2" );
    // public static FullQualifiedName CUSTOM_LIST_3_FQN           = new FullQualifiedName( "ICDC.CustomList3" );
    // public static FullQualifiedName IS_CS_CLEARED_FQN           = new FullQualifiedName( "ICDC.IsCallStackCleared" );
    // public static FullQualifiedName IS_CS_CAUTION_FQN           = new FullQualifiedName( "ICDC.IsCallStackCaution" );
    // public static FullQualifiedName IS_CS_ATTENTION_FQN         = new FullQualifiedName( "ICDC.IsCallStackAttention" );

    /*
     * EntityTypes
     */

    public static FullQualifiedName DISPATCH_ET_FQN = new FullQualifiedName( "ICDC.Dispatch" );

    /*
     * EntitySets
     */

    public static FullQualifiedName DISPATCHES_ES_FQN   = new FullQualifiedName( "ICDC.Dispatches" );
    public static String            DISPATCHES_ES_ALIAS = DISPATCHES_ES_FQN.getFullQualifiedNameAsString();
    public static String            DISPATCHES_ES_NAME  = "IowaCityDispatchCenter_Dispatches";

    private static Dataset<Row> getPayloadFromCsv( final SparkSession sparkSession ) {

        String csvPath = Resources.getResource( "dispatch.csv" ).getPath();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( csvPath );

        return payload;
    }

    public static Map<Flight, Dataset<Row>> getFlight( final SparkSession sparkSession ) {

        Dataset<Row> payload = getPayloadFromCsv( sparkSession );

        // @formatter:off
        Flight flight = Flight
            .newFlight()
                .createEntities()
                    .addEntity( DISPATCHES_ES_ALIAS )
                        .to( DISPATCHES_ES_NAME )
                        .ofType( DISPATCH_ET_FQN )
                        .key( DISPATCH_ID_FQN, DISPATCH_NUM_FQN )
                        .addProperty( DISPATCH_ID_FQN ).value( row -> getInteger( row.getAs( "Dis_ID" ) ) ).ok()
                        .addProperty( DISPATCH_NUM_FQN ).value( row -> getInteger( row.getAs( "Dis_No" ) ) ).ok()
                        .addProperty( DISPATCH_DATE_FQN ).value( row -> getDispatchDate( row.getAs( "Dis_Date" ) ) ).ok()
                        .addProperty( DISPATCH_TIME_FQN ).value( row -> getDispatchTime( row.getAs( "DIS_TIME" ) ) ).ok()
                        .addProperty( DISPATCH_CASE_FQN ).value( row -> getString( row.getAs( "Dis_Case" ) ) ).ok()
                        .addProperty( DISPATCH_ZONE_FQN ).value( row -> getString( row.getAs( "Dis_Zone" ) ) ).ok()
                        .addProperty( DISPATCH_ORI_FQN ).value( row -> getString( row.getAs( "Dis_ORI" ) ) ).ok()
                        .addProperty( CASE_ID_FQN ).value( row -> getUUID( row.getAs( "Case_ID" ) ) ).ok()
                        .addProperty( CASE_NUM_FQN ).value( row -> getString( row.getAs( "Case_Number" ) ) ).ok()
                        .addProperty( OPERATOR_FQN ).value( row -> getString( row.getAs( "Operator" ) ) ).ok()
                        .addProperty( HOW_REPORTED_FQN ).value( row -> getString( row.getAs( "HowReported" ) ) ).ok()
                        .addProperty( L_NAME_FQN ).value( row -> getString( row.getAs( "LName" ) ) ).ok()
                        .addProperty( L_ADDRESS_FQN ).value( row -> getString( row.getAs( "LAddress" ) ) ).ok()
                        .addProperty( L_ADDRESS_APT_FQN ).value( row -> getString( row.getAs( "LAddress_Apt" ) ) ).ok()
                        .addProperty( L_CITY_FQN ).value( row -> getString( row.getAs( "LCity" ) ) ).ok()
                        .addProperty( L_STATE_FQN ).value( row -> getString( row.getAs( "LState" ) ) ).ok()
                        .addProperty( L_ZIP_FQN ).value( row -> getInteger( row.getAs( "LZip" ) ) ).ok()
                        .addProperty( L_PHONE_FQN ).value( row -> getString( row.getAs( "LPhone" ) ) ).ok()
                        .addProperty( CALL_NUM_911_FQN ).value( row -> getString( row.getAs( "CallNumber_911" ) ) ).ok()
                        .addProperty( CLEARED_BY_FQN ).value( row -> getString( row.getAs( "ClearedBy" ) ) ).ok()
                        .addProperty( CLEARED_BY_2_FQN ).value( row -> getString( row.getAs( "ClearedBy2" ) ) ).ok()
                        .addProperty( LOCATION_FQN ).value( row -> getString( row.getAs( "Location" ) ) ).ok()
                        .addProperty( TRANSFER_IR_FQN ).value( row -> getString( row.getAs( "TransferIR" ) ) ).ok()
                        .addProperty( N_ADDRESS_FQN ).value( row -> getString( row.getAs( "NAddress" ) ) ).ok()
                        .addProperty( N_CITY_FQN ).value( row -> getString( row.getAs( "NCity" ) ) ).ok()
                        .addProperty( N_STATE_FQN ).value( row -> getString( row.getAs( "NState" ) ) ).ok()
                        .addProperty( N_ZIP_FQN ).value( row -> getString( row.getAs( "NZip" ) ) ).ok()
                        .addProperty( N_PHONE_FQN ).value( row -> getString( row.getAs( "NPhone" ) ) ).ok()
                        .addProperty( N_CELL_PHONE_FQN ).value( row -> getString( row.getAs( "NCellPhone" ) ) ).ok()
                        .addProperty( ASSIGNED_OFFICER_FQN ).value( row -> getString( row.getAs( "ASSIGNED_OFFICER" ) ) ).ok()
                        .addProperty( TYPE_ID_FQN ).value( row -> getString( row.getAs( "TYPE_ID" ) ) ).ok()
                        .addProperty( TYPE_CLASS__FQN ).value( row -> getString( row.getAs( "TYPE_CLASS" ) ) ).ok()
                        .addProperty( MBI_NO_LOC_FQN ).value( row -> getString( row.getAs( "MBI_No_Loc" ) ) ).ok()
                        .addProperty( PROQA_FQN ).value( row -> getString( row.getAs( "PROQA" ) ) ).ok()
                        .addProperty( PROQA_LEVEL_FQN ).value( row -> getString( row.getAs( "PROQA_LEVEL" ) ) ).ok()
                        .addProperty( PROQA_TYPE_FQN ).value( row -> getString( row.getAs( "PROQA_TYPE" ) ) ).ok()
                        .addProperty( MOBILE_STATUS_FQN ).value( row -> getString( row.getAs( "Mobile_Status" ) ) ).ok()
                        .addProperty( LATITUDE_FQN ).value( row -> getString( row.getAs( "Latitude" ) ) ).ok()
                        .addProperty( LONGITUDE_FQN ).value( row -> getString( row.getAs( "Longitude" ) ) ).ok()
                        .addProperty( ZONE_ID_FQN ).value( row -> getString( row.getAs( "ZONE_ID" ) ) ).ok()
                        .addProperty( SUB_ZONE_FQN ).value( row -> getString( row.getAs( "SubZone" ) ) ).ok()
                        .addProperty( NO_SUB_ZONE_OK_FQN ).value( row -> getString( row.getAs( "NoSubZoneOK" ) ) ).ok()
                        .addProperty( ESN_FQN ).value( row -> getString( row.getAs( "ESN" ) ) ).ok()
                        .addProperty( FIRE_DISPATCH_LEVEL_FQN ).value( row -> getString( row.getAs( "FireDispatchLevel" ) ) ).ok()
                        .addProperty( CFS_FIRE_FQN ).value( row -> getString( row.getAs( "CFS_Fire" ) ) ).ok()
                        .addProperty( CFS_EMS_FQN ).value( row -> getString( row.getAs( "CFS_EMS" ) ) ).ok()
                        .addProperty( CFS_LEA_FQN ).value( row -> getString( row.getAs( "CFS_LEA" ) ) ).ok()
                        .addProperty( INCIDENT_M_ADDR_ID_FQN ).value( row -> getString( row.getAs( "IncidentMasterAddressID" ) ) ).ok()
                        .addProperty( FIRE_DISTRICT_FQN ).value( row -> getString( row.getAs( "FireDistrict" ) ) ).ok()
                        .addProperty( LINKED_LEA_FQN ).value( row -> getString( row.getAs( "LinkedLEA" ) ) ).ok()
                        .addProperty( CALL_FOR_SERVICE_ID_FQN ).value( row -> getString( row.getAs( "CallForServiceID" ) ) ).ok()
                        .addProperty( ASSIGNED_OFFICER_ID_FQN ).value( row -> getString( row.getAs( "AssignedOfficerID" ) ) ).ok()
                        .addProperty( PRIORITY_FQN ).value( row -> getString( row.getAs( "Priority" ) ) ).ok()
                        .addProperty( CFS_DATE_TIME_JANET_FQN ).value( row -> getDateTimeJanet( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                        .addProperty( ALERTED_TIME_FQN ).value( row -> getString( row.getAs( "AlertedTime" ) ) ).ok()
                        .addProperty( PARENT_DISPATCH_ID_FQN ).value( row -> getString( row.getAs( "ParentDis_Id" ) ) ).ok()
                        .addProperty( MEDICAL_ZONE_FQN ).value( row -> getString( row.getAs( "Medical_Zone" ) ) ).ok()
                        .addProperty( UPSIZE_TS_FQN ).value( row -> getString( row.getAs( "upsize_ts" ) ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Map<Flight, Dataset<Row>> result = new HashMap<>( 1 );
        result.put( flight, payload );

        return result;
    }
}
