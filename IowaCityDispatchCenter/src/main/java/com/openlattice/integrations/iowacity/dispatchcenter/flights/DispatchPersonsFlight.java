package com.openlattice.integrations.iowacity.dispatchcenter.flights;

import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsBoolean;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsDate;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsString;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsUUID;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.DISPATCH_ID_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight.UPSIZE_TS_FQN;
import static com.openlattice.integrations.iowacity.dispatchcenter.flights.SystemUserBaseFlight.OFFICER_ID_FQN;

import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.config.JdbcIntegrationConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DispatchPersonsFlight {

    /*
     * PropertyTypes
     */

    private static final Logger            logger                   = LoggerFactory
            .getLogger( DispatchPersonsFlight.class );
    public static FullQualifiedName DISPATCH_PERSON_ID_FQN = new FullQualifiedName( "ICDC.DispatchPersonId" );       // Int64
    public static FullQualifiedName DOB_FQN                = new FullQualifiedName( "ICDC.DOB" );                    // Date
    public static FullQualifiedName OSQ_FQN                = new FullQualifiedName( "ICDC.OSQ" );                    // Int16
    public static FullQualifiedName O_NAME_FQN             = new FullQualifiedName( "ICDC.OName" );                  // String
    public static FullQualifiedName O_ADDRESS_FQN          = new FullQualifiedName( "ICDC.OAddress" );               // String
    public static FullQualifiedName O_ADDRESS_APT_FQN      = new FullQualifiedName( "ICDC.OAddressApt" );            // String
    public static FullQualifiedName O_CITY_FQN             = new FullQualifiedName( "ICDC.OCity" );                  // String
    public static FullQualifiedName O_STATE_FQN            = new FullQualifiedName( "ICDC.OState" );                 // String
    public static FullQualifiedName O_ZIP_FQN              = new FullQualifiedName( "ICDC.OZip" );                   // String
    public static FullQualifiedName O_PHONE_FQN            = new FullQualifiedName( "ICDC.OPhone" );                 // String
    public static FullQualifiedName O_SEX_FQN              = new FullQualifiedName( "ICDC.OSex" );                   // String
    public static FullQualifiedName O_RACE_FQN             = new FullQualifiedName( "ICDC.ORace" );                  // String
    public static FullQualifiedName TYPE_FQN               = new FullQualifiedName( "ICDC.Type" );                   // Int16
    public static FullQualifiedName VEHICLE_MAKE_FQN       = new FullQualifiedName( "ICDC.VehicleMake" );            // String
    public static FullQualifiedName VEHICLE_MODEL_FQN      = new FullQualifiedName( "ICDC.VehicleModel" );           // String
    public static FullQualifiedName VEHICLE_YEAR_FQN       = new FullQualifiedName( "ICDC.VehicleYear" );            // String
    public static FullQualifiedName VEHICLE_VIN_FQN        = new FullQualifiedName( "ICDC.VehicleVIN" );             // String
    public static FullQualifiedName VEHICLE_COLOR_FQN      = new FullQualifiedName( "ICDC.VehicleColor" );           // String
    public static FullQualifiedName VEHICLE_STYLE_FQN      = new FullQualifiedName( "ICDC.VehicleStyle" );           // String
    public static FullQualifiedName LIC_FQN                = new FullQualifiedName( "ICDC.LIC" );                    // String
    public static FullQualifiedName LIS_FQN                = new FullQualifiedName( "ICDC.LIS" );                    // String
    public static FullQualifiedName NCIC_FQN               = new FullQualifiedName( "ICDC.NCIC" );                   // Int16
    public static FullQualifiedName CELL_PHONE_FQN         = new FullQualifiedName( "ICDC.CellPhone" );              // String
    public static FullQualifiedName CFS_PERSON_ID_FQN      = new FullQualifiedName( "ICDC.CallForServicePersonId" ); // Guid
    public static FullQualifiedName BADGE_NUM_FQN          = new FullQualifiedName( "ICDC.BadgeNumber" );            // String
    public static FullQualifiedName AGE_FQN                = new FullQualifiedName( "ICDC.Age" );                    // Int16
    public static FullQualifiedName HEIGHT_FQN             = new FullQualifiedName( "ICDC.Height" );                 // String
    public static FullQualifiedName WEIGHT_FQN             = new FullQualifiedName( "ICDC.Weight" );                 // String
    public static FullQualifiedName EYES_FQN               = new FullQualifiedName( "ICDC.Eyes" );                   // String
    public static FullQualifiedName HAIR_FQN               = new FullQualifiedName( "ICDC.Hair" );                   // String
    public static FullQualifiedName ETHNICITY_FQN          = new FullQualifiedName( "ICDC.Ethnicity" );              // String
    public static FullQualifiedName ADDITIONAL_FQN         = new FullQualifiedName( "ICDC.Additional" );             // String
    public static FullQualifiedName TRANSFER_VEHICLE_FQN   = new FullQualifiedName( "ICDC.TransferVehicle" );        // Boolean
    public static FullQualifiedName JUV_FQN                = new FullQualifiedName( "ICDC.Juv" );                    // Boolean
    public static FullQualifiedName LIT_FQN                = new FullQualifiedName( "ICDC.LIT" );                    // String
    public static FullQualifiedName LIY_FQN                = new FullQualifiedName( "ICDC.LIY" );                    // String
    public static FullQualifiedName MNI_NUM_FQN            = new FullQualifiedName( "ICDC.MNINumber" );              // String

    /*
     * these columns and their values seem to be references to columns in the other data sets
     */

    // public static FullQualifiedName DISPATCH_ID_FQN = new FullQualifiedName( "ICDC.DispatchId" ); // DispatchFlight.DISPATCH_ID_FQN
    // public static FullQualifiedName OFFICER_ID_FQN  = new FullQualifiedName( "ICDC.OfficerId" );  // DispatchFlight.ASSIGNED_OFFICER_ID_FQN, SystemUserBaseFlight.OFFICER_ID_FQN
    // public static FullQualifiedName UPSIZE_TS_FQN   = new FullQualifiedName( "ICDC.UpsizeTs" );   // DispatchFlight.UPSIZE_TS_FQN

    /*
     * sample data points to these columns having all empty / null / 0 values. skipping for now.
     */

    // public static FullQualifiedName SSN_FQN             = new FullQualifiedName( "ICDC.SSN" );
    // public static FullQualifiedName DL_NUM_FQN          = new FullQualifiedName( "ICDC.DLNumber" );
    // public static FullQualifiedName DL_STATE_FQN        = new FullQualifiedName( "ICDC.DLState" );
    // public static FullQualifiedName ARREST_FQN          = new FullQualifiedName( "ICDC.Arrest" );
    // public static FullQualifiedName FIELD_STOP_ID_FQN   = new FullQualifiedName( "ICDC.FieldStopId" );
    // public static FullQualifiedName CITATION_ID_FQN     = new FullQualifiedName( "ICDC.CitationId" );
    // public static FullQualifiedName PASSENGERS_FQN      = new FullQualifiedName( "ICDC.Passengers" );
    // public static FullQualifiedName TEMP_MNI_NUM_FQN    = new FullQualifiedName( "ICDC.TempMNINumber" );
    // public static FullQualifiedName TEMP_MBI_ID_FQN     = new FullQualifiedName( "ICDC.TempMBIId" );
    // public static FullQualifiedName TEMP_MVI_NUM_FQN    = new FullQualifiedName( "ICDC.TempMVINumber" );
    // public static FullQualifiedName SHOES_FQN           = new FullQualifiedName( "ICDC.Shoes" );
    // public static FullQualifiedName HAT_FQN             = new FullQualifiedName( "ICDC.Hat" );
    // public static FullQualifiedName FACIAL_FQN          = new FullQualifiedName( "ICDC.Facial" );
    // public static FullQualifiedName WEAPON_FQN          = new FullQualifiedName( "ICDC.Weapon" );
    // public static FullQualifiedName JACKET_FQN          = new FullQualifiedName( "ICDC.Jacket" );
    // public static FullQualifiedName GLASSES_FQN         = new FullQualifiedName( "ICDC.Glasses" );
    // public static FullQualifiedName BUILD_FQN           = new FullQualifiedName( "ICDC.Build" );
    // public static FullQualifiedName SHIRT_FQN           = new FullQualifiedName( "ICDC.Shirt" );
    // public static FullQualifiedName PANTS_FQN           = new FullQualifiedName( "ICDC.Pants" );
    // public static FullQualifiedName FLIGHT_DIR_FQN      = new FullQualifiedName( "ICDC.FlightDir" );
    // public static FullQualifiedName LEG_DISPATCH_ID_FQN = new FullQualifiedName( "ICDC.LegacyDispatchId" );
    // public static FullQualifiedName COLOR_SECONDARY_FQN = new FullQualifiedName( "ICDC.ColorSecondary" );
    // public static FullQualifiedName MBI_ID_FQN          = new FullQualifiedName( "ICDC.MBIId" );

    /*
     * EntityTypes
     */
    public static FullQualifiedName MVI_NUM_FQN            = new FullQualifiedName( "ICDC.MVINumber" );              // String

    /*
     * EntitySets
     */
    public static FullQualifiedName DISPATCH_PERSON_ET_FQN = new FullQualifiedName( "ICDC.DispatchPerson" );
    public static        FullQualifiedName DISPATCH_PERSON_ES_FQN   = new FullQualifiedName( "ICDC.DispatchPersons" );
    public static        String            DISPATCH_PERSON_ES_ALIAS = DISPATCH_PERSON_ES_FQN
            .getFullQualifiedNameAsString();
    public static        String            DISPATCH_PERSON_ES_NAME  = "IowaCityDispatchCenter_DispatchPersons";

    private static Dataset<Row> getPayloadFromCsv( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        //        String csvPath = Resources.getResource( "dispatch_persons.csv" ).getPath();

        String sql = "\"(select * from dbo.Dispatch_Persons where Dis_id IN "
                + "( select distinct (Dis_Id) from Dispatch where CFS_DateTimeJanet > DateADD(d, -7, GETDATE()) ) ) Dispatch_Persons\"";
        logger.info( "SQL Query for persons: {}", sql );
        Dataset<Row> payload = sparkSession
                .read()
                .format( "jdbc" )
                .option( "url", config.getUrl() )
                .option( "dbtable", sql )
                .option( "password", config.getDbPassword() )
                .option( "user", config.getDbUser() )
                .option( "driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver" )
                .load();
        payload.createOrReplaceTempView( "Dispatch_Persons" );
        //                .filter( col( "Timercvd" ).geq( DateTime.now().minusDays( 2 ) ) )
        //                .filter( col( "Type" ).notEqual( "2" ) );

        return payload;
    }

    public static Map<Flight, Dataset<Row>> getFlight( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        Dataset<Row> payload = getPayloadFromCsv( sparkSession, config );

        // 1. "ICDC.DispatchPersonId" should be something else. what does the "ID" column represent?
        // 2. what format is "ICDC.Height" supposed to be?
        // 3. should "ICDC.MNINumber" and "ICDC.MVINumber" be UUIDs?
        // 4. what is "ICDC.Type"?

        // @formatter:off
        Flight flight = Flight
            .newFlight()
                .createEntities()
                    .addEntity( DISPATCH_PERSON_ES_ALIAS )
                        .to( DISPATCH_PERSON_ES_NAME )
                        .ofType( DISPATCH_PERSON_ET_FQN )
                        .key( DISPATCH_PERSON_ID_FQN )
                        .addProperty( DISPATCH_PERSON_ID_FQN ).value( row -> getAsString( row.getAs( "ID" ) ) ).ok()
                        .addProperty( DISPATCH_ID_FQN ).value( row -> getAsString( row.getAs( "Dis_ID" ) ) ).ok()
                        .addProperty( OFFICER_ID_FQN ).value( row -> getAsString( row.getAs( "OfficerID" ) ) ).ok()
                        .addProperty( DOB_FQN ).value( row -> getAsDate( row.getAs( "DOB" ) ) ).ok()
                        .addProperty( OSQ_FQN ).value( row -> getAsString( row.getAs( "OSQ" ) ) ).ok()
                        .addProperty( O_NAME_FQN ).value( row -> getAsString( row.getAs( "OName" ) ) ).ok()
                        .addProperty( O_ADDRESS_FQN ).value( row -> getAsString( row.getAs( "OAddress" ) ) ).ok()
                        .addProperty( O_ADDRESS_APT_FQN ).value( row -> getAsString( row.getAs( "OAddress_Apt" ) ) ).ok()
                        .addProperty( O_CITY_FQN ).value( row -> getAsString( row.getAs( "OCity" ) ) ).ok()
                        .addProperty( O_STATE_FQN ).value( row -> getAsString( row.getAs( "OState" ) ) ).ok()
                        .addProperty( O_ZIP_FQN ).value( row -> getAsString( row.getAs( "OZip" ) ) ).ok()
                        .addProperty( O_PHONE_FQN ).value( row -> getAsString( row.getAs( "OPhone" ) ) ).ok()
                        .addProperty( O_SEX_FQN ).value( row -> getAsString( row.getAs( "OSex" ) ) ).ok()
                        .addProperty( O_RACE_FQN ).value( row -> getAsString( row.getAs( "ORace" ) ) ).ok()
                        .addProperty( TYPE_FQN ).value( row -> getAsString( row.getAs( "Type" ) ) ).ok()
                        .addProperty( VEHICLE_MAKE_FQN ).value( row -> getAsString( row.getAs( "MAKE" ) ) ).ok()
                        .addProperty( VEHICLE_MODEL_FQN ).value( row -> getAsString( row.getAs( "MODEL" ) ) ).ok()
                        .addProperty( VEHICLE_YEAR_FQN ).value( row -> getAsString( row.getAs( "VehYear" ) ) ).ok()
                        .addProperty( VEHICLE_VIN_FQN ).value( row -> getAsString( row.getAs( "VIN" ) ) ).ok()
                        .addProperty( VEHICLE_COLOR_FQN ).value( row -> getAsString( row.getAs( "Color" ) ) ).ok()
                        .addProperty( VEHICLE_STYLE_FQN ).value( row -> getAsString( row.getAs( "Style" ) ) ).ok()
                        .addProperty( LIC_FQN ).value( row -> getAsString( row.getAs( "LIC" ) ) ).ok()
                        .addProperty( LIS_FQN ).value( row -> getAsString( row.getAs( "LIS" ) ) ).ok()
                        .addProperty( NCIC_FQN ).value( row -> getAsString( row.getAs( "NCIC" ) ) ).ok()
                        .addProperty( CELL_PHONE_FQN ).value( row -> getAsString( row.getAs( "CellPhone" ) ) ).ok()
                        .addProperty( CFS_PERSON_ID_FQN ).value( row -> getAsUUID( row.getAs( "CallForServicePersonId" ) ) ).ok()
                        .addProperty( BADGE_NUM_FQN ).value( row -> getAsString( row.getAs( "BadgeNumber" ) ) ).ok()
                        .addProperty( AGE_FQN ).value( row -> getAsString( row.getAs( "Age" ) ) ).ok()
                        .addProperty( HEIGHT_FQN ).value( row -> getAsString( row.getAs( "Height" ) ) ).ok()
                        .addProperty( WEIGHT_FQN ).value( row -> getAsString( row.getAs( "Weight" ) ) ).ok()
                        .addProperty( EYES_FQN ).value( row -> getAsString( row.getAs( "Eyes" ) ) ).ok()
                        .addProperty( HAIR_FQN ).value( row -> getAsString( row.getAs( "Hair" ) ) ).ok()
                        .addProperty( ETHNICITY_FQN ).value( row -> getAsString( row.getAs( "Ethnicity" ) ) ).ok()
                        .addProperty( ADDITIONAL_FQN ).value( row -> getAsString( row.getAs( "Additional" ) ) ).ok()
                        .addProperty( TRANSFER_VEHICLE_FQN ).value( row -> getAsBoolean( row.getAs( "TransferVehicle" ) ) ).ok()
                        .addProperty( JUV_FQN ).value( row -> getAsBoolean( row.getAs( "Juv" ) ) ).ok()
                        .addProperty( LIT_FQN ).value( row -> getAsString( row.getAs( "LIT" ) ) ).ok()
                        .addProperty( LIY_FQN ).value( row -> getAsString( row.getAs( "LIY" ) ) ).ok()
                        .addProperty( MNI_NUM_FQN ).value( row -> getAsString( row.getAs( "MNI_No" ) ) ).ok()
                        .addProperty( MVI_NUM_FQN ).value( row -> getAsString( row.getAs( "MVI_NO" ) ) ).ok()
                        .addProperty( UPSIZE_TS_FQN ).value( row -> getAsString( row.getAs( "upsize_ts" ) ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Map<Flight, Dataset<Row>> result = new HashMap<>( 1 );
        result.put( flight, payload );

        return result;
    }
}
