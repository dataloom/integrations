package com.openlattice.integrations.iowacity.dispatchcenter.flights;

import com.google.common.io.Resources;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getActive;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getEmployeeId;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getString;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getUUID;

public class SystemUserBaseFlight {

    // PropertyTypes
    public static FullQualifiedName FIRST_NAME_FQN  = new FullQualifiedName( "ICDC.FirstName" );
    public static FullQualifiedName LAST_NAME_FQN   = new FullQualifiedName( "ICDC.LastName" );
    public static FullQualifiedName TITLE_FQN       = new FullQualifiedName( "ICDC.Title" );
    public static FullQualifiedName EMPLOYEE_ID_FQN = new FullQualifiedName( "ICDC.EmployeeId" );
    public static FullQualifiedName OFFICER_ID_FQN  = new FullQualifiedName( "ICDC.OfficerId" );
    public static FullQualifiedName ORI_FQN         = new FullQualifiedName( "ICDC.ORI" );
    public static FullQualifiedName ACTIVE_FQN      = new FullQualifiedName( "ICDC.Active" );

    // EntityTypes
    public static FullQualifiedName EMPLOYEE_ET_FQN = new FullQualifiedName( "ICDC.Employee" );

    // EntitySets
    public static FullQualifiedName EMPLOYEES_ES_FQN   = new FullQualifiedName( "ICDC.Employees" );
    public static String            EMPLOYEES_ES_ALIAS = EMPLOYEES_ES_FQN.getFullQualifiedNameAsString();
    public static String            EMPLOYEES_ES_NAME  = "IowaCityDispatchCenter_Employees";

    private static Dataset<Row> getPayloadFromCsv() {

        final SparkSession sparkSession = MissionControl.getSparkSession();
        String csvPath = Resources.getResource( "systemuserbase.csv" ).getPath();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( csvPath );

        return payload;
    }

    public static Map<Flight, Dataset<Row>> getFlight() {

        // @formatter:off
        Flight flight = Flight
            .newFlight()
                .createEntities()
                    .addEntity( EMPLOYEES_ES_ALIAS )
                        .to( EMPLOYEES_ES_NAME )
                        .ofType( EMPLOYEE_ET_FQN )
                        .key( EMPLOYEE_ID_FQN, OFFICER_ID_FQN )
                        .addProperty( FIRST_NAME_FQN ).value( row -> getString( row.getAs( "FirstName" ) ) ).ok()
                        .addProperty( LAST_NAME_FQN ).value( row -> getString( row.getAs( "LastName" ) ) ).ok()
                        .addProperty( TITLE_FQN ).value( row -> getString( row.getAs( "Title" ) ) ).ok()
                        .addProperty( EMPLOYEE_ID_FQN ).value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
                        .addProperty( OFFICER_ID_FQN ).value( row -> getUUID( row.getAs( "officerid" ) ) ).ok()
                        .addProperty( ORI_FQN ).value( row -> getString( row.getAs( "ori" ) ) ).ok()
                        .addProperty( ACTIVE_FQN ).value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Map<Flight, Dataset<Row>> result = new HashMap<>( 1 );
        result.put( flight, getPayloadFromCsv() );

        return result;
    }
}
