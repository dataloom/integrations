package com.openlattice.integrations.iowacity.dispatchcenter.flights;

import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getActive;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsString;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getAsUUID;
import static com.openlattice.integrations.iowacity.dispatchcenter.Helpers.getEmployeeId;

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

public class SystemUserBaseFlight {

    private static final Logger logger = LoggerFactory.getLogger( SystemUserBaseFlight.class );

    /*
     * PropertyTypes
     */

    public static FullQualifiedName FIRST_NAME_FQN  = new FullQualifiedName( "ICDC.FirstName" );    // String
    public static FullQualifiedName LAST_NAME_FQN   = new FullQualifiedName( "ICDC.LastName" );     // String
    public static FullQualifiedName TITLE_FQN       = new FullQualifiedName( "ICDC.Title" );        // String
    public static FullQualifiedName EMPLOYEE_ID_FQN = new FullQualifiedName( "ICDC.EmployeeId" );   // String
    public static FullQualifiedName OFFICER_ID_FQN  = new FullQualifiedName( "ICDC.OfficerId" );    // Guid
    public static FullQualifiedName ORI_FQN         = new FullQualifiedName( "ICDC.ORI" );          // String
    public static FullQualifiedName ACTIVE_FQN      = new FullQualifiedName( "ICDC.Active" );       // Boolean

    /*
     * EntityTypes
     */

    public static FullQualifiedName EMPLOYEE_ET_FQN = new FullQualifiedName( "ICDC.Employee" );

    /*
     * EntitySets
     */

    public static FullQualifiedName EMPLOYEES_ES_FQN   = new FullQualifiedName( "ICDC.Employees" );
    public static String            EMPLOYEES_ES_ALIAS = EMPLOYEES_ES_FQN.getFullQualifiedNameAsString();
    public static String            EMPLOYEES_ES_NAME  = "IowaCityDispatchCenter_Employees";

    private static Dataset<Row> getPayloadFromCsv( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        //String csvPath = Resources.getResource( "system_user_base.csv" ).getPath();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "jdbc" )
                .option( "url", config.getUrl() )
                .option( "dbtable", "dbo.SystemUserBase" )
                .option( "user", config.getDbUser() )
                .option( "password", config.getDbPassword() )
                .option( "driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .load();

        return payload;
    }

    public static Map<Flight, Dataset<Row>> getFlight( final SparkSession sparkSession, JdbcIntegrationConfig config ) {

        Dataset<Row> payload = getPayloadFromCsv( sparkSession, config );

        // @formatter:off
        Flight flight = Flight
            .newFlight()
                .createEntities()
                    .addEntity( EMPLOYEES_ES_ALIAS )
                        .to( EMPLOYEES_ES_NAME )
                        .ofType( EMPLOYEE_ET_FQN )
                        .key( EMPLOYEE_ID_FQN, OFFICER_ID_FQN )
                        .addProperty( FIRST_NAME_FQN ).value( row -> getAsString( row.getAs( "FirstName" ) ) ).ok()
                        .addProperty( LAST_NAME_FQN ).value( row -> getAsString( row.getAs( "LastName" ) ) ).ok()
                        .addProperty( TITLE_FQN ).value( row -> getAsString( row.getAs( "Title" ) ) ).ok()
                        .addProperty( EMPLOYEE_ID_FQN ).value( row -> getEmployeeId( row.getAs( "EmployeeId" ) ) ).ok()
                        .addProperty( OFFICER_ID_FQN ).value( row -> getAsUUID( row.getAs( "OfficerId" ) ) ).ok()
                        .addProperty( ORI_FQN ).value( row -> getAsString( row.getAs( "ORI" ) ) ).ok()
                        .addProperty( ACTIVE_FQN ).value( row -> getActive( row.getAs( "EmployeeId" ) ) ).ok()
                        .ok()
                    .ok()
                .done();
        // @formatter:on

        Map<Flight, Dataset<Row>> result = new HashMap<>( 1 );
        result.put( flight, payload );

        return result;
    }
}
