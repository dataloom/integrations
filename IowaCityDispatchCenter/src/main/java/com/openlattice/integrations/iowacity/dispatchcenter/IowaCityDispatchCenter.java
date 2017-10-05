package com.openlattice.integrations.iowacity.dispatchcenter;

import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.mappers.ObjectMappers;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchPersonsFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchTypeFlight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.config.JdbcIntegrationConfig;
import java.io.File;
import java.io.IOException;
import java.util.Properties;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IowaCityDispatchCenter {

    private static final Logger logger = LoggerFactory.getLogger( IowaCityDispatchCenter.class );

    private static final Environment environment = Environment.STAGING;

    public static void main( String[] args ) throws InterruptedException, IOException {
        JdbcIntegrationConfig config = ObjectMappers.getYamlMapper()
                .readValue( new File( args[ 0 ] ), JdbcIntegrationConfig.class );

        final String jwtToken = MissionControl.getIdToken( config.getOlsUser(), config.getOlsPassword() );
        final SparkSession sparkSession = MissionControl.getSparkSession();

        Dataset<Row> dispatch = DispatchFlight.getPayloadFromCsv( sparkSession, config );
        Dataset<Row> dispatch_person = DispatchPersonsFlight.getPayloadFromCsv( sparkSession, config );
        Dataset<Row> dispatch_type = DispatchTypeFlight.getPayloadFromCsv( sparkSession, config );

        String jdbcUrl = "jdbc:postgresql://wonderwoman.openlattice.com:30001/johnsoncounty_iowa?ssl=true&sslmode=require";
        Properties properties = new Properties();
        properties.setProperty( "user", "itjecc" );
        properties.setProperty( "password", config.getOlsPassword() );

        dispatch.write()
                .option( "batchsize", 20000 )
                .option( "driver", "org.postgresql.Driver" )
                .mode( SaveMode.Overwrite)
                .jdbc( jdbcUrl, "dispatch", properties );
        dispatch_person.write()
                .option( "batchsize", 20000 )
                .option( "driver", "org.postgresql.Driver" )
                .mode(SaveMode.Overwrite)
                .jdbc( jdbcUrl, "dispatch_person", properties );
        dispatch_type.write()
                .option( "batchsize", 20000 )
                .option( "driver", "org.postgresql.Driver" )
                .mode(SaveMode.Overwrite)
                .jdbc( jdbcUrl, "dispatch_type", properties );

        //
        //        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        //        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        //
        //        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        //        EdmApi edmApi = retrofit.create( EdmApi.class );
        //        PermissionsApi permissionsApi = retrofit.create( PermissionsApi.class );
        //
        //        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
        //                .loadConfiguration( RequiredEdmElements.class );
        //
        //        if ( requiredEdmElements != null ) {
        //            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionsApi );
        //            manager.ensureEdmElementsExist( requiredEdmElements );
        //        }
        //
        //        int dispatchFlightStart = 0, dispatchFlightStep = 20000, dispatchFlightEnd = 3277890;
        //        int dispatchTypeFlightStart = 0, dispatchTypeFlightStep = 20000, dispatchTypeFlightEnd = 3810310;
        //        int dispatchPersonStart = 0, dispatchPersonStep = 20000, dispatchPersonEnd = 2583339;
        //
        //
        //        Map<Flight, Dataset<Row>> systemUserBaseFlight = SystemUserBaseFlight.getFlight( sparkSession, config );
        //
        //        DispatchFlight.getPayloadFromCsv( sparkSession, config );
        //
        //        Map<Flight, Dataset<Row>> flights = new LinkedHashMap<>(
        //                ( dispatchFlightEnd + dispatchTypeFlightEnd + dispatchPersonEnd +1 ) / 20000 );
        //        flights.putAll( systemUserBaseFlight );
        //        Shuttle shuttle = new Shuttle( environment, jwtToken );
        //
        //        for ( dispatchFlightStart = 0;
        //                dispatchFlightStart < dispatchFlightEnd;
        //                dispatchFlightStart += dispatchFlightStep ) {
        //            logger.info( "Starting registration: integration of dispatch ids {} -> {} ",
        //                    dispatchFlightStart,
        //                    dispatchFlightStart + dispatchFlightStep );
        //            Map<Flight, Dataset<Row>> dispatchFlight = DispatchFlight
        //                    .getFlight( sparkSession, config, dispatchFlightStart, dispatchFlightStart + dispatchFlightStep );
        //            flights.putAll( dispatchFlight );
        //
        //            logger.info( "Finishing registration: integration of dispatch ids {} -> {} ",
        //                    dispatchFlightStart,
        //                    dispatchFlightStart + dispatchPersonStep );
        //        }
        //
        //        for ( dispatchTypeFlightStart = 0;
        //                dispatchTypeFlightStart < dispatchTypeFlightEnd;
        //                dispatchTypeFlightStart += dispatchTypeFlightStep ) {
        //            logger.info( "Starting registration: integration of dispatch type ids {} -> {} ",
        //                    dispatchTypeFlightStart,
        //                    dispatchTypeFlightStart + dispatchTypeFlightStep );
        //            Map<Flight, Dataset<Row>> dispatchTypeFlight = DispatchTypeFlight.getFlight( sparkSession,
        //                    config,
        //                    dispatchTypeFlightStart,
        //                    dispatchTypeFlightStart + dispatchTypeFlightStep );
        //            flights.putAll( dispatchTypeFlight );
        //            logger.info( "Finishing registration: integration of dispatch type ids {} -> {} ",
        //                    dispatchTypeFlightStart,
        //                    dispatchTypeFlightStart + dispatchTypeFlightStep );
        //        }
        //
        //        for ( dispatchPersonStart = 0;
        //                dispatchPersonStart < dispatchPersonEnd;
        //                dispatchPersonStart += dispatchPersonStep ) {
        //            logger.info( "Starting registration: integration of dispatch person ids {} -> {} ",
        //                    dispatchPersonStart,
        //                    dispatchPersonStart + dispatchPersonStep );
        //            Map<Flight, Dataset<Row>> dispatchPersonsFlight = DispatchPersonsFlight
        //                    .getFlight( sparkSession, config, dispatchPersonStart, dispatchPersonStart + dispatchPersonStep );
        //            flights.putAll( dispatchPersonsFlight );
        //            shuttle.launch( flights );
        //
        //            logger.info( "Finishing registration: integration of dispatch person ids {} -> {} ",
        //                    dispatchPersonStart,
        //                    dispatchPersonStart + dispatchPersonStep );
        //        }
        //
        //        shuttle.launch( flights );

    }
}
