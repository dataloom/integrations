package com.openlattice.integrations.iowacity.dispatchcenter;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchPersonsFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchTypeFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.SystemUserBaseFlight;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.config.JdbcIntegrationConfig;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

public class IowaCityDispatchCenter {

    private static final Logger logger = LoggerFactory.getLogger( IowaCityDispatchCenter.class );

    private static final Environment environment = Environment.STAGING;

    public static void main( String[] args ) throws InterruptedException, IOException {
        JdbcIntegrationConfig config = ObjectMappers.getYamlMapper()
                .readValue( new File( args[ 0 ] ), JdbcIntegrationConfig.class );

        final String jwtToken = MissionControl.getIdToken( config.getOlsUser(), config.getOlsPassword() );
        final SparkSession sparkSession = MissionControl.getSparkSession();

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionsApi = retrofit.create( PermissionsApi.class );

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionsApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }

        Map<Flight, Dataset<Row>> systemUserBaseFlight = SystemUserBaseFlight.getFlight( sparkSession, config );
        Map<Flight, Dataset<Row>> dispatchFlight = DispatchFlight.getFlight( sparkSession, config );
        Map<Flight, Dataset<Row>> dispatchTypeFlight = DispatchTypeFlight.getFlight( sparkSession, config );
        Map<Flight, Dataset<Row>> dispatchPersonsFlight = DispatchPersonsFlight.getFlight( sparkSession, config );

        Map<Flight, Dataset<Row>> flights = new HashMap<>();
        flights.putAll( systemUserBaseFlight );
        flights.putAll( dispatchFlight );
        flights.putAll( dispatchTypeFlight );
        flights.putAll( dispatchPersonsFlight );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }
}
