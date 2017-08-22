package com.openlattice.integrations.iowacity.dispatchcenter;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.DispatchFlight;
import com.openlattice.integrations.iowacity.dispatchcenter.flights.SystemUserBaseFlight;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

public class IowaCityDispatchCenter {

    private static final Environment environment = Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException {

        final String jwtToken = args[ 0 ];

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edmApi = retrofit.create( EdmApi.class );

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }

        Map<Flight, Dataset<Row>> systemUserBaseFlight = SystemUserBaseFlight.getFlight();
        Map<Flight, Dataset<Row>> dispatchFlight = DispatchFlight.getFlight();

        Map<Flight, Dataset<Row>> flights = new HashMap<>();
        flights.putAll( systemUserBaseFlight );
        flights.putAll( dispatchFlight );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }
}
