/*
 * Copyright (C) 2017. OpenLattice, Inc
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * You can contact the owner of the copyright at support@openlattice.com
 *
 */

package com.openlattice.integrations.iowacity.dispatchcenter;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
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
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

public class IowaCityDispatchCenter2 {

    private static final Logger logger = LoggerFactory.getLogger( IowaCityDispatchCenter2.class );

    private static final Environment  environment = Environment.STAGING;
    private static final ObjectMapper mapper      = new ObjectMapper();

    public static void main( String[] args ) throws InterruptedException, IOException, SQLException {
        JdbcIntegrationConfig config = ObjectMappers.getYamlMapper()
                .readValue( new File( args[ 0 ] ), JdbcIntegrationConfig.class );
        HikariConfig iowadbconfig = new HikariConfig();
        iowadbconfig.setJdbcUrl( config.getUrl() );
        iowadbconfig.setMaximumPoolSize( 2 );

        HikariDataSource iowadb = new HikariDataSource( iowadbconfig );

        HikariConfig oldbconfig = new HikariConfig();
        oldbconfig.setJdbcUrl(
                "jdbc:postgresql://wonderwoman.openlattice.com:30001/johnsoncounty_iowa?ssl=true&sslmode=require" );
        oldbconfig.setUsername( "itjecc" );
        oldbconfig.setPassword( config.getOlsPassword() );
        oldbconfig.setMaximumPoolSize( 100 );

        HikariDataSource oldb = new HikariDataSource( oldbconfig );

        Progress progress = mapper.readValue( new File( "progress.json" ), Progress.class );

        if ( progress == null ) {
            progress = new Progress();
        }

        Connection iowaConn = iowadb.getConnection();
        Connection olsConn = oldb.getConnection();

        ResultSet allIds = iowaConn.createStatement().executeQuery( DispatchFlight.getAllIdsQuery() );

        while ( allIds.next() ) {
            long id = allIds.getLong( "Dis_ID" );
            if ( !progress.dispatchIds.contains( id ) ) {
                PreparedStatement p = iowaConn.prepareStatement( DispatchFlight.getIdQuery( id ) );
//                PreparedStatement insertRow = olsConn.prepareStatement();
                p.setLong(1, id);
                ResultSet row = p.executeQuery();
                if( row.next() ) {
                    //Write row to DB.


                }
                progress.dispatchIds.add( id );
            }
        }

        ResultSet allPersonIds = iowaConn.createStatement().executeQuery( DispatchPersonsFlight.getAllIdsQuery() );

        while(allPersonIds.next()) {
            long id = allPersonIds.getLong("ID");
            if( !progress.dispatchPersonIds.contains( id )) {
                PreparedStatement p = iowaConn.prepareStatement( DispatchPersonsFlight.getIdQuery( id ) );
                p.setLong(1, id);
                ResultSet row = p.executeQuery();
                if( row.next() ) {
                    //Write rows to OLS DB.

                }
                progress.dispatchPersonIds.add( id );
            }

        }


        ResultSet allTypeIds = iowaConn.createStatement().executeQuery( DispatchTypeFlight.getAllIdsQuery() );

        while(allTypeIds.next()) {
            long id = allTypeIds.getLong("ID");
            if( !progress.dispatchTypeIds.contains( id )) {
                PreparedStatement p = iowaConn.prepareStatement( DispatchTypeFlight.getIdQuery( id ) );
                p.setLong(1, id);
                ResultSet row = p.executeQuery();
                if( row.next() ) {
                    //Write rows to OLS DB.

                }
                progress.dispatchTypeIds.add( id );
            }
        }



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

        int dispatchFlightStart = 0, dispatchFlightStep = 20000, dispatchFlightEnd = 3277890;
        int dispatchTypeFlightStart = 0, dispatchTypeFlightStep = 20000, dispatchTypeFlightEnd = 3810310;
        int dispatchPersonStart = 0, dispatchPersonStep = 20000, dispatchPersonEnd = 2583339;

        Map<Flight, Dataset<Row>> systemUserBaseFlight = SystemUserBaseFlight.getFlight( sparkSession, config );
        Map<Flight, Dataset<Row>> flights = new LinkedHashMap<>(
                ( dispatchFlightEnd + dispatchTypeFlightEnd + dispatchPersonEnd + 1 ) / 20000 );
        flights.putAll( systemUserBaseFlight );
        Shuttle shuttle = new Shuttle( environment, jwtToken );

        for ( dispatchFlightStart = 0;
                dispatchFlightStart < dispatchFlightEnd;
                dispatchFlightStart += dispatchFlightStep ) {
            logger.info( "Starting registration: integration of dispatch ids {} -> {} ",
                    dispatchFlightStart,
                    dispatchFlightStart + dispatchFlightStep );
            Map<Flight, Dataset<Row>> dispatchFlight = DispatchFlight
                    .getFlight( sparkSession, config, dispatchFlightStart, dispatchFlightStart + dispatchFlightStep );
            flights.putAll( dispatchFlight );

            logger.info( "Finishing registration: integration of dispatch ids {} -> {} ",
                    dispatchFlightStart,
                    dispatchFlightStart + dispatchPersonStep );
        }

        for ( dispatchTypeFlightStart = 0;
                dispatchTypeFlightStart < dispatchTypeFlightEnd;
                dispatchTypeFlightStart += dispatchTypeFlightStep ) {
            logger.info( "Starting registration: integration of dispatch type ids {} -> {} ",
                    dispatchTypeFlightStart,
                    dispatchTypeFlightStart + dispatchTypeFlightStep );
            Map<Flight, Dataset<Row>> dispatchTypeFlight = DispatchTypeFlight.getFlight( sparkSession,
                    config,
                    dispatchTypeFlightStart,
                    dispatchTypeFlightStart + dispatchTypeFlightStep );
            flights.putAll( dispatchTypeFlight );
            logger.info( "Finishing registration: integration of dispatch type ids {} -> {} ",
                    dispatchTypeFlightStart,
                    dispatchTypeFlightStart + dispatchTypeFlightStep );
        }

        for ( dispatchPersonStart = 0;
                dispatchPersonStart < dispatchPersonEnd;
                dispatchPersonStart += dispatchPersonStep ) {
            logger.info( "Starting registration: integration of dispatch person ids {} -> {} ",
                    dispatchPersonStart,
                    dispatchPersonStart + dispatchPersonStep );
            Map<Flight, Dataset<Row>> dispatchPersonsFlight = DispatchPersonsFlight
                    .getFlight( sparkSession, config, dispatchPersonStart, dispatchPersonStart + dispatchPersonStep );
            flights.putAll( dispatchPersonsFlight );
            shuttle.launch( flights );

            logger.info( "Finishing registration: integration of dispatch person ids {} -> {} ",
                    dispatchPersonStart,
                    dispatchPersonStart + dispatchPersonStep );
        }

        shuttle.launch( flights );

    }

    public static class Progress {
        Set<Long> dispatchIds       = Sets.newHashSetWithExpectedSize( 3200000 );
        Set<Long> dispatchTypeIds   = Sets.newHashSetWithExpectedSize( 3200000 );
        Set<Long> dispatchPersonIds = Sets.newHashSetWithExpectedSize( 3200000 );
    }
}
