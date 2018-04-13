/*
 * Copyright (C) 2018. OpenLattice, Inc
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

package com.openlattice.middlesex;

import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.FilterablePayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.openlattice.client.RetrofitFactory;
import com.openlattice.client.RetrofitFactory.Environment;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class CambridgeIncidents {
    private static final Logger logger = LoggerFactory.getLogger( CambridgeIncidents.class );
    private static final Environment environment = Environment.PRODUCTION;

    private static final DateTimeHelper bdHelper = new DateTimeHelper( TimeZones.America_NewYork, "dd/MM/YY HH:mm" );
    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_NewYork, "dd/MM/yyyy" );


    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        final String incidentsPath = args[ 0 ];
        final String jwtToken = args[ 1 ];

        SimplePayload iPayload = new SimplePayload( incidentsPath );

        Payload suspectsPayload = new FilterablePayload( incidentsPath );
        Map<String, String> caseIdToTime = iPayload.getPayload()
                .collect( Collectors.toMap( row -> row.get( "Role" ) ) );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Flight suspectsflight = Flight.newFlight()
                .createEntities()

                .addEntity( "people" )
                .to( "CambridgePeople_1" )
                    .addProperty( "nc.SubjectIdentification", "PersonID" )
                    .addProperty( "nc.PersonGivenName", "First" )
                    .addProperty( "nc.PersonMiddleName", "Middle" )
                    .addProperty( "nc.PersonSurname", "Last" )
                    .addProperty( "nc.PersonBirthDate" )
                        .value( row -> bdHelper.parseLocalDate( row.getAs( "DOB" ) ) ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonSex", "Sex" )
                    .addProperty( "nc.PersonRace" )
                        .value( CambridgeIncidents::standardRaceList  ).ok

                .done()

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( suspectsflight, iPayload );

        shuttle.launchPayloadFlight( flights );
    }


        public static List standardRaceList( Row row ) {
            String sr = row.getAs("Race");

            if  (sr != null) {

                String[] racesArray = StringUtils.split( sr, "," );
                List<String> races = Arrays.asList( racesArray );

                if ( races != null ) {
                    Collections.replaceAll( races, "A", "asian" );
                    Collections.replaceAll( races, "W", "white" );
                    Collections.replaceAll( races, "B", "black" );
                    Collections.replaceAll( races, "I", "amindian" );
                    Collections.replaceAll( races, "U", "pacisland" );
                    Collections.replaceAll( races, "", "" );

                    List<String> finalRaces = races
                            .stream()
                            .filter( StringUtils::isNotBlank )
                            //                    .filter( (race) ->
                            //                            (race != "Client refused") && (race != "Data not collected")
                            //                    )
                            .collect( Collectors.toList() );

                    return finalRaces;
                }
                return null;
            }
            return null;
        }

}

