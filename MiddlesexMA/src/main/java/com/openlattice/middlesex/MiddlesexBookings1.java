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

package com.openlattice.middlesex;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MiddlesexBookings1 {

    private static final Logger                      logger      = LoggerFactory
            .getLogger( MiddlesexBookings1.class );
    private static final RetrofitFactory.Environment environment = Environment.PRODUCTION;
    private static final DateTimeHelper              dtHelper    = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -5 ), "MM/dd/yy" );
    private static final DateTimeHelper              bdHelper    = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -5 ), "MM/dd/yy" );

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String path = args[0];
        final String jwtToken = args[1];
        //final String username = args[ 1 ];
        //final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );
        //.sample( false, .1 );
        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        //        if ( requiredEdmElements != null ) {
        //            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm,
        //                    retrofit.create( PermissionsApi.class ) );
        //            reem.ensureEdmElementsExist( requiredEdmElements );
        //        }

        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( "suspect" )
                .to( "MSOSuspects" )
                .addProperty( "nc.PersonGivenName", "f_name" )
                .addProperty( "nc.PersonMiddleName", "m_name" )
                .addProperty("nc.PersonSurName", "l_name" )
                .addProperty("nc.SSN", "ssno" )
                .addProperty( "nc.PersonRace", "race" )
                .addProperty( "nc.MaritalStatus", "marit" )
                .addProperty( "nc.PersonBirthDate" ).value( MiddlesexBookings1::safeDOBParse ).ok()
                .addProperty( "nc.PersonBirthPlace", "birth" )
                .addProperty("nc.SubjectIdentification" ).value( MiddlesexBookings1::getSubjectIdentification ).ok()
                .endEntity()

                .addEntity( "address" )
                .to( "MSOAddresses" )
                .addProperty( "location.street", "addr" )
                .addProperty( "location.city", "city" )
                .addProperty( "location.state", "state" )
                .addProperty( "location.zip", "zip" )
                .endEntity()

                .addEntity( "booking" )
                .to( "MSOBookings" )
                .addProperty( "justice.ReferralDate" ).value( row -> safeDateParse( row.getAs( "dt_asg" ) ) ).ok()
                .addProperty( "publicsafety.ReleaseDate" ).value( row -> safeDateParse( row.getAs( "dt_rel" ) ) ).ok()
                .addProperty( "justice.ReleaseComments", "rel_com" )
                .addProperty( "justice.Bail", "bail" )
                .addProperty( "j.OffenseViolatedStatute", "maj_off" )
                .addProperty( "j.CaseNumberText", "dockno" )
                .addProperty( "j.ArrestAgency", "ar_agen" )
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "bookedin" )
                .to( "MSOBooked" )
                .fromEntity( "suspect" )
                .toEntity( "booking" )
                .addProperty( "general.stringid" ).value( row -> "booked in" ).ok()
                .endAssociation()

                .addAssociation( "livesat" )
                .to( "MSOLivesAt" )
                .fromEntity( "suspect" )
                .toEntity( "address" )
                .addProperty( "general.stringid" ).value( row -> "lives at" ).ok()
                .endAssociation()

                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        shuttle.launch( flights );
    }

    public static String safeDateParse( Object obj ) {
        if ( obj == null ) return null;
        String date = obj.toString();
        if ( date.equals( "  -   -" ) ) {
            return null;
        }
        return dtHelper.parse( date );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "dt_ob" );
        if ( dob == null ) {
            return null;
        }
        if ( dob.contains( "#" ) ) {
            return null;
        }
        if ( dob.equals( "  -   -" ) ) {
            return null;
        }
        return bdHelper.parse( dob );
    }

    public static String getSubjectIdentification( Row row ) {
        return row.getAs( "insno_a" ).toString().trim();
    }
}
