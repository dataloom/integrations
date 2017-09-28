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

package com.openlattice.integrations.wisconsin.danecounty;

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
import java.util.HashMap;
import java.util.Map;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

/**
 * Created by mtamayo on 6/19/17.
 */
public class DaneCountySheriffs {

    private static final Logger                      logger            = LoggerFactory
            .getLogger( DaneCountySheriffs.class );
    private static final RetrofitFactory.Environment environment       = Environment.PRODUCTION;
    private static final DateTimeHelper              dtHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "yyyy-MM-dd HH:mm:ss.SSS" );
    private static final DateTimeHelper              bdHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "yyyy-MM-dd HH:mm:ss.SSS" );
    public static        FullQualifiedName           ARREST_AGENCY_FQN = new FullQualifiedName( "j.ArrestAgency" );
    public static        FullQualifiedName           FIRSTNAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    public static        FullQualifiedName           LASTNAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    public static        FullQualifiedName           SEX_FQN           = new FullQualifiedName( "nc.PersonSex" );
    public static        FullQualifiedName           RACE_FQN          = new FullQualifiedName( "nc.PersonRace" );
    public static        FullQualifiedName           DOB_FQN           = new FullQualifiedName( "nc.PersonBirthDate" );
    public static        FullQualifiedName           OFFICER_ID_FQN    = new FullQualifiedName( "publicsafety.officerID" );
    public static        FullQualifiedName           ARREST_DATE_FQN   = new FullQualifiedName(
            "publicsafety.arrestdate" );
    static String[] fieldNames;

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String path = args[ 0 ];
        final String jwtToken = args[ 1 ];

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
        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm,
                    retrofit.create( PermissionsApi.class ) );
            reem.ensureEdmElementsExist( requiredEdmElements );
        }

//        Flight flight = Flight.newFlight()
//                .createEntities()
//                .addEntity( "suspect" )
//                .to( "DCSOArrestSuspects" )
//                .ofType( "general.person" )
//                .key( "nc.SubjectIdentification" )
//                .addProperty( "nc.PersonGivenName" )
//                .value( "first" ).ok()
//                .addProperty( "nc.PersonMiddleName" )
//                .value( "middle" ).ok()
//                .addProperty( "nc.PersonSurName" )
//                .value( "last" ).ok()
//                .addProperty( "nc.PersonSex" )
//                .value( "sex" ).ok()
//                .addProperty( "nc.PersonRace" )
//                .value( "race" ).ok()
//                .addProperty("nc.PersonEthnicity" )
//                .value( "ethnic" ).ok()
//                .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
//                .value( DaneCountySheriffs::safeDOBParse ).ok()
//                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
//                .value( DaneCountySheriffs::getSubjectIdentification ).ok()
//                .ok()
//                .addEntity( "arrest" )
//                .to( "DCSOArrests" )
//                .ofType( new FullQualifiedName( "lawenforcement.arrest" ) )
//                .key( new FullQualifiedName( "j.ArrestSequenceID" ) )
//                .addProperty( new FullQualifiedName( "publicsafety.ArrestDate" ) )
//                .value( row -> dtHelper.parse( row.getAs( "bookdt" ) ) ).ok()
//                .addProperty( "publicsafety.ReleaseDate" )
//                .value( row -> dtHelper.parse( row.getAs( "bookdt" ) ) ).ok()
//                .addProperty( new FullQualifiedName( "justice.disposition" ) )
//                .value( "dispos" ).ok()
//                .addProperty( new FullQualifiedName( "justice.EventType" ) )
//                .value( "classif" ).ok()
//                .addProperty( new FullQualifiedName( "j.OffenseViolatedStatute" ) )
//                .value( "judstat" ).ok()
//                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
//                .value( DaneCountySheriffs::getArrestSequenceID )
//                .ok()
//                .addProperty( new FullQualifiedName( "j.ArrestCategory" ) )
//                .value( "arsttyp" )
//                .ok()
//                .ok()
//                .ok()
//                .createAssociations()
//                .addAssociation( "arrestedin" )
//                .ofType( new FullQualifiedName( "lawenforcement.arrestedin" ) )
//                .to( "DCSOArrestedIn" )
//                .key( new FullQualifiedName( "j.ArrestSequenceID" ),
//                        new FullQualifiedName( "nc.SubjectIdentification" ) )
//                .fromEntity( "suspect" )
//                .toEntity( "arrest" )
//                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
//                .value( DaneCountySheriffs::getSubjectIdentification )
//                .ok()
//                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
//                .value( DaneCountySheriffs::getArrestSequenceID )
//                .ok()
//                .ok()
//                .ok()
//                .done();
//
//        Shuttle shuttle = new Shuttle( environment, jwtToken );
//        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
//
//        flights.put( flight, payload );
//
//        shuttle.launch( flights );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "birthd" );
        if ( dob == null ) {
            return null;
        }
        if ( dob.contains( "#" ) ) {
            return null;
        }
        return bdHelper.parse( dob );
    }

    public static String getArrestSequenceID( Row row ) {
        return "DCSO-" + row.getAs( "arstnum" ).toString().trim();
    }

    public static String getSubjectIdentification( Row row ) {
        return "DCSO-" + row.getAs( "nmmain" ).toString().trim();
    }
}
