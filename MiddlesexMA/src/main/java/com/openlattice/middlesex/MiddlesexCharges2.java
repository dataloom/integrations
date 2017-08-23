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
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
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
public class MiddlesexCharges2 {

    private static final Logger                      logger            = LoggerFactory
            .getLogger( MiddlesexCharges2.class );
    private static final Environment environment       = Environment.STAGING;
    private static final DateTimeHelper              dtHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -5 ), "MM/dd/yyyy" );
    private static final DateTimeHelper              bdHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -5 ), "yyyy-MM-dd HH:mm:ss.SSS" );
    public static        String                      ENTITY_SET_NAME   = "veronapd_dccjs";
    public static        FullQualifiedName           ARREST_AGENCY_FQN = new FullQualifiedName( "j.ArrestAgency" );
    public static        FullQualifiedName           FIRSTNAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    //public static FullQualifiedName MIDDLENAME_FQN               = new FullQualifiedName( "nc.PersonMiddleName" );
    public static        FullQualifiedName           LASTNAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    public static        FullQualifiedName           SEX_FQN           = new FullQualifiedName( "nc.PersonSex" );
    public static        FullQualifiedName           RACE_FQN          = new FullQualifiedName( "nc.PersonRace" );
    public static        FullQualifiedName           ETHNICITY_FQN     = new FullQualifiedName( "nc.PersonEthnicity" );
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

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "suspect" )
                .useCurrentSync()
                .to( "MSOSuspects" )
                .ofType( new FullQualifiedName( "general.person" ) )
                .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( MiddlesexCharges2::getSubjectIdentification ).ok()
                .ok()
                .addEntity( "arrest" )
                .to( "MSOArrest" )
                .ofType( new FullQualifiedName( "lawenforcement.arrest" ) )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .addProperty( new FullQualifiedName( "publicsafety.ArrestDate" ) )
                .value( row -> dtHelper.parse( row.getAs( "dt_asg" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "publicsafety.ReleaseDate" ) )
                .value( row -> dtHelper.parse( row.getAs( "dt_rel" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.disposition" ) )
                .value( row -> row.getAs( "rel_com" ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.EventType" ) )
                .value( row -> row.getAs( "classif" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.OffenseViolatedStatute" ) )
                .value( row -> row.getAs( "maj_off" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( MiddlesexCharges2::getArrestSequenceID )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestCategory" ) )
                .value( row -> row.getAs( "arsttyp" ) )
                .ok()
                .ok()
                .ok()
                .createAssociations()
                .addAssociation( "arrestedin" )
                .ofType( new FullQualifiedName( "lawenforcement.arrestedin" ) )
                .to( "DCSOArrestedIn" )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ),
                        new FullQualifiedName( "nc.SubjectIdentification" ) )
                .fromEntity( "suspect" )
                .toEntity( "arrest" )
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( MiddlesexCharges2::getSubjectIdentification )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( MiddlesexCharges2::getArrestSequenceID )
                .ok()
                .ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        shuttle.launch( flights );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "dt_ob" );
        if ( dob == null ) {
            return null;
        }
        if ( dob.contains( "#" ) ) {
            return null;
        }
        return bdHelper.parse( dob );
    }

    public static String safeParse( Row row ) {
        String date = row.getAs( "Date" );
        String time = row.getAs( "Time" );
        if ( StringUtils.endsWith( date, "/10" ) ) {
            date = "2010";
        }
        if ( StringUtils.endsWith( date, "/11" ) ) {
            date = "2011";
        }
        if ( StringUtils.endsWith( date, "/12" ) ) {
            date = "2012";
        }
        if ( StringUtils.endsWith( date, "/13" ) ) {
            date = "2013";
        }
        if ( StringUtils.endsWith( date, "/14" ) ) {
            date = "2014";
        }
        if ( StringUtils.endsWith( date, "/15" ) ) {
            date = "2015";
        }
        if ( StringUtils.endsWith( date, "/16" ) ) {
            date = "2016";

        }
        if ( StringUtils.endsWith( date, "/17" ) ) {
            date = "2017";
        }
        if ( date.contains( "#" ) || time.contains( "#" ) ) {
            return null;
        }
        return dtHelper.parse( date + " " + time );
    }

    public static String getArrestSequenceID( Row row ) {
        return "DCSO-" + row.getAs( "arstnum" ).toString().trim();
    }

    public static String getSubjectIdentification( Row row ) {
        return row.getAs( "insno_a" ).toString().trim();
    }
}
