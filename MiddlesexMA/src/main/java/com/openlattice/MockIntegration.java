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

package com.openlattice;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.MoreObjects;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MockIntegration {

    private static final Logger         logger         = LoggerFactory
            .getLogger( MockIntegration.class );
    private static final Environment    environment    = Environment.STAGING;
    private static final DateTimeHelper dtHelper       = new DateTimeHelper( TimeZones.America_NewYork,
            "MM/dd/yyyy" );
    private static final DateTimeHelper bdHelper       = new DateTimeHelper( TimeZones.America_NewYork,
            "MM/dd/yyyy" );
    private static final Pattern        nameMatcher    = Pattern.compile( "(.+), (.+) (.*) (.*)" );
    private static final Pattern        raceMatcher    = Pattern.compile( "(.+) - (.+)" );
    private static final Pattern        addressMatcher = Pattern.compile( "(.+)\\|(.+)" );
    private static final Pattern        chargeMatcher  = Pattern.compile( "(.+) - (.+) (.+)" );
    private static final Pattern        heightMatcher  = Pattern.compile( "([0-9])'([0-9]+)'' - ([0-9]+) LBS." );

    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String arrestsPath = args[ 0 ];
        final String chargesPath = args[ 1 ];
        final String jwtToken = args[ 2 ];
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
                .load( arrestsPath );

        Dataset<Row> chargeData = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( chargesPath );

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm,
                    retrofit.create( PermissionsApi.class ) );
            reem.ensureEdmElementsExist( requiredEdmElements );
        }

        Flight visits = Flight.newFlight()
                .createEntities()
                .addEntity( "patient" )
                .to( "MockPatients" )
                .ofType( new FullQualifiedName( "general.person" ) )
                .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .addProperty( "nc.SubjectIdentification" )
                .value( MockIntegration::getSubjectIdentification ).ok()
                .addProperty( "nc.PersonGivenName", "firstName" )
                .addProperty( "nc.PersonSurName", "lastName" )
                .addProperty( "nc.PersonRace", "race" )
                .addProperty( "nc.PersonEthnicity", "ethnicity" )
                .addProperty( "nc.PersonBirthDate" )
                .value( MockIntegration::safeDOBParse ).ok()
                .addProperty( "nc.PersonSex", "sex" )
                .endEntity()
                .addEntity( "visit" )
                .to( "MockVisit" )
                .ofType("patiet.visit" )
                .key( "general.stringid" )
                .addProperty("general.stringid", "id")
                .addProperty( "mock.reason", "VisitReason" )
                .addProperty( "mock.symptoms", "Symptoms" )
                .addProperty( "mock.drugpresent", "DrugPresent" )
                .addProperty( "mock.temp", "Temperature" )
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "patientvisited" )
                .ofType( "patient.visit" ).to( "MockPatientVisited" )
                .fromEntity( "patient" )
                .toEntity( "visit" )
                .key( "nc.SubjectIdentification" )
                .addProperty( "nc.SubjectIdentification" ).value( MockIntegration::getSubjectIdentification )
                .ok()
                .endAssociation()
                .endAssociations()
                .done();
        DateTime.now().dayOfWeek().get();
        Flight arrests = Flight.newFlight()
                .createEntities()
                .addEntity( "suspect" )
                .to( "MockSuspects" )
                .ofType( new FullQualifiedName( "general.person" ) )
                .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .addProperty( "nc.SubjectIdentification" )
                .value( MockIntegration::getSubjectIdentification ).ok()
                .addProperty( "nc.PersonGivenName", "firstName" )
                .addProperty( "nc.PersonSurName", "lastName" )
                .addProperty( "nc.PersonRace", "race" )
                .addProperty( "nc.PersonEthnicity", "ethnicity" )
                .addProperty( "nc.PersonBirthDate" )
                .value( MockIntegration::safeDOBParse ).ok()
                .addProperty( "nc.PersonSex", "sex" )
                .endEntity()
                .addEntity( "arrest" )
                .to( "MockArrest" )
                .ofType( "lawenforcement.arrest" )
                .key("j.ArrestSequenceID" )
                .addProperty( "j.ArrestSequenceID" ).value( MockIntegration::getArrestSequenceID )
                .ok()
                .addProperty("j.ArrestCategory", "ArrestCategory")
                .addProperty("j.CaseNumberText", "CaseNumberText")
                .addProperty("justice.EventType", "EventType")
                .addProperty("j.OffenseViolatedStatute","OffenseViolatedStatute")
                .addProperty("j.OffenseQualifierText","OffenseQualifierText")
                .addProperty("j.EnforcementOfficialBadgeIdentification", "EnforcementOfficialBadgeIdentification")
                .addProperty("justice.OffenderAlcohol", "OffenderAlcohol")
                .addProperty("publicsafety.OffenseDate")
                .value(row->dtHelper.parse( row.getAs("OffenseDate") ))
                .ok()
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation( "arrestedin" )
                .ofType( "lawenforcement.arrestedin" ).to( "MockSuspectArrestedIn" )
                .to( "MockSuspectArrestedIn" )
                .fromEntity( "suspect" )
                .toEntity( "arrest" )
                .key( "nc.SubjectIdentification", "j.ArrestSequenceID" )
                .addProperty( "nc.SubjectIdentification" ).value( MockIntegration::getSubjectIdentification )
                .ok()
                .addProperty( "j.ArrestSequenceID" ).value( MockIntegration::getArrestSequenceID )
                .ok()
                .endAssociation()
                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 2 );
        flights.put( visits, payload );
        flights.put( arrests, chargeData );

        shuttle.launch( flights );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "DOB" );
        if ( dob == null ) {
            return null;
        }
        return bdHelper.parse( dob.trim() );
    }

    public static String getSex( Row row ) {
        String sr = row.getAs( "sex_race" );
        if ( sr == null ) {
            return null;
        }

        Matcher m = raceMatcher.matcher( sr );
        if ( m.matches() ) {
            return m.group( 1 );
        }
        return null;
    }

    public static String getRace( Row row ) {
        String sr = row.getAs( "sex_race" );
        if ( sr == null ) {
            return null;
        }
        Matcher m = raceMatcher.matcher( sr );
        if ( m.matches() ) {
            return m.group( 2 );
        }
        return null;
    }

    public static String getFirstName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        Matcher m = nameMatcher.matcher( name );
        if ( m.matches() ) {
            logger.info( "Parsed first {} into: {}", name, m.group( 2 ) );
            return m.group( 2 );
        }
        return null;
    }

    public static String getMiddleName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        Matcher m = nameMatcher.matcher( name );
        if ( m.matches() ) {
            return m.group( 3 ) + "  " + m.group( 4 );
        }
        return null;
    }

    public static String getLastName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        Matcher m;
        try {
            m = nameMatcher.matcher( name );
        } catch ( Exception e ) {
            logger.error( "Unable to match: {}", name, e );
            return null;
        }
        if ( m.matches() ) {
            logger.info( "Parsed first {} into: {}", name, m.group( 1 ) );
            return m.group( 1 );
        }
        return null;
    }

    public static String getArrestAddress( Row row ) {
        String addr = row.getAs( "arrest_incident_addresses" );
        if ( addr == null ) {
            return null;
        }
        Matcher m = addressMatcher.matcher( addr );
        if ( m.matches() ) {
            return m.group( 1 );
        }
        return null;
    }

    public static String getIncidentAddress( Row row ) {
        String addr = row.getAs( "arrest_incident_addresses" );
        if ( addr == null ) {
            return null;
        }

        Matcher m = addressMatcher.matcher( addr );
        if ( m.matches() ) {
            return m.group( 2 );
        }
        return null;

    }

    public static String getOffenseViolatedStatute( Row row ) {
        String charge = row.getAs( "Charge" );
        if ( charge == null ) {
            return null;
        }
        Matcher m;
        try {
            m = chargeMatcher.matcher( charge );
        } catch ( Exception e ) {
            logger.error( "Unable to match: {}", charge, e );
            return null;
        }
        if ( m.matches() ) {
            return m.group( 2 );
        }
        return null;
    }

    public static String getOffenseQualifierText( Row row ) {
        String charge = row.getAs( "Charge" );
        if ( charge == null ) {
            return null;
        }
        Matcher m = chargeMatcher.matcher( charge );
        if ( m.matches() ) {
            return m.group( 3 );
        }
        return null;
    }

    public static Integer getHeight( Row row ) {
        String wh = row.getAs( "Height- Weight" );
        if ( wh == null ) {
            return null;
        }
        Matcher m = heightMatcher.matcher( wh );
        if ( m.matches() ) {
            int feet = Integer.parseInt( m.group( 1 ) );
            int inches = Integer.parseInt( m.group( 2 ) );
            return ( feet * 12 ) + inches;
        }
        return null;
    }

    public static Integer getWeight( Row row ) {
        String wh = row.getAs( "Height- Weight" );
        if ( wh == null ) {
            return null;
        }
        Matcher m = heightMatcher.matcher( wh );
        if ( m.matches() ) {
            return Integer.parseInt( m.group( 3 ) );
        }
        return null;
    }

    public static String getChargeReportedDate( Row row ) {
        String time = MoreObjects.firstNonNull( row.getString( 11 ), "12:00" );
        String reportedDate = row.getAs( "Rpt. Date Rpt. Time Rpt. Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getOffenseStartDate( Row row ) {
        String time = MoreObjects.firstNonNull( row.getString( 11 ), "12:00" );
        String reportedDate = row.getAs( "From Date From Time From Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getOffenseEndDate( Row row ) {
        String time = MoreObjects.firstNonNull( row.getString( 11 ), "12:00" );
        String reportedDate = row.getAs( "To Date To Time To Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getArrestSequenceID( Row row ) {
        return "MOCK-" + row.getAs( "ArrestSequenceID" ).toString().trim();
    }

    public static String getSubjectIdentification( Row row ) {
        return "MOCK-" + UUID.randomUUID();
    }
}
