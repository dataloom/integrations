package com.openlattice.integrations.wisconsin.danecounty;

import com.dataloom.client.RetrofitFactory;
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
import org.apache.commons.lang.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by mtamayo on 6/19/17.
 */
public class MadisonPoliceDept {

    private static final Logger                      logger            = LoggerFactory
            .getLogger( MadisonPoliceDept.class );
    private static final RetrofitFactory.Environment environment       = RetrofitFactory.Environment.STAGING;
    private static final DateTimeHelper              dtHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "MM/dd/yyyy hh:mm:ss aa" );
    private static final DateTimeHelper              bdHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "MM/dd/yyyy" );
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
        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm );
            reem.ensureEdmElementsExist( requiredEdmElements );
        }

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "suspect" )
                .to( "MadisonArrestSuspects" )
                .ofType( new FullQualifiedName( "general.person" ) )
                .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .addProperty( new FullQualifiedName( "nc.PersonGivenName" ) )
                .value( row -> row.getAs( "FirstName" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSurName" ) )
                .value( row -> row.getAs( "LastName" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSex" ) )
                .value( row -> row.getAs( "Sex" ) )
                .ok()
                .addProperty( new FullQualifiedName( "nc.PersonRace" ) )
                .value( row -> row.getAs( "Race" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonEthnicity" ) )
                .value( row -> row.getAs( "Ethnicty" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
                .value( MadisonPoliceDept::safeDOBParse)
                .ok()
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( MadisonPoliceDept::getSubjectIdentification ).ok()
                .ok()
                .addEntity( "arrest" )
                .to( "MadisonArrests" )
                .ofType( new FullQualifiedName( "lawenforcement.arrest" ) )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .addProperty( new FullQualifiedName( "publicsafety.ArrestDate" ) )
                .value( MadisonPoliceDept::safeParse )
                .ok()
                .addProperty( new FullQualifiedName( "j.OffenseQualifierText" ) )
                .value( row -> row.getAs( "Description" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestLocation" ) )
                .value( row -> row.getAs( "Address" ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.OffenderAlcohol" ) )
                .value( row -> row.getAs( "OffenderAlcohol" ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.VictimAlcohol" ) )
                .value( row -> row.getAs( "VictimAlcohol" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( MadisonPoliceDept::getArrestSequenceID )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestCategory" ) )
                .value( row -> row.getAs( "ArrestType" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.EnforcementOfficialBadgeIdentification" ) )
                .value( row -> row.getAs( "IBM" ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.weapons" ) )
                .value( row -> row.getAs( "Weapons" ) )
                .ok()
                .ok()
                .ok()
                .createAssociations()
                .addAssociation( "arrestedin" )
                .ofType( new FullQualifiedName( "lawenforcement.arrestedin" ) )
                .to( "MadisonArrestedIn" )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ),
                        new FullQualifiedName( "nc.SubjectIdentification" ) )
                .fromEntity( "suspect" )
                .toEntity( "arrest" )
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( MadisonPoliceDept::getSubjectIdentification )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( MadisonPoliceDept::getArrestSequenceID )
                .ok()
                .ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        shuttle.launch( flights );
    }

    public static String safeDOBParse(Row row ) {
        String dob = row.getAs("DOB");
        if (dob == null ) {
            return null;
        }
        if( dob.contains( "#" ) ) {
            return null;
        }
        return bdHelper.parse( row.getAs( "DOB" ) );
    }
    public static String safeParse( Row row ) {
        String date = row.getAs( "Date" );
        String time = row.getAs( "Time" );
        if( StringUtils.endsWith(date,"/10") ) {
            date = "2010";
        }
        if( StringUtils.endsWith(date,"/11") ) {
            date = "2011";
        }
        if( StringUtils.endsWith(date,"/12") ) {
            date = "2012";
        }
        if( StringUtils.endsWith(date,"/13") ) {
            date = "2013";
        }
        if( StringUtils.endsWith(date,"/14") ) {
            date = "2014";
        }
        if( StringUtils.endsWith(date,"/15") ) {
            date = "2015";
        }
        if( StringUtils.endsWith(date,"/16") ) {
            date = "2016";

        }
        if( StringUtils.endsWith(date,"/17") ) {
            date = "2017";
        }
        if ( date.contains( "#" ) || time.contains( "#" ) ) {
            return null;
        }
        return dtHelper.parse( date + " " + time );
    }

    public static String getArrestSequenceID( Row row ) {
        return "Madison-" + row.getAs( "Arrest #" );
    }

    public static String getSubjectIdentification( Row row ) {
        return "Madison-" + row.getAs( "Jacket #" );
    }
}
