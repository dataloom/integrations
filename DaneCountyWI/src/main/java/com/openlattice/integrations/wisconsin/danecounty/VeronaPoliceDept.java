package com.openlattice.integrations.wisconsin.danecounty;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

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

public class VeronaPoliceDept {
    private static final Logger            logger            = LoggerFactory.getLogger( VeronaPoliceDept.class );
    private static final Environment       environment       = Environment.STAGING;
    private static final DateTimeHelper    dtHelper          = new DateTimeHelper( DateTimeZone.forOffsetHours( -6 ),
            "MM/dd/YY HH:mm" );
    private static final DateTimeHelper    bdHelper          = new DateTimeHelper( DateTimeZone.forOffsetHours( -6 ),
            "MM/dd/YY" );
    public static        FullQualifiedName ARREST_AGENCY_FQN = new FullQualifiedName( "j.ArrestAgency" );
    public static        FullQualifiedName FIRSTNAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    //public static FullQualifiedName MIDDLENAME_FQN               = new FullQualifiedName( "nc.PersonMiddleName" );
    public static        FullQualifiedName LASTNAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    public static        FullQualifiedName SEX_FQN           = new FullQualifiedName( "nc.PersonSex" );
    public static        FullQualifiedName RACE_FQN          = new FullQualifiedName( "nc.PersonRace" );
    public static        FullQualifiedName ETHNICITY_FQN     = new FullQualifiedName( "nc.PersonEthnicity" );
    public static        FullQualifiedName DOB_FQN           = new FullQualifiedName( "nc.PersonBirthDate" );
    public static        FullQualifiedName OFFICER_ID_FQN    = new FullQualifiedName( "publicsafety.officerID" );
    public static        FullQualifiedName ARREST_DATE_FQN   = new FullQualifiedName( "publicsafety.arrestdate" );

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
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm,
                    retrofit.create( PermissionsApi.class ) );
            reem.ensureEdmElementsExist( requiredEdmElements );
        }

        Flight flight = Flight.newFlight()
                .createEntities()
                .addEntity( "suspect" )
                .to( "VeronaArrestSuspects" )
                .ofType( new FullQualifiedName( "general.person" ) )
                .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .addProperty( new FullQualifiedName( "nc.PersonGivenName" ) )
                .value( row -> row.getAs( "SuspectFirstName" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSurName" ) )
                .value( row -> row.getAs( "SuspectLastName" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSex" ) )
                .value( row -> row.getAs( "SuspectGender" ) )
                .ok()
                .addProperty( new FullQualifiedName( "nc.PersonRace" ) )
                .value( row -> row.getAs( "SuspectRace" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonEthnicity" ) )
                .value( row -> row.getAs( "SuspectEthicity" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
                .value( row -> bdHelper.parse( row.getAs( "DateofBirth" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( VeronaPoliceDept::getSubjectIdentification ).ok()
                .ok()
                .addEntity( "arrest" )
                .to( "VeronaArrests" )
                .ofType( new FullQualifiedName( "lawenforcement.arrest" ) )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( VeronaPoliceDept::getArrestSequenceID )
                .ok()
                .addProperty( new FullQualifiedName( "publicsafety.ArrestDate" ) )
                .value( row -> dtHelper.parse( row.getAs( "arrestdate" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "publicsafety.OffenseDate" ) )
                .value( row -> dtHelper.parse( row.getAs( "IncidentDate" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.OffenseQualifierText" ) )
                .value( row -> row.getAs( "DescriptionofOffense" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.OffenseViolatedStatute" ) )
                .value( row -> row.getAs( "OffenseStatute" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.EnforcementOfficialBadgeIdentification" ) )
                .value( row -> row.getAs( "OfficerBadgeNumber" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestCategory" ) )
                .value( row -> row.getAs( "ArrestType" ) )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestLocation" ) )
                .value( row -> row.getAs( "IncidentAddress" ) )
                .ok()
                .ok()
                .ok()
                .createAssociations()
                .addAssociation( "arrestedin" )
                .ofType( new FullQualifiedName( "lawenforcement.arrestedin" ) )
                .to( "VeronaArrestedIn" )
                .key( new FullQualifiedName( "j.ArrestSequenceID" ),
                        new FullQualifiedName( "nc.SubjectIdentification" ) )
                .fromEntity( "suspect" )
                .toEntity( "arrest" )
                .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                .value( VeronaPoliceDept::getSubjectIdentification )
                .ok()
                .addProperty( new FullQualifiedName( "j.ArrestSequenceID" ) )
                .value( VeronaPoliceDept::getArrestSequenceID )
                .ok()
                .ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        shuttle.launch( flights );
    }

    public static String getArrestSequenceID( Row row ) {
        return row.getAs( "Agency" ) + "-" + row.getAs( "ArrestorCitationNumber" );
    }

    public static String getSubjectIdentification( Row row ) {
        return row.getAs( "Agency" ) + "-" + row.getAs( "SuspectUniqueIDforyourAgency" );
    }

    public static String getFirstName( Object obj ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[ 1 ].trim();
    }

    public static String getLastName( Object obj ) {
        String name = obj.toString();
        String[] names = name.split( "," );
        return names[ 0 ].trim();
    }
}