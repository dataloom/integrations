package com.openlattice.integrations.wisconsin.danecounty;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

public class VeronaPoliceDept {
    private static final Logger logger          = LoggerFactory.getLogger( VeronaPoliceDept.class );
    private static final Environment environment = Environment.PRODUCTION;
    public static        String ENTITY_SET_NAME = "veronapd_dccjs";
    public static FullQualifiedName ARREST_AGENCY_FQN            = new FullQualifiedName( "j.ArrestAgency" );
    public static FullQualifiedName FIRSTNAME_FQN                = new FullQualifiedName( "nc.PersonGivenName" );
    //public static FullQualifiedName MIDDLENAME_FQN               = new FullQualifiedName( "nc.PersonMiddleName" );
    public static FullQualifiedName LASTNAME_FQN                 = new FullQualifiedName( "nc.PersonSurName" );
    public static FullQualifiedName SEX_FQN                      = new FullQualifiedName( "nc.PersonSex" );
    public static FullQualifiedName RACE_FQN                     = new FullQualifiedName( "nc.PersonRace" );
    public static FullQualifiedName ETHNICITY_FQN                     = new FullQualifiedName( "nc.PersonEthnicity" );
    public static FullQualifiedName DOB_FQN                      = new FullQualifiedName( "nc.PersonBirthDate" );
    public static FullQualifiedName OFFICER_ID_FQN               = new FullQualifiedName( "publicsafety.officerID" );
    public static FullQualifiedName ARREST_DATE_FQN              = new FullQualifiedName( "publicsafety.arrestdate" );

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
        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader.loadConfiguration( RequiredEdmElements.class );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );
        if( requiredEdmElements != null ) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm );
            reem.ensureEdmElementsExist( requiredEdmElements );
        }
        payload = payload.sample( false, .10 );
        // @formatter:off
/*
        Flight flight = Flight.newFlight()

                .addEntity( new FullQualifiedName( "publicsafety.jaildata" ) )
                    .to( ENTITY_SET_NAME )
                    .key( new FullQualifiedName( "general.guid" ) )
                    .addProperty( new FullQualifiedName( "general.guid" ) )
                        .value( row -> UUID.randomUUID() )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.firstname" ) )
                        .value( row -> getFirstName( row.getAs( "Name" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.lastname" ) )
                        .value( row -> getLastName( row.getAs( "Name" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "general.dob" ) )
                        .value( row -> row.getAs( "Date of Birth" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "publicsafety.datebooked" ) )
                        .value( row -> row.getAs( "Date Booked" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "publicsafety.datereleased" ) )
                        .value( row -> row.getAs( "Date Released" ) )
                        .ok()
                    .ok()
                .done();

        // @formatter:on
*/
    //    Shuttle shuttle = new Shuttle( jwtToken );
   //     shuttle.launch( flight, payload );*/
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