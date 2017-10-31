package com.openlattice.middlesex;


import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.io.Resources;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import com.openlattice.shuttle.util.Parsers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

public class BurlingtonPDFlight {

    private static final Logger logger = LoggerFactory.getLogger( BurlingtonPDFlight.class );

    //which environment to send integration to
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException {
/*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        //Parse dates correctly, from input string columns. CHECK OFFSET HOURS
        private static final DateTimeHelper dtHelper          = new DateTimeHelper( DateTimeZone
                .forOffsetHours( -6 ), "YYYYMMdd" );
        private static final DateTimeHelper              bdHelper          = new DateTimeHelper( DateTimeZone
                .forOffsetHours( -6 ), "YYYYMMdd" );

        final SparkSession sparkSession = MissionControl.getSparkSession();

        //gets csv path, username, pwd, put them in build.gradle run.args. These are the positions in the string arg.
        final String path = args[0];        //added this line from Julia's tutorial.
        final String username = args[ 1 ];
        final String password = args[ 2 ];

        // Get jwtToken to verify data integrator has write permissions to dataset using username/pwd above.
        final String jwtToken = MissionControl.getIdToken( username, password );
        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //purpose:
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );

        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );


        // Configure Spark to load and read your datasource
        Dataset<Row> payload = sparkSession.read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );


        //load edm.yaml and ensure all EDM elements exist
        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }
         //all EDM elements should now exist, and we should be safe to proceed with the integration


        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        Flight flight = Flight.newFlight()
            .createEntities()
                .addEntity("incident")  //variable name within flight. Doesn't have to match anything in edm.yaml
                    .to("BurlingtonIncidents")       //name of entity set belonging to
                    .addProperty("general.StringID", "casenum")
                    .addProperty("publicsafety.OffenseNIBRS", "ibrcode")
                    .addProperty("date.IncidentReportedDateTime").value( row -> bdHelper.parse( row.getAs( "reporteddate" ) ) )   //these rows are shorthand for a full function .value as below
                        .ok()
                    .addProperty("j.ArrestCategory", "TypeOfArrest")
                    .endEntity()
                .addEntity("suspect")
                    .to("BurlingtonPeopleinCAD")
                    .addProperty("nc.SSN", "SSN")
                    .addProperty("nc.PersonSurName", "lastname")
                    .addProperty("nc.PersonGivenName", "firstname")
                    .addProperty("nc.PersonBirthDate").value( row -> bdHelper.parse( row.getAs( "DOB" ) ) )
                        .ok()
                    .addProperty("nc.PersonRace", "race")
                    .addProperty("nc.PersonSex", "Sex")
                    .endEntity()
                .addEntity("address")
                    .to("BurlingtonIncidentAddresses")
                .addProperty("location.Address")    //unique ID for address
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + Parsers.getAsString(row.getAs("StreetName"))
                                    + Parsers.getAsString(row.getAs("City")) + Parsers.getAsString(row.getAs("State"))
                        })
                        .ok()
                    .addProperty("location.street")
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName"))
                        })
                        .ok()
                   .addProperty("location.city", "City")
                    .addProperty("location.state", "State")
                //.addProperty("nonsuspects")       //break apart nonsuspects - can do by col factor? or something else.

        // At this point, your flight contains 1 table's worth of data
        // If you want to integrate more tables, create another flight (flight2) and
        // add the flight to flights
        flights.put( flight, payload );

        // Send your flight plan to Shuttle and complete integration
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

   // private static Dataset<Row> getPayloadFromCsv(final SparkSession sparkSession ) {

        //String csvPath = Resources.getResource( "DemoJustice9-28.csv" ).getPath();

        //return payload;
    }

