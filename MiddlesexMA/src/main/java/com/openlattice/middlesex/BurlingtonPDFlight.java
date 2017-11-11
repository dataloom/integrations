package com.openlattice.middlesex;


import com.dataloom.client.RetrofitFactory;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.util.Parsers;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class BurlingtonPDFlight {

    private static final Logger logger = LoggerFactory.getLogger( BurlingtonPDFlight.class );

    //which environment to send integration to
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;

    //Parse dates correctly, from input string columns. Offset from UTC.
    private static final DateTimeHelper dtHelper = new DateTimeHelper(DateTimeZone
            .forOffsetHours(-4), "YYYYMMdd");
    private static final DateTimeHelper bdHelper = new DateTimeHelper(DateTimeZone
            .forOffsetHours(-4), "YYYYMMdd");

    public static void main( String[] args ) throws InterruptedException {
/*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        final SparkSession sparkSession = MissionControl.getSparkSession();

        //gets csv path, username, pwd, put them in build.gradle run.args. These are the positions in the string arg.
        final String path = args[0];        //added this line from Julia's tutorial.
        final String jwtToken = args[ 1 ];

        // Get jwtToken to verify data integrator has write permissions to dataset using username/pwd above.
//        final String jwtToken = MissionControl.getIdToken( username, password );
//        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        // Configure Spark to load and read your datasource
        Dataset<Row> payload = sparkSession.read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        //another payload, subset of the payload of the entire csv above. Arrest reports only
        Dataset<Row> arPayload = payload.filter( payload.col( "casenum" ).endsWith( "-AR" ) );
        long arTotal = arPayload.count();

        //offense reports only
        Dataset<Row> ofPayload = payload.filter( payload.col( "casenum" ).endsWith( "-OF" ) );
        long ofTotal = ofPayload.count();

        //3rd payload - subset of arrests that are arrestees
        Dataset<Row> ar2Payload = payload.filter( payload.col("PersonType").equalTo("Arrestee"));
        long ar2Total = ar2Payload.count();


        Map<Flight, Dataset<Row>> flights = new LinkedHashMap<>( 2 );
        Flight arFlight = Flight.newFlight()
            .createEntities()
                .addEntity("arincident")  //variable name within flight. Doesn't have to match anything anywhere else
                    .to("Burlington PD Incidents")       //name of entity set belonging to
                    .addProperty("general.StringID")
                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .addProperty("justice.offensereportid", "casenum")           //arrest report number
                    .addProperty("justice.offensenibrs", "ibrcode")
                    .addProperty("date.IncidentReportedDateTime")
                        .value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )   //these rows are shorthand for a full function .value as below
                        .ok()
                    //.addProperty("j.ArrestCategory","TypeOfArrest")
                    .endEntity()
//                .addEntity("arrest")
//                    .to("BurlingtonPDArrests")       //name of entity set belonging to
//                    .addProperty("j.ArrestSequenceID", "casenum")   //arrest report #
//                    .addProperty("justice.cadnumber")                       //CAD call #
//                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
//                    .addProperty("justice.offensenibrs", "ibrcode")
//                    .addProperty("publicsafety.OffenseDate")
//                        .value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )
//                        .ok()
//                    .addProperty("j.ArrestCategory","TypeOfArrest")
//                    .addProperty("location.address")
//                        .value(row -> {
//                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName")) + " "
//                                    + Parsers.getAsString(row.getAs("City")) + ", " + Parsers.getAsString(row.getAs("State"));
//                        })
//                        .ok()
//                    .endEntity()
                .addEntity("arpeople")
                    .to("BurlingtonPDJusticeInvolvedPeople")
                    .addProperty("nc.SubjectIdentification", "SSN")
                    .addProperty("nc.SSN", "SSN")
                    .addProperty("nc.PersonSurName", "lastname")
                    .addProperty("nc.PersonGivenName", "firstname")
                    .addProperty("nc.PersonBirthDate")
                        .value( row -> bdHelper.parse( row.getAs( "DOB" ) ) )
                        .ok()
                    .addProperty("nc.PersonRace", "race")
                    .addProperty("nc.PersonSex", "Sex")
                    .addProperty("justice.persontype", "PersonType")
                    .endEntity()
                .addEntity("araddress")
                    .to("BurlingtonPDIncidentAddresses")
                    .addProperty("location.Address")    //unique ID for address
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName")) + " "
                                    + Parsers.getAsString(row.getAs("City")) + ", " + Parsers.getAsString(row.getAs("State"));
                        })
                        .ok()
                    .addProperty("location.street")
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName"));
                        })
                        .ok()
                    .addProperty("location.city", "City")
                    .addProperty("location.state", "State")
                    .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation("Appears In")
                    .to("BurlingtonPDAppearsIn")
                    .fromEntity("arpeople")
                    .toEntity("arincident")
                    .addProperty("general.stringid").value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .endAssociation()
                .addAssociation("Occurred At")
                    .to("BurlingtonPDOccurredAt")
                    .fromEntity("arincident")
                    .toEntity("araddress")
                    .addProperty("general.stringid")
                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                .endAssociation()
            .endAssociations()
            .done();

        Flight ar2Flight = Flight.newFlight()
                .createEntities()
                    .addEntity("ar2incident")  //variable name within flight. Doesn't have to match anything anywhere else
                        .to("Burlington PD Incidents")       //name of entity set belonging to
                        .useCurrentSync()
                        .addProperty("general.StringID")
                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                        .addProperty("justice.offensereportid", "casenum")           //arrest report number
                        .addProperty("justice.offensenibrs", "ibrcode")
                        .addProperty("date.IncidentReportedDateTime")
                        .value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )   //these rows are shorthand for a full function .value as below
                        .ok()
                    .endEntity()
                    .addEntity("ar2arrest")
                        .to("BurlingtonPDArrests")       //name of entity set belonging to
                        .addProperty("j.ArrestSequenceID", "casenum")   //arrest report #
                        .addProperty("justice.cadnumber")                       //CAD call #
                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                        .addProperty("justice.offensenibrs", "ibrcode")
                        .addProperty("publicsafety.OffenseDate")
                        .value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )
                        .ok()
                        .addProperty("j.ArrestCategory","TypeOfArrest")
                        .addProperty("location.address")
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName")) + " "
                                    + Parsers.getAsString(row.getAs("City")) + ", " + Parsers.getAsString(row.getAs("State"));
                        })
                        .ok()
                    .endEntity()
                    .addEntity("ar2people")
                        .to("BurlingtonPDJusticeInvolvedPeople")
                        .useCurrentSync()
                        .addProperty("nc.SubjectIdentification", "SSN")
                        .addProperty("nc.SSN", "SSN")
                        .addProperty("nc.PersonSurName", "lastname")
                        .addProperty("nc.PersonGivenName", "firstname")
                        .addProperty("nc.PersonBirthDate")
                        .value( row -> bdHelper.parse( row.getAs( "DOB" ) ) )
                        .ok()
                        .addProperty("nc.PersonRace", "race")
                        .addProperty("nc.PersonSex", "Sex")
                        .addProperty("justice.persontype", "PersonType")
                    .endEntity()
                    .addEntity("ar2address")
                    .to("BurlingtonPDIncidentAddresses")
                    .useCurrentSync()
                        .addProperty("location.Address")    //unique ID for address
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName")) + " "
                                    + Parsers.getAsString(row.getAs("City")) + ", " + Parsers.getAsString(row.getAs("State"));
                        })
                        .ok()
                        .addProperty("location.street")
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName"));
                        })
                        .ok()
                        .addProperty("location.city", "City")
                        .addProperty("location.state", "State")
                    .endEntity()
                .endEntities()

                .createAssociations()
                    .addAssociation("Appears In2")
                    .to("BurlingtonPDAppearsIn")
                        .useCurrentSync()
                        .fromEntity("ar2people")
                        .toEntity("ar2incident")
                        .addProperty("general.stringid").value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .endAssociation()
                    .addAssociation("Arrested In")
                    .to("BurlingtonPDArrestedIn")
                        .fromEntity("ar2people")
                        .toEntity("ar2arrest")
                        .addProperty("j.ArrestSequenceID", "casenum")
                        .addProperty("nc.SubjectIdentification", "SSN")
                    .endAssociation()
                    .addAssociation("Occurred At2")
                    .to("BurlingtonPDOccurredAt")
                        .useCurrentSync()
                        .fromEntity("ar2incident")
                        .toEntity("ar2address")
                        .addProperty("general.stringid")
                        .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .endAssociation()
                .endAssociations()
                .done();


        Flight ofFlight = Flight.newFlight()
                .createEntities()
                    .addEntity("incident")  //variable name within flight. Doesn't have to match anything anywhere else
                        .to("Burlington PD Incidents")       //name of entity set belonging to
                        .useCurrentSync()
                        .addProperty("general.StringID")
                            .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                        .addProperty("justice.offensereportid", "casenum")           //offense report number
                        .addProperty("justice.offensenibrs", "ibrcode")
                        .addProperty("date.IncidentReportedDateTime").value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )
                        .ok()
                        //.addProperty("j.ArrestCategory","TypeOfArrest")
                    .endEntity()
//                    .addEntity("offense")
//                        .to("BurlingtonPDOffenses")
//                        .addProperty("justice.offensereportid", "casenum")
//                        .addProperty("justice.offensenibrs", "ibrcode")
//                        .addProperty("publicsafety.OffenseDate")
//                            .value( row -> dtHelper.parse( row.getAs( "reporteddate" ) ) )
//                            .ok()
//                    .endEntity()
                    .addEntity("ofpeople")
                        .to("BurlingtonPDJusticeInvolvedPeople")
                        .useCurrentSync()
                        .addProperty("nc.SubjectIdentification", "SSN")
                        .addProperty("nc.SSN", "SSN")
                        .addProperty("nc.PersonSurName", "lastname")
                        .addProperty("nc.PersonGivenName", "firstname")
                        .addProperty("nc.PersonBirthDate").value( row -> bdHelper.parse( row.getAs( "DOB" ) ) )
                        .ok()
                        .addProperty("nc.PersonRace", "race")
                        .addProperty("nc.PersonSex", "Sex")
                        .addProperty("justice.persontype", "PersonType")
                    .endEntity()
                    .addEntity("address")
                        .to("BurlingtonPDIncidentAddresses")
                        .useCurrentSync()
                        .addProperty("location.Address")    //unique ID for address
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName")) + " "
                                    + Parsers.getAsString(row.getAs("City")) + ", " + Parsers.getAsString(row.getAs("State"));
                        })
                        .ok()
                        .addProperty("location.street")
                        .value(row -> {
                            return Parsers.getAsString(row.getAs("StreetNum")) + " " + Parsers.getAsString(row.getAs("StreetName"));
                        })
                        .ok()
                        .addProperty("location.city", "City")
                        .addProperty("location.state", "State")
                    .endEntity()
                .endEntities()

                .createAssociations()
                    .addAssociation("OF Appears In")
                        .to("BurlingtonPDAppearsIn")
                        .useCurrentSync()
                        .fromEntity("ofpeople")
                        .toEntity("incident")
                        .addProperty("general.stringid")
                            .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .endAssociation()
                    .addAssociation("OF Occurred At")
                        .to("BurlingtonPDOccurredAt")
                        .useCurrentSync()
                        .fromEntity("incident")
                        .toEntity("address")
                        .addProperty("general.stringid")
                            .value( row -> getCadFromCasenum( row.getAs( "casenum" ) ) ).ok()
                    .endAssociation()
                .endAssociations()
                .done();


        // add all the flights to flights
        flights.put( arFlight, arPayload );
        flights.put( ar2Flight, ar2Payload );
        flights.put( ofFlight, ofPayload );

        // Send your flight plan to Shuttle and complete integration
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );


        //return flights;
    }

    // Custom Functions for Parsing CAD number from casenum - modified from functions for splitting first and last names
    public static String getCadFromCasenum( Object obj ) {
        String name = obj.toString();
        String pattern = "\\-[a-zA-Z]";
        String[] names = name.split( pattern );
        return names[ 0 ].trim();
        }


        }



