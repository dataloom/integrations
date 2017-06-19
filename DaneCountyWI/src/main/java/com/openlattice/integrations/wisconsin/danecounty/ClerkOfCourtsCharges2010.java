package com.openlattice.integrations.wisconsin.danecounty;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.commons.codec.binary.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by mtamayo on 6/19/17.
 */
public class ClerkOfCourtsCharges2010 {
    private static final Logger                      logger            = LoggerFactory
            .getLogger( VeronaPoliceDept.class );
    private static final RetrofitFactory.Environment environment       = RetrofitFactory.Environment.LOCAL;
    private static final DateTimeHelper              dtHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "MM/dd/YY HH:mm" );
    private static final DateTimeHelper              bdHelper          = new DateTimeHelper( DateTimeZone
            .forOffsetHours( -6 ), "MM/dd/YY" );
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
    public static        Base64.Encoder              encoder           = Base64.getEncoder();
    public static        Splitter                    nameSplitter      = Splitter.on( " " ).omitEmptyStrings()
            .trimResults();

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

        Dataset<Row> charges = sparkSession
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
                .addEntity( "charge" )
                .to( "DaneCountyCharges" )
                .useCurrentSync()
                .ofType( new FullQualifiedName( "justice.charge" ) )
                .key( new FullQualifiedName( "justice.ArrestTrackingNumber" ) )
                .addProperty( new FullQualifiedName( "justice.ArrestTrackingNumber" ) )
                .value( row -> row.getAs( "ATN" ) ).ok()
                .addProperty( new FullQualifiedName( "publicsafety.ArrestDate" ) )
                .value( row -> bdHelper.parse( row.getAs( "Incident Date" ) ) ).ok()
                .addProperty( new FullQualifiedName( "justice.plea" ) )
                .value( row -> row.getAs( "Plea" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.ReferralDate" ) )
                .value( row -> bdHelper.parse( row.getAs( "Referral Date" ) ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.disposition" ) )
                .value( row -> row.getAs( "Dispo" ) ).ok()
                .addProperty( new FullQualifiedName( "j.OffenseQualifierText" ) )
                .value( row -> row.getAs( "Description" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.severity" ) )
                .value( row -> row.getAs( "Severity" ) )
                .ok()
                .addProperty( new FullQualifiedName( "justice.SentencingConditionsMet" ) )
                .value( row -> row.getAs( "Sentencing Conditions Met" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.ChargeStatus" ) )
                .value( row -> row.getAs( "Charge Status" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.EventType" ) )
                .value( row -> row.getAs( "Event type" ) ).ok()
                .ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>( 2 );

        flights.put( flight, charges );

        shuttle.launch( flights );
    }

    public static String getFirstName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        List<String> names = nameSplitter.splitToList( name );
        Preconditions.checkState( names.size() > 0, "Must have at least some parts of name" );
        return names.get( 0 ) + " " + names.get( 1 );
    }

    public static String getMiddleName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        List<String> names = nameSplitter.splitToList( name );
        Preconditions.checkState( names.size() > 0, "Must have at least some parts of name" );
        if ( names.size() > 2 ) {
            return names.get( 1 );
        }
        return null;
    }

    public static String getLastName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        List<String> names = nameSplitter.splitToList( name );
        Preconditions.checkState( names.size() > 0, "Must have at least some parts of name" );
        return names.get( names.size() - 1 );
    }

    public static String getSubjectIdentification( Row row ) {
        String name = row.getAs( "Defendant Name" );
        String gender = row.getAs( "Gender" );
        String race = row.getAs( "Race" );
        String dob = row.getAs( "DOB" );

        StringBuilder sb = new StringBuilder();
        sb
                .append( encoder.encodeToString( StringUtils.getBytesUtf8( name ) ) )
                .append( "|" )
                .append( encoder.encodeToString( StringUtils.getBytesUtf8( gender ) ) )
                .append( "|" )
                .append( encoder.encodeToString( StringUtils.getBytesUtf8( race ) ) )
                .append( "|" )
                .append( encoder.encodeToString( StringUtils.getBytesUtf8( dob ) ) );
        return sb.toString();
    }

    public static String getArrestSequenceID( Row row ) {
        return row.getAs( "Agency" ) + "-" + row.getAs( "ArrestorCitationNumber" );
    }
}
