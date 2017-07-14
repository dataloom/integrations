package com.openlattice.integrations.wisconsin.danecounty;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.CharMatcher;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
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

import java.util.*;

/**
 * Created by mtamayo on 6/20/17.
 */
public class CaseChargesWithDispo2010 {
    private static final Logger                      logger       = LoggerFactory
            .getLogger( CaseChargesWithDispo2010.class );
    private static final RetrofitFactory.Environment environment  = RetrofitFactory.Environment.STAGING;
    public static        Base64.Encoder              encoder      = Base64.getEncoder();
    public static        Splitter                    nameSplitter = Splitter.on( " " ).omitEmptyStrings()
            .trimResults();
    private static final     DateTimeHelper              bdHelper     = new DateTimeHelper( DateTimeZone.forOffsetHours( -6 ), "MM/dd/YY");
    public static void main( String[] args ) throws InterruptedException {
        final String path = args[ 0 ];
        final String jwtToken = args[ 1 ];

        final SparkSession sparkSession = MissionControl.getSparkSession();

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
                .addEntity( "dacase" )
                .to( "DADaneCountyCases" )
                .key( new FullQualifiedName( "justice.dacasenumber" ) )
                .ofType( new FullQualifiedName( "justice.case" ) )
                .addProperty( new FullQualifiedName( "justice.dacasenumber" ) )
                .value( row -> row.getAs( "DA Case #" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.CaseStatus" ) )
                .value( row -> row.getAs( "DA Case Status" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.ReferralDate" ) )
                .value( row -> wrapParse( row,  "Ref Date" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.ReferralType" ) )
                .value( row -> row.getAs( "Ref Type" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.ReferralAgency" ) )
                .value( row -> row.getAs( "Ref Agency" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.AgencyCaseNumber" ) )
                .value( row -> row.getAs( "Agency Case #" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.courtcasenumber" ) )
                .value( row -> row.getAs( "Court Case #" ) ).ok()
                .addProperty( new FullQualifiedName( "justice.courtcasetype" ) )
                .value( row -> row.getAs( "CC# Type" ) ).ok()
                .ok()
                .addEntity( "defendant" )
                .to( "DaneCountyDADefendants" )
                .entityIdGenerator( row -> UUID.randomUUID().toString() )
                .ofType( new FullQualifiedName( "general.person" ) )
                .addProperty( new FullQualifiedName( "nc.PersonGivenName" ) )
                .value( CaseChargesWithDispo2010::getFirstName ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonMiddleName" ) )
                .value( CaseChargesWithDispo2010::getMiddleName ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSurName" ) )
                .value( CaseChargesWithDispo2010::getLastName ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
                .value( row -> bdHelper.parse( row.getAs( "Defendant DOB" )  ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonRace" ) )
                .value( row -> row.getAs( "Defendant Race" ) ).ok()
                .addProperty( new FullQualifiedName( "nc.PersonSex" ) )
                .value( row -> row.getAs( "Defendant Gender" ) ).ok()
                .addProperty( new FullQualifiedName( "person.age" ) )
                .value( row -> row.getAs( "Defendant's Age at Incident" ) ).ok()
                .ok()
                .ok()
                .createAssociations()
                .addAssociation( "appearsincase" )
                .to( "AppearsInDaDaneCountyCase" )
                .ofType( new FullQualifiedName( "general.appearsin" ) )
                .fromEntity( "defendant" )
                .toEntity( "dacase" )
                .entityIdGenerator( row -> UUID.randomUUID().toString() )
                .addProperty( new FullQualifiedName( "general.stringid" ) )
                .value( row -> "Workaround" ).ok()
                .ok()
                .ok()
                .done();
        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new LinkedHashMap<>( 2 );

        flights.put( flight, payload );
        shuttle.launch( flights );
    }

    public static String wrapParse( Row row , String field ) {
        try {
            return bdHelper.parse( row.getAs(field ) );
        } catch (IllegalArgumentException e ) {
            logger.info( "Problematic row: {}", row );
        }
        return null;
    }

    public static String getFirstName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        if( name == null ) return null;
        List<String> names = nameSplitter.splitToList( name );
        if( names .size() < 2  ) {
            return null;
        }
        Preconditions.checkState( names.size() > 1, "Must have at least some parts of name" );
        return names.get( 1 );
    }

    public static String getMiddleName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        if( name == null ) return null;
        name = name.replace( " Jr", "" );
        name = name.replace( " JR", "" );
        name = name.replace( " Sr", "" );
        name = name.replace( " SR", "" );

        List<String> names = nameSplitter.splitToList( name );
        Preconditions.checkState( names.size() > 0, "Must have at least some parts of name" );
        StringBuilder sb = new StringBuilder();
        for ( int i = 2; i < names.size(); ++i ) {
            sb.append( names.get( i ) ).append( " " );
        }

        return sb.toString().trim();
    }

    public static String getLastName( Row row ) {
        String name = row.getAs( "Defendant Name" );
        if ( StringUtils.isBlank( name ) ) {
            return null;
        }
        List<String> names = nameSplitter.splitToList( name );
        Preconditions.checkState( names.size() > 0, "Must have at least some parts of name" );

        if ( name.contains( " Jr" ) || name.contains( " JR" ) ) {
            return names.get( 0 ).trim().replace( ",","" ) + " Jr";
        }

        if ( name.contains( " Sr" ) || name.contains( " SR" ) ) {
            return names.get( 0 ).trim().replace( ",","" ) + " Sr";
        }

        return names.get( 0 ).replace( ",","" );
    }

    public static String getSubjectIdentification( Row row ) {
        String name = row.getAs( "Defendant Name" );
        String gender = row.getAs( "Defendant Gender" );
        String race = row.getAs( "Defendant Race" );
        String dob = row.getAs( "Defendant DOB" );

        StringBuilder sb = new StringBuilder();
        sb
                .append( encoder.encodeToString( name.getBytes( Charsets.UTF_8 ) ) )
                .append( "|" )
                .append( encoder.encodeToString( gender.getBytes( Charsets.UTF_8 ) ) )
                .append( "|" )
                .append( encoder.encodeToString( race.getBytes( Charsets.UTF_8 ) ) )
                .append( "|" )
                .append( encoder.encodeToString( dob.getBytes( Charsets.UTF_8 ) ) );
        return sb.toString();
    }
}
