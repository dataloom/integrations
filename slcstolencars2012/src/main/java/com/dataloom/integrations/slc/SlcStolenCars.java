package com.dataloom.integrations.slc;

import java.io.File;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.storm.shade.com.google.common.collect.ImmutableSet;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.edm.internal.EntitySet;
import com.dataloom.edm.internal.EntityType;
import com.google.common.base.Optional;
import com.kryptnostic.shuttle.Flight;
import com.kryptnostic.shuttle.Shuttle;

import retrofit2.Retrofit;

public class SlcStolenCars {
    private static final SparkSession sparkSession;

    public static String              ES_NAME  = "slcstolencars2012";
    public static FullQualifiedName   ES_TYPE  = new FullQualifiedName( "publicsafety", "stolencars" );
    private static FullQualifiedName  CASE_FQN = new FullQualifiedName( "publicsafety", "case" );
    private static FullQualifiedName  OC_FQN   = new FullQualifiedName( "publicsafety", "offensecode" );
    private static FullQualifiedName  OD_FQN   = new FullQualifiedName( "publicsafety", "offensedescription" );
    private static FullQualifiedName  RD_FQN   = new FullQualifiedName( "publicsafety", "reportdate" );
    private static FullQualifiedName  OCC_FQN  = new FullQualifiedName( "publicsafety", "occdate" );
    private static FullQualifiedName  DOW_FQN  = new FullQualifiedName( "general", "dayofweek" );
    private static FullQualifiedName  ADDR_FQN = new FullQualifiedName( "general", "address" );
    private static FullQualifiedName  LAT_FQN  = new FullQualifiedName( "location", "latitude" );
    private static FullQualifiedName  LON_FQN  = new FullQualifiedName( "location", "longitude" );

    private static Pattern            p        = Pattern.compile( ".*\\n*.*\\n*\\((.+),(.+)\\)" );

    static {
        sparkSession = SparkSession.builder()
                .master( "local" )
                .appName( "test" )
                .getOrCreate();

    }

    public static void main( String[] args ) throws InterruptedException {
        String jwtToken = args[ 0 ];
        String path = new File( args[ 1 ] ).getAbsolutePath();
        Retrofit retrofit = RetrofitFactory.newClient( Environment.PRODUCTION, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );
        /*
         * UUID caseId = edm.createPropertyType( new PropertyType( CASE_FQN, "Case #", Optional.of(
         * "The case it was filed under" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) ); UUID ocId =
         * edm.createPropertyType( new PropertyType( OC_FQN, "Offense Code", Optional.of( "The code of the offense" ),
         * ImmutableSet.of(), EdmPrimitiveTypeKind.String ) ); UUID odId = edm.createPropertyType( new PropertyType(
         * OD_FQN, "Offense Description", Optional.of( "The description of the offense." ), ImmutableSet.of(),
         * EdmPrimitiveTypeKind.String ) ); UUID rdId = edm.createPropertyType( new PropertyType( RD_FQN, "Report Date",
         * Optional.of( "The day the car was reported stolen" ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) );
         * UUID occId = edm.createPropertyType( new PropertyType( OCC_FQN, "Offense Code Commited Date", Optional.of(
         * "I'm not really sure what this means." ), ImmutableSet.of(), EdmPrimitiveTypeKind.String ) ); UUID dowId =
         * edm.createPropertyType( new PropertyType( DOW_FQN, "Day of Week", Optional.of( "Day of week car was stolen"
         * ), ImmutableSet.of(), EdmPrimitiveTypeKind.Int16 ) ); UUID addrId = edm.createPropertyType( new PropertyType(
         * ADDR_FQN, "Day of Week", Optional.of( "Address the car was stolen from." ), ImmutableSet.of(),
         * EdmPrimitiveTypeKind.String ) ); UUID latId = edm.createPropertyType( new PropertyType( LAT_FQN, "Latitude",
         * Optional.of( "" ), ImmutableSet.of(), EdmPrimitiveTypeKind.Double ) ); UUID lonId = edm.createPropertyType(
         * new PropertyType( LON_FQN, "Longitude", Optional.of( "Longitude" ), ImmutableSet.of(),
         * EdmPrimitiveTypeKind.Double ) );
         */

        UUID caseId = edm.getPropertyTypeId( CASE_FQN.getNamespace(), CASE_FQN.getName() );
        UUID ocId = edm.getPropertyTypeId( OC_FQN.getNamespace(), OC_FQN.getName() );
        UUID odId = edm.getPropertyTypeId( OD_FQN.getNamespace(), OD_FQN.getName() );
        UUID rdId = edm.getPropertyTypeId( RD_FQN.getNamespace(), RD_FQN.getName() );
        UUID occId = edm.getPropertyTypeId( OCC_FQN.getNamespace(), OCC_FQN.getName() );
        UUID dowId = edm.getPropertyTypeId( DOW_FQN.getNamespace(), DOW_FQN.getName() );
        UUID addrId = edm.getPropertyTypeId( ADDR_FQN.getNamespace(), ADDR_FQN.getName() );
        UUID latId = edm.getPropertyTypeId( LAT_FQN.getNamespace(), LAT_FQN.getName() );
        UUID lonId = edm.getPropertyTypeId( LON_FQN.getNamespace(), LON_FQN.getName() );

        /*
         * UUID esId = edm.createEntityType( new EntityType( ES_TYPE, "Stolen cars in Salt Lake City",
         * "Stolen cars in Salt Lake City", ImmutableSet.of(), ImmutableSet.of( caseId ), ImmutableSet.of( caseId, ocId,
         * odId, rdId, occId, dowId, addrId, latId, lonId ) ) );
         
        UUID esId = edm.getEntityTypeId( ES_TYPE.getNamespace(), ES_TYPE.getName() );
        edm.createEntitySets( ImmutableSet.of( new EntitySet(
                ES_TYPE,
                esId,
                ES_NAME,
                "Salt Lake Lake City Stolen Cars (2012)",
                Optional.of( "All cars stolen in Salt Lake City in 2012." ) ) ) );
                */
        /*
         * Get the dataset.
         */

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( path );

        Flight flight = Flight.newFlight()
                .addEntity().to( ES_NAME ).as( ES_TYPE )
                .id( CASE_FQN )
                .addProperty().value( row -> row.getAs( "CASE" ) ).as( CASE_FQN ).ok()
                .addProperty().value( row -> row.getAs( "OFFENSE CODE" ) ).as( OC_FQN ).ok()
                .addProperty().value( row -> row.getAs( "OFFENSE DESCRIPTION" ) ).as( OD_FQN ).ok()
                .addProperty().value( row -> row.getAs( "REPORT DATE" ) ).as( RD_FQN ).ok()
                .addProperty().value( row -> row.getAs( "OCC DATE" ) ).as( OCC_FQN ).ok()
                .addProperty().value( row -> Integer.parseInt( row.getAs( "DAY OF WEEK" ) ) ).as( DOW_FQN ).ok()
                .addProperty().value( row -> row.getAs( "LOCATION" ) ).as( ADDR_FQN ).ok()
                .addProperty().value( SlcStolenCars::getLat ).as( LAT_FQN ).ok()
                .addProperty().value( SlcStolenCars::getLon ).as( LON_FQN ).ok()
                .ok()
                .done();

        Shuttle shuttle = new Shuttle( jwtToken );
        shuttle.launch( flight, payload );
    }

    public static double getLat( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        m.matches();
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 1 ) );
    }

    public static double getLon( Row row ) {
        String location = row.getAs( "LOCATION" );
        if ( StringUtils.isBlank( location ) ) {
            return 0D;
        }
        Matcher m = p.matcher( location );
        if ( !m.matches() ) {
            return 0D;
        }
        return Double.parseDouble( m.group( 2 ) );
    }
}
