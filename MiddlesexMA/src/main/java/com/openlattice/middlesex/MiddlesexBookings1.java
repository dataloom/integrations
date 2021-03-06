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

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.dataloom.streams.StreamUtil;
import com.google.common.collect.ImmutableList;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.util.CsvUtil;
import com.openlattice.shuttle.util.Parsers;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;



/**
 * @author Matthew Tamayo-Rios &lt;matthew@openlattice.com&gt;
 */
public class MiddlesexBookings1 {

    private static final Logger                      logger      = LoggerFactory
            .getLogger( MiddlesexBookings1.class );
    private static final RetrofitFactory.Environment environment = Environment.PRODUCTION;
    private static final DateTimeHelper              dtHelper    = new DateTimeHelper(  TimeZones.America_NewYork,
            "MM/dd/yy");
    private static final DateTimeHelper              bdHelper    = new DateTimeHelper(  TimeZones.America_NewYork,
            "MM/dd/yy");


    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */

        final String path = args[ 0 ];
        final String jwtToken = args[ 1 ];

        //final SparkSession sparkSession = MissionControl.getSparkSession();
        //final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        Stream<Map<String, String>> payload = StreamUtil.stream( () -> {
            try {
                return CsvUtil.newDefaultMapper()
                        .readerFor( Map.class )
                        .with( CsvUtil.newDefaultSchemaFromHeader() )
                        .readValues( new File( path ) );
            } catch ( IOException e ) {
                logger.error( "Unable to read csv file", e );
                return ImmutableList.<Map<String, String>>of().iterator();
            }
        } );
//        Dataset<Row> payload = sparkSession
//                .read()
//                .format( "com.databricks.spark.csv" )
//                .option( "header", "true" )
//                .load( path );


        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( "arrestee" )
                    .to( "MSOSuspects" )
                    .key( new FullQualifiedName( "nc.SubjectIdentification" ) )
                    .addProperty( new FullQualifiedName( "nc.PersonGivenName" ) )
                        .value( row -> row.getAs( "f_name" ) ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonMiddleName" ) )
                        .value( row -> row.getAs( "m_name" ) ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonSurName" ) )
                        .value( row -> row.getAs( "l_name" ) ).ok()
                    .addProperty( new FullQualifiedName( "nc.SSN" ) )
                        .value( row -> row.getAs( "ssno" ) ).ok()
                    .addProperty( "nc.PersonSex" )
                        .value(row -> "M" ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonRace" ) )
                        .value( MiddlesexBookings1::standardRace ).ok()
                    .addProperty("nc.PersonEthnicity")
                        .value( MiddlesexBookings1::standardEthnicity ).ok()
                    .addProperty( new FullQualifiedName( "nc.MaritalStatus" ) )
                        .value( row -> row.getAs( "marit" ) ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
                        .value( MiddlesexBookings1::safeDOBParse ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonBirthPlace" ) )
                        .value( row -> row.getAs( "birth" ) ).ok()
                    .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                        .value( MiddlesexBookings1::getSubjectIdentification ).ok()
                    .addProperty("nc.PersonEyeColorText", "eyes")
                    .addProperty("nc.PersonHairColorText", "hair")
                    .addProperty("nc.PersonWeightMeasure", "wgt")
                    .addProperty("nc.PersonHeightMeasure", "hgt")
                    .addProperty("nc.complexion", "cmplx")
                .endEntity()
                .addEntity( "address" )
                    .to( "MSOAddresses" )
                    .key( new FullQualifiedName("location.Address"))
                    .addProperty("location.Address")
                    .value(row -> {
                        return Parsers.getAsString(row.getAs("addr")) + " " + Parsers.getAsString(row.getAs("city")) + ", "
                                + Parsers.getAsString(row.getAs("state")) + " " + Parsers.getAsString(row.getAs("zip"));
                    })
                        .ok()
                    .addProperty( new FullQualifiedName( "location.street" ) )
                        .value( row -> row.getAs( "addr" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "location.city" ) )
                        .value( row -> row.getAs( "city" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "location.state" ) )
                        .value( row -> row.getAs( "state" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "location.zip" ) )
                        .value( row -> row.getAs( "zip" ) )
                        .ok()
                    .ok()
                .addEntity( "booking" )
                    .to( "MSOBookings" )
                    .key( new FullQualifiedName( "j.CaseNumberText" ) )
                    .addProperty( new FullQualifiedName( "justice.ReferralDate" ) )
                        .value( row -> dtHelper.parse( row.getAs( "dt_asg" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "publicsafety.ReleaseDate" ) )
                        .value( row -> dtHelper.parse( row.getAs( "dt_rel" ) ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "justice.ReleaseComments" ) )
                        .value( row -> row.getAs( "rel_com" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "justice.Bail" ) )
                        .value( row -> row.getAs( "bail" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "j.OffenseViolatedStatute" ) )
                        .value( row -> row.getAs( "maj_off" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "j.CaseNumberText" ) )
                        .value( row -> row.getAs( "dockno" ) )
                        .ok()
                    .addProperty( new FullQualifiedName( "j.ArrestAgency" ) )
                        .value( row -> row.getAs( "ar_agen" ) )
                        .ok()
                    .addProperty("justice.ReleaseType", "rel_type")
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "bookedin" )
                .ofType( new FullQualifiedName( "general.appearsin" ) )
                    .to( "MSOBooked" )
                    .fromEntity( "arrestee" )
                    .toEntity( "booking" )
                    .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
                        .value( MiddlesexBookings1::getSubjectIdentification )
                        .ok()
//                    .addProperty( new FullQualifiedName( "j.CaseNumberText" ) )
//                    .value( row -> row.getAs( "dockno" ) )
//                    .ok()
                .ok()
                .addAssociation( "livesat" )
                    .ofType( new FullQualifiedName( "location.livesat" ) )
                    .to( "MSOLivesAt" )
                    .key( new FullQualifiedName( "general.stringid" ) )
                    .fromEntity( "arrestee" )
                    .toEntity( "address" )
                    .addProperty( "general.stringid")
                        .value( MiddlesexBookings1::getSubjectIdentification ).ok()
//                    .value( row -> UUID.randomUUID().toString() )
//                    .ok()
                .ok()
                .endAssociations()

                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Stream<Map<String, String>>>  flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        shuttle.launch( flights );
    }

    public static String safeDateParse( Object obj ) {
        if ( obj == null ) return null;
        String date = obj.toString();
        if ( date.equals( "  -   -" ) ) {
            return null;
        }
        return dtHelper.parse( date );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "dt_ob" );
        if ( dob == null ) {
            return null;
        }
        if ( dob.contains( "#" ) ) {
            return null;
        }
        if ( dob.equals( "  -   -" ) ) {
            return null;
        }
        return bdHelper.parse( dob );
    }

    public static String getSubjectIdentification( Row row ) {
        return row.getAs( "insno_a" ).toString().trim();
    }

    public static String standardRace( Row row ) {
        String sr = row.getAs("race");
        if ( sr != null ) {
            //String name = row.toString();
            if ( sr.equals("ASN")) { return "asian"; }
                else if ( sr.equals("BLA")) { return "black"; }
                   else if ( sr.equals("CAU")) { return "white"; }
                      else if ( sr.equals("IND")) { return "amindian"; }
                         else if ( sr.equals("OTR")) { return "other"; }
                            else if ( sr.equals("")) { return ""; }
                               else if (sr.equals("HSP")) { return "";}
             }
        return null;}

     public static String standardEthnicity( Row row ) {
        String sr = row.getAs("race");
        if (sr != null) {
                //String name = row.toString();
                if (sr.equals("HSP")) { return "hispanic"; }
            }
         return null;
      }

}
