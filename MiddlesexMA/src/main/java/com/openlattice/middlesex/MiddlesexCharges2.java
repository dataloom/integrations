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
import com.google.common.base.MoreObjects;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkState;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */
public class MiddlesexCharges2 {

    private static final Logger         logger         = LoggerFactory
            .getLogger( MiddlesexCharges2.class );
    private static final Environment    environment    = Environment.PRODUCTION;
    private static final DateTimeHelper dtHelper       = new DateTimeHelper( TimeZones.America_NewYork,
            "yyyy/MM/dd HH:mm" );
    private static final DateTimeHelper bdHelper       = new DateTimeHelper( TimeZones.America_NewYork,
            "yyyy/MM/dd" );
    private static final Pattern        nameMatcher    = Pattern.compile( "(.+), (.+) (.*) (.*)" ); //.* matches all char, even empty string
    private static final Pattern        raceMatcher    = Pattern.compile( "(.+) - (.+)" ); //.+ matches all char, must be at least 1
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
        final String incidents2016Path = args[ 1 ];
        final String incidents2017Path = args[ 2 ];
        final String jwtToken = args[ 3 ];

        //final SparkSession sparkSession = MissionControl.getSparkSession();
        SimplePayload payload = new SimplePayload( arrestsPath );
        SimplePayload incident16Data = new SimplePayload( incidents2016Path );
        SimplePayload incident17Data = new SimplePayload( incidents2017Path );

        //final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

//        Dataset<Row> payload = sparkSession
//                .read()
//                .format( "com.databricks.spark.csv" )
//                .option( "header", "true" )
//                .load( arrestsPath );
//
//        Dataset<Row> incident16Data = sparkSession
//                .read()
//                .format( "com.databricks.spark.csv" )
//                .option( "header", "true" )
//                .load( incidents2016Path );
//
//        Dataset<Row> incident17Data = sparkSession
//                .read()
//                .format( "com.databricks.spark.csv" )
//                .option( "header", "true" )
//                .load( incidents2017Path );


        Flight flight = Flight.newFlight()
                .createEntities()

                .addEntity( "arrestee" )
                .to( "LPDArrestSuspects" ) //for name/DOB unique IDs
                //.to("LPDArrestees")  //for UUID unique IDs
                    .addProperty( new FullQualifiedName( "nc.SubjectIdentification" ) )
    //                .value( row -> UUID.randomUUID().toString() )
    //                    .ok()
                    .value(row -> {
                        return Parsers.getAsString(row.getAs("Name")) + " " + Parsers.getAsString(row.getAs("DOB"));
                        }).ok()
                    //.value( MiddlesexCharges2::getSubjectIdentification ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonGivenName" ) )
                    .value( MiddlesexCharges2::getFirstName ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonMiddleName" ) )
                    .value( MiddlesexCharges2::getMiddleName ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonSurName" ) )
                    .value( MiddlesexCharges2::getLastName ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonNameSuffixText" ) )
                    .value( MiddlesexCharges2::getSuffixes ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonRace" ) )
                    .value( MiddlesexCharges2::getRace ).ok()
                    .addProperty("nc.PersonEthnicity")
                       .value( MiddlesexCharges2::getEthnicity ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonBirthDate" ) )
                    .value( MiddlesexCharges2::safeDOBParse ).ok()
                    .addProperty( new FullQualifiedName( "nc.PersonSex" ) )
                    .value( MiddlesexCharges2::getSex ).ok()
                    .addProperty( "nc.PersonHeightMeasure" ).value( MiddlesexCharges2::getHeight ).ok()
                    .addProperty( "nc.PersonWeightMeasure" ).value( MiddlesexCharges2::getWeight ).ok()
                .endEntity()

//                .addEntity( "arrestAddress" )
//                .to( "LPDAddresses" )
//                .addProperty( new FullQualifiedName( "location.address" ) )
//                .value( MiddlesexCharges2::getArrestAddress ).ok()
//                .endEntity()

                .addEntity( "incidentAddress" )
                .to( "LPDAddresses" )
                    .addProperty( new FullQualifiedName( "location.Address" ) )
                    .value( MiddlesexCharges2::getIncidentAddress ).ok()
                    .endEntity()

                    .addEntity("incident")
                    .to("LPDIncidents")
                         .addProperty( "location.address" ).value( MiddlesexCharges2::getIncidentAddress ).ok()
                         .addProperty("criminaljustice.incidentid").value( MiddlesexCharges2::getIncidentID ).ok()
                .endEntity()

                .addEntity("offense")
                    .to("LPDOffenses")
                        .addProperty("criminaljustice.offenseid").value( MiddlesexCharges2::getArrestSequenceID ).ok()
                        .addProperty("criminaljustice.localstatute", "Charge")
                .endEntity()

//                .addEntity( "charge" )
//                .to( "LPDCharge" )
//                .addProperty( "j.ChargeSequenceID", "Case Number" )
//                .addProperty( "j.OffenseViolatedStatute" )
//                .value( MiddlesexCharges2::getOffenseViolatedStatute ).ok()
//                .addProperty( "j.OffenseQualifierText" )
//                .value( MiddlesexCharges2::getOffenseQualifierText ).ok()
//                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "arrestedin" )
                    .to( "LPDArrestedIn" )
                    .fromEntity( "arrestee" )
                    .toEntity( "incident" )
                    .addProperty( "nc.SubjectIdentification" )
//                       .value( row -> UUID.randomUUID().toString() )
//                         .ok()
                       .value(row -> {
                       return Parsers.getAsString(row.getAs("Name")) + " " + Parsers.getAsString(row.getAs("DOB"));
                          }).ok()
                    .addProperty( "arrestedin.id" ).value( MiddlesexCharges2::getIncidentID )
                       .ok()
                    .addProperty( new FullQualifiedName( "arrest.date" ) )
                       .value( row -> dtHelper.parse( ( row.getAs( "arr_date" ) + " " + row.getAs( "Time" ) ).trim() ) )
                       .ok()
                    .addProperty( "criminaljustice.bookingnumber", "bookingnum" )
                    .addProperty( "arrest.category", "type_charge" )
                    .addProperty("criminaljustice.localstatute", "Charge")
                    .addProperty( "location.address" ).value( MiddlesexCharges2::getArrestAddress ).ok()
                .endAssociation()

                //incident occurred at address
                .addAssociation( "arrestedat" )
                .to( "LPDOccurredAt" )
                    .fromEntity( "incident" )
                    .toEntity( "incidentAddress" )
                    .addProperty( "general.stringid").value( MiddlesexCharges2::getIncidentID ).ok()
                    .addProperty( "location.address" ).value( MiddlesexCharges2::getIncidentAddress ).ok()
                .endAssociation()

//                .addAssociation( "occurredat" )
//                .to( "LPDOccurredAt" )
//                .fromEntity( "charge" )
//                .toEntity( "incidentAddress" )
//                .addProperty( "general.stringid", "Case Number" )
//                .addProperty( "location.address" ).value( MiddlesexCharges2::getIncidentAddress ).ok()
//                .endAssociation()

//                .addAssociation( "chargedwith" )
//                .to( "LPDChargedWith" )
//                .fromEntity( "suspect" )
//                .toEntity( "charge" )
//                .addProperty( "general.stringid" )
//                .value( row -> MoreObjects.firstNonNull( row.getAs( "bookingnum" ), "" ) + MoreObjects
//                        .firstNonNull( row.getAs( "Case Number" ), "" ) ).ok()
//                .endAssociation()
//
//                .addAssociation( "chargeappears" ) //change to offense appears in arrest
//                .to( "LPDChargeAppearsIn" )
//                .fromEntity( "offense" )
//                .toEntity( "arrest" )
//                .addProperty( "general.stringid" )
//                .value( row -> MoreObjects.firstNonNull( row.getAs( "Case Number" ), "" ) + MoreObjects
//                        .firstNonNull( row.getAs( "Charge" ), "" ) ).ok()
//                .endAssociation()

                .endAssociations()
                .done();

        Flight incidents = Flight.newFlight()
                .createEntities()
                .addEntity("incident")
                .to("LPDIncidents")
                    .useCurrentSync()
                    .addProperty( "location.address" ).value( MiddlesexCharges2::getIncidentAddress ).ok()
                    .addProperty("criminaljustice.incidentid").value( MiddlesexCharges2::getIncidentflightID ).ok()
                    .addProperty( "incident.reporteddatetime" )
                       .value( MiddlesexCharges2::getChargeReportedDate ).ok()
                    .addProperty( "incident.startdatetime" )
                       .value( MiddlesexCharges2::getOffenseStartDate ).ok()
                    .addProperty( "incident.enddatetime" )
                       .value( MiddlesexCharges2::getOffenseEndDate ).ok()
                    .addProperty( "criminaljustice.localstatute", "Class" )
                    .addProperty("criminaljustice.nibrs", "IBR Code Counts")
                    .addProperty("criminaljustice.incidentflag", "Flags")
                    .addProperty( "incident.narrative", "comments" )
                    .addProperty( "criminaljustice.casestatus", "Case Status" )
                    .addProperty( "location.address", "Location" )
                    .addProperty( "criminaljustice.beatsector", "Beat Sector Neighborhood" )
                .endEntity()

                .addEntity( "address" )
                .to( "LPDAddresses" )
                .useCurrentSync()
                .ofType( "general.Address" )
                .key( "location.address" )
                .addProperty( "location.address", "Location" )
                .addProperty( "location.name", "Beat Sector Neighborhood" )
                .endEntity()
                .endEntities().done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        SetMultimap<Flight, Payload> flights = HashMultimap.create();
        flights.put( flight, payload );
        flights.put( incidents, incident16Data );
        flights.put( incidents, incident17Data );

        shuttle.launchPayloadFlight( flights );
    }

    public static String safeDOBParse( Row row ) {
        String dob = row.getAs( "DOB" );
        if ( dob == null ) {
            return null;
        }
        return bdHelper.parse( dob.trim() );
    }

    public static String getSex( Row row ) {
        String sr = row.getAs("sex_race");
        if (sr == null) {
            return null;
        }

        Matcher m = raceMatcher.matcher(sr);
        if (m.matches()) {
            String sex = m.group(1).trim();
            if (sex.equals("MALE")) {
                return "M";
            } else if (sex.equals("FEMALE")) {
                return "F";
            } else if (sex.equals("N.A.")) {
                return "";
            }
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
            String race = m.group( 2 ).trim(); //capturing the race string. The .trim ensures that if there are any spaces around N.A., they will be trimmed.
            if (race.equals("N.A.")||race.equals("HISPANIC")) {
                return "";
            }
            else if ( race.equals("BLACK")) { return "black"; }
            else if ( race.equals("WHITE")) { return "white"; }
            else if ( race.equals("ASIAN")) { return "asian"; }
            else {
                return "";
            }
        }
        return null;
    }

    public static String getEthnicity( Row row ) {
        String sr = row.getAs( "sex_race" );
        if ( sr == null ) {
            return null;
        }

        Matcher m = raceMatcher.matcher( sr );
        if ( m.matches() ) {
            String race = m.group( 2 ).trim(); //capturing the race string. The .trim ensures that if there are any spaces around N.A., they will be trimmed.
            if (race.equals("HISPANIC")) {
                return "hispanic";
            }
            else {
                return "";
            }
        }
        return null;
    }


    static class PersonName {
        String first;
        String last;
        String middle;
        Set<String> suffixes;
    }

    //Double spaces below are important and specific to MA.
    static Set<String> suffixes = ImmutableSet.<String>builder()
            .add( " JR.", " SR.", " JR", " SR", " IV", " V", " VI", " III", " II", " I" )
            .add( "  JR.", "  SR.", "  JR", "  SR", "  IV", "  V", "  VI", "  III", "  II", "  I" )
            .build();
    public static PersonName splitName( String name ) {
        PersonName p = new PersonName();

        p.suffixes = new HashSet<>();
        for( String suffix : suffixes ) {
            if( name.contains( suffix ) ) {
                p.suffixes.add( suffix.trim() );
                name = name.replace( suffix, "");
            }
        }

        String[] pieces = StringUtils.split( name, ',');

        checkState( pieces.length >= 2 , "Must have at least two pieces");

        p.last = pieces[ 0 ].replace( "  ", " " ).trim();

        String[] remaining = StringUtils.split( pieces[1], ' ' );
        p.first = remaining[ 0 ];
        if( remaining.length > 1 ) {
            p.middle = StringUtils.join(Arrays.copyOfRange(remaining, 1, remaining.length));
        }
        return p;
    }

    public static Set<String> getSuffixes( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        PersonName p = splitName( name );
   //     logger.info( "Parsed suffixes {} -> {}", name, p.suffixes );
        return p.suffixes;
    }

    public static String getFirstName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        PersonName p = splitName( name );
 //       logger.info("Parsed first {} -> {}", name, p.first);
        return p.first;
//        Matcher m = nameMatcher.matcher( name );
//        if ( m.matches() ) {
//            logger.info( "Parsed first {} into: {}", name, m.group( 2 ) );
//            return m.group( 2 );
//        } else {
//            logger.info( "Unable to parse name: {}", name );
//        }
//        return null;
    }

    public static String getMiddleName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        PersonName p = splitName( name );
  //      logger.info("Parsed middle {} -> {}", name, p.middle);
        return p.middle;
//        Matcher m = nameMatcher.matcher( name );
//        if ( m.matches() ) {
//            String middle = m.group( 3 ) + "  " + m.group( 4 );
//            logger.info( "Parsed middle {} into: {}", name, middle );
//            return middle;
//        } else {
//            logger.info( "Unable to parse name: {}", name );
//        }
//        return null;
    }

    public static String getLastName( Row row ) {
        String name = row.getAs( "Name" );
        if ( name == null ) {
            return null;
        }
        PersonName p = splitName( name );
   //     logger.info("Parsed last {} -> {}", name, p.last);
        return p.last;
//        Matcher m;
//        try {
//            m = nameMatcher.matcher( name );
//        } catch ( Exception e ) {
//            logger.error( "Unable to match: {}", name, e );
//            return null;
//        }
//        if ( m.matches() ) {
//            logger.info( "Parsed last {} into: {}", name, m.group( 1 ) );
//            return m.group( 1 );
//        } else {
//            logger.info( "Unable to parse name: {}", name );
//        }
//        return null;
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
        String time = MoreObjects.firstNonNull( row.getAs("OLrep-time" ), "12:00" );
        String reportedDate = row.getAs( "Rpt. Date Rpt. Time Rpt. Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getOffenseStartDate( Row row ) {
        String time = MoreObjects.firstNonNull( row.getAs( "OLstart-time" ), "12:00" );
        String reportedDate = row.getAs( "From Date From Time From Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getOffenseEndDate( Row row ) {
        String time = MoreObjects.firstNonNull( row.getAs( "OLend-time" ), "12:00" );
        String reportedDate = row.getAs( "To Date To Time To Day" ) + " " + time;
        return dtHelper.parse( reportedDate.trim() );
    }

    public static String getArrestSequenceID( Row row ) {
        return "LPD-" + row.getAs( "bookingnum" ).toString().trim();
    }

    public static String getIncidentID( Row row ) {
        return "LPD-" + row.getAs( "Case Number" ).toString().trim();
    }

    public static String getIncidentflightID( Row row ) {
        return "LPD-" + row.getAs( "Case # Off. Seq." ).toString().trim();
    }

// the booking number was not unique to a person (if they were arrested more than once.
// public static String getSubjectIdentification( Row row ) {
//        return "LPD-" + row.getAs( "bookingnum" );
//    }
}
