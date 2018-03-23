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

package com.openlattice.integrations.baltimore;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.edm.EdmApi;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;
import javax.swing.text.html.parser.Parser;
import java.util.*;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class HistoricalBHR {

    private static final Logger         logger      = LoggerFactory
            .getLogger( HistoricalBHR.class );

    private static final Environment    environment = RetrofitFactory.Environment.PRODUCTION;

    private static final DateTimeHelper dtHelper    = new DateTimeHelper( TimeZones.America_NewYork,
            "MM/dd/YY HHmm 'hrs'", "MM/dd/YY HHmm 'Hrs'", "MM/dd/YY HHmm 'hrs.'", "MM/dd/YY HHmm 'hr.'", "MM/dd/YY HHmm 'h'", "MM/dd/YY HHmm'hrs'", "MM/dd/YY HHmm'hrs.'",
            "MM/dd/YY HHmm", "MM/dd/YY HHmm 'Hrs.'", "MM/dd/YY HHmm 'hr.s'", "MM/dd/YY HHmm 'hr'", "MM-dd-YY HHmm'hr'", "MM/dd/YY HHmm'hr.'", "MM/dd/YY HHmm'h'",
            "MM-dd-YY HHmm 'hrs'", "MM-dd-YY HHmm 'hrs.'", "MM-dd-YY HHmm'hrs'", "MM/dd/YY HHmm 'hours'", "MM/ddYY HHmm 'hrs'", "MM/dd/YYYY", "MM/dd/YY");
    private static final DateTimeHelper bdHelper    = new DateTimeHelper( TimeZones.America_NewYork,
            "MM/dd/YY" );


    public static void main( String[] args ) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String bhrPath = args[ 0 ];
        final String testPath = args[ 1 ];
        final String followupPath = args[ 2 ];
        final String jwtToken = args[ 3 ];

        SimplePayload payload = new SimplePayload( bhrPath );
        SimplePayload tpayload = new SimplePayload( testPath );
        SimplePayload fpayload = new SimplePayload( followupPath );


        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        Flight bhrflight = Flight.newFlight()
                .createEntities()

                .addEntity( "historicalbhr" )
                   // .to( "BaltimoreHistoricalBHR" )     //test run
                    .to("baltimore_city_pd_bhr")
                        .addProperty( "nc.SubjectIdentification", "ConsumerID" )
                        .addProperty( "bhr.dispatchReason", "X1.Reason" )
                        .addProperty( "bhr.complaintNumber", "X2.CC" )
                        .addProperty( "bhr.companionOffenseReport" )
                            .value( row -> stringtoBool(  row.getAs("X3.Companion"))).ok()
                        .addProperty( "bhr.incident", "X4.Crime" )
                        .addProperty( "bhr.locationOfIncident", "X5.Address" )
                        .addProperty( "bhr.unit", "X6.Unit" )
                        .addProperty( "bhr.postOfOccurrence", "X7.Post" )     //change from int to string
                        .addProperty( "bhr.cadNumber").value( row -> Parsers.parseInt( row.getAs( "X8.CAD" ) )).ok()
                        .addProperty( "bhr.onView")
                            .value( row -> stringtoBool(  row.getAs("X9.OnView"))).ok()
                        .addProperty( "bhr.timeOccurred" )
                            .value(  row -> dtHelper.parseTime( row.getAs( "X10.Occurred" ) ) ).ok()
                        .addProperty( "bhr.dateOccurred" )
                            .value(  row -> dtHelper.parseDate( row.getAs( "X10.Occurred" ) ) ).ok()
                        .addProperty( "bhr.timeReported" )
                            .value(  row -> dtHelper.parseTime( row.getAs( "X11.Reported" ) ) ).ok()
                        .addProperty( "bhr.dateReported" )
                            .value(  row -> dtHelper.parseDate( row.getAs( "X11.Reported" ) ) ).ok()
                        .addProperty( "bhr.address", "X13a.Address" )
                        .addProperty( "bhr.phone")
                            .value( row -> getPhoneNumber( row.getAs( "X13b.Phone" )) ).ok()
                        .addProperty( "bhr.militaryStatus", "X14.Military" )
                        .addProperty( "bhr.gender", "X14a.Gender" )
                        .addProperty( "bhr.age").value( row -> Parsers.parseInt( row.getAs( "X14c.Age" ) ) ).ok()
                        .addProperty( "bhr.homeless")
                            .value( row -> stringtoBool(  row.getAs("X15.Homeless"))).ok()
                        .addProperty( "bhr.homelessLocation", "X15a.Location" )
                        .addProperty( "bhr.drugsAlcohol", "X16.Substances" )
                        .addProperty( "bhr.drugType")
                            .value( row -> getDrugs( row.getAs(  "X16a.Drugs"  ))).ok()
                        .addProperty( "bhr.prescribedMedication", "X17.Meds" )
                        .addProperty( "bhr.takingMedication", "X17a.Taking" )
                        .addProperty( "bhr.prevPsychAdmission", "X18.PrevPsych" )
                        .addProperty( "bhr.selfDiagnosis", "X19.SelfDiags" )
                        .addProperty( "bhr.selfDiagnosisOther", "X19a.Other" )
                        .addProperty( "bhr.armedWithWeapon")
                            .value( row -> stringtoBool(  row.getAs("X20.Weapon"))).ok()
                        .addProperty( "bhr.armedWeaponType", "X20a.WeaponType" )
                        .addProperty( "bhr.accessToWeapons")
                            .value( row -> stringtoBool( row.getAs ( "X21.Access") ) ).ok()
                        .addProperty( "bhr.accessibleWeaponType", "X21a.WeaponType" )
                        .addProperty( "bhr.observedBehaviors", "X22.Behaviors" )
                        .addProperty( "bhr.observedBehaviorsOther", "X22a.Other" )
                        .addProperty( "bhr.emotionalState", "X22b.EmotionalState" )
                        .addProperty( "bhr.emotionalStateOther")
                            .value( row -> getemotionalStateOther(row)).ok()
                        .addProperty( "bhr.photosTakenOf", "X23.Photos" )
                        .addProperty( "bhr.injuries", "X24.Injuries" )
                        .addProperty( "bhr.injuriesOther", "X24a.Other" )
                        .addProperty( "bhr.suicidal")
                            .value( row -> stringtoBool( row.getAs( "X25.Suicidal" ) ) ).ok()
                        .addProperty( "bhr.suicidalActions", "X25a.Type" )
                        .addProperty( "bhr.suicideAttemptMethod", "X26.Method" )
                        .addProperty( "bhr.suicideAttemptMethodOther", "X26a.Other" )
                        .addProperty( "bhr.complainantName", "X27.Complainant" )
                        .addProperty( "bhr.complainantAddress", "X27a.Address" )
                        .addProperty( "bhr.complainantConsumerRelationship", "X27b.Relationship" )
                        .addProperty( "bhr.complainantPhone").value( row -> getPhoneNumber( row.getAs( "X27c.Phone" ) ) ).ok()
                        .addProperty( "bhr.disposition", "X28.Disposition" )
                        .addProperty( "bhr.provider", "X28a.Provider" )
                        .addProperty( "bhr.hospitalTransport" )
                            .value( row -> stringtoBool( row.getAs( "X28b.Hospital" ) ) ).ok()
                        .addProperty( "bhr.hospital", "X28c.HospitalName" )
                        .addProperty( "bhr.deescalationTechniques", "X29.Deescalation" )
                        .addProperty( "bhr.deescalationTechniquesOther" )
                            .value( row -> getdeescalationTechniquesOther(row)).ok()
                        .addProperty( "bhr.specializedResourcesCalled", "X30.Specialized" )
                        .addProperty( "bhr.incidentNarrative", "X31.Narrative" )
                        .addProperty( "bhr.officerName", "X32.Officer" )
                        .addProperty( "bhr.officerSeqID", "X34.SEQ" )
                        .addProperty( "bhr.officerInjuries", "X35.Injuries" )
                        .addProperty( "bhr.officerCertification", "X36.Certification" )
                        .addProperty( "bhr.supervisor", "X37.Supervisor" )
                        .addProperty( "bhr.supervisorID", "X37a.SEQ" )
                .endEntity()
                .addEntity( "people" )
                    //.to("Baltimore_ppl_test")       //test run
                      .to("baltimore_city_pd_people")
                        .addProperty( "nc.SubjectIdentification", "ConsumerID" )
                //.value( row -> UUID.randomUUID().toString() ).ok()
                        .addProperty("nc.PersonGivenName", "firstname")
                        .addProperty("nc.PersonSurName", "lastname")
                        .addProperty( "nc.PersonSuffix", "suffix" )
                        .addProperty( "nc.PersonMiddleName" )
                           .value( row -> getMiddleName( row.getAs( "X13.Name" ))).ok()
                        .addProperty( "nc.PersonSex")
                            .value( HistoricalBHR::standardSex ).ok()
                        .addProperty( "nc.PersonRace", "X14b.Race" )
                        .addProperty( "nc.PersonBirthDate" )
                            .value( row -> bdHelper.parseDate( row.getAs( "X14d.DOB" )) ).ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "appearsin" )
                    //.to("baltimore_appearsin_test")     //test run
                      .to("baltimore_city_pd_appearsin")
                    .fromEntity( "people" )
                    .toEntity( "historicalbhr" )
                    .addProperty( "general.stringid", "X2.CC" )
                    .endAssociation()
                .endAssociations()

                .done();

        Flight testflight = Flight.newFlight()
                .createEntities()
                .addEntity( "testperson" )
                .to("baltimore_city_pd_people")
                .useCurrentSync()
                    .addProperty( "nc.SubjectIdentification", "nc.SubjectIdentification" )
                    .addProperty( "nc.PersonGivenName" , "nc.PersonGivenName")
                    .addProperty( "nc.PersonMiddleName", "nc.PersonMiddleName" )
                    .addProperty( "nc.PersonSurName", "nc.PersonSurName" )
                    .addProperty( "nc.PersonBirthDate" ).value( row -> bdHelper.parseDate( row.getAs( "X14d.DOB" )) ).ok()
                    .addProperty( "nc.PersonRace", "nc.PersonRace" )
                    .addProperty( "nc.PersonSex", "nc.PersonSex" )
                    .addProperty( "person.picture", "person.picture" )
                .endEntity()
                .endEntities()
                .done();

        Flight followupflight = Flight.newFlight()
                .createEntities()
                .addEntity( "followup" )
                .to("baltimore_city_pd_followup")
                .useCurrentSync()
                    .addProperty( "bhr.dateReported")
                        .value(  row -> dtHelper.parseTime( row.getAs( "bhr.dateReported" ) ) ).ok()
                    .addProperty( "bhr.complaintNumber", "bhr.complaintNumber" )
                    .addProperty( "bhr.officerName", "bhr.officerName" )
                    .addProperty( "bhr.officerSeqID", "bhr.officerSeqID" )
                    .addProperty( "health.staff", "health.staff" )
                    .addProperty( "bhr.followupreason", "bhr.followupreason" )
                    .addProperty( "event.comments", "event.comments" )
                .endEntity()
                .addEntity( "followupperson" )
                .to("baltimore_city_pd_people")
                .useCurrentSync()
                    .addProperty( "nc.SubjectIdentification", "nc.SubjectIdentification" )
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "fappearsin" )
                    .to("baltimore_city_pd_appearsin")
                    .useCurrentSync()
                    .fromEntity( "followupperson" )
                    .toEntity( "followup" )
                    .addProperty( "general.stringid", "bhr.complaintNumber" )
                .endAssociation()
                .endAssociations()

                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload>  flights = new LinkedHashMap<>( 1 );
        flights.put( bhrflight, payload );
        flights.put( testflight, tpayload );
        flights.put( followupflight, fpayload );

        shuttle.launchPayloadFlight( flights );

    }

    public static String getPhoneNumber( Object obj ) {
        String str = Parsers.getAsString( obj );
        if ( str != null ) {
            str = str.replaceAll( "[()*\\- ]", "" );
            //str = str.substring( 0, 10 );
            return str;
        }
        return null;
    }

    public static Boolean stringtoBool ( Object obj ) {
        String boorow = Parsers.getAsString( obj );
        if ( boorow == null) {
            return null;
        }
        else if ( boorow.equalsIgnoreCase( "No" ) )  return false;
        else if ( boorow.equalsIgnoreCase( "Yes" )) return true;
        return null;
    }

    public static String getDrugs ( Object obj) {
        String drugrow = Parsers.getAsString( obj );
        if (drugrow != null) {
            if ( drugrow.equals( "--Select--" )) {
                return null;
            }
            return drugrow;
        }
        return null;
    }

    //here entries are always "last first" or "last first middle"
//    public static String getFirstName( Object obj ) {
//        String name = obj.toString();
//        if (name.length() == 0 ) return null;
//        String[] names = name.trim().split( " +" );     //trim before split to handle cases like " last first middle "
//        if ( names.length > 2 ) {   //this split is for parsing out middle names if they are there
//            if ( Arrays.asList( names ).contains( "Jr." ) || Arrays.asList( names ).contains( "Jr" ) || Arrays.asList( names ).contains( "III" )) {
//                String ar[] = names;
//                return ar[ 2 ].trim();
//            }
//            return names[ 1 ].trim();
//        }
//        return names[ 1 ].trim();
//    }
//
//    public static String getLastName( Object obj ) {
//        String name = obj.toString();
//        if ( name.length() ==0 ) return null;
////        String lastname;
////        String suffix;
//
//        String[] names = name.trim().split( " +" );
//        if ( names.length > 1 ) {
//            if ( Arrays.asList( names ).contains( "Jr." ) || Arrays.asList( names ).contains( "Jr" ) || Arrays.asList( names ).contains( "III" )) {
//                String ar[] = names;
//                return ar[ 0 ].trim() + " " + ar[ 1 ].trim();
//            } else
//                return names[ 0 ].trim();
//        }
//        return null;
//    }

    public static String getMiddleName( Object obj) {
        String name = obj.toString();
        if (name.length() == 0 ) return null;
        String[] names = name.trim().split( " +" );
        if ( names.length > 2 ) {
            if ( Arrays.asList( names ).contains( "Jr." ) || Arrays.asList( names ).contains( "Jr" ) || Arrays.asList( names ).contains( "III" )) {
                if  (names.length == 3 ) { return null; }
            return names[names.length-1].trim(); }
        }
        return null;
    }

//    public static String getPersonID( Row row) {
//        String first = ( getFirstName( row.getAs( "X13.Name" ) ));
//        String middle = ( getMiddleName( row.getAs( "X13.Name" ) ));
//        String last = ( getLastName( row.getAs( "X13.Name" ) ));
//
//        if ( middle == null && first != null) {
//            StringBuilder person =  new StringBuilder( first );
//            person.append( " " );
//            person.append(last);
//            return person.toString();
//        }
//        StringBuilder person = new StringBuilder( first ) ;
//        person.append( " " ).append( middle ).append( " " ).append( last );
//        return person.toString();
//
//    }

    public static String getemotionalStateOther( Row other) {
        String state = Parsers.getAsString( other.getAs( "X22c.Other" ) );
        String describe = Parsers.getAsString( other.getAs( "X22d.Describe" ) );

        if (StringUtils.isNotBlank( state ) && StringUtils.isNotBlank( describe )) {
            return state + " - " + describe;
        }

        return null;
    }

    public static String getdeescalationTechniquesOther( Row row ) {
                String state =  Parsers.getAsString( row.getAs ( "X29a.Other" ));
                String describe = Parsers.getAsString( row.getAs( "X29b.Describe") );

                if ( StringUtils.isNotBlank( state ) && StringUtils.isNotBlank( describe )) {
                    return state + " - " + describe;
                }
                return null;
            }

    public static String standardSex( Row row) {
        String sex = row.getAs( "X14a.Gender" );
        //if (StringUtils.isBlank( sex )) return null;

        if ( sex != null ) {
            if (sex.equals( "Male" )) {return "M"; };
            if (sex.equals( "Female" )) {return "F"; };
            if ( sex.equals( "" )) { return null; };

        }
        return null;

    }

    }


