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
 */

package com.openlattice.integrations.demodatajustice;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.edm.EdmApi;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

public class DemoData {

    private static final Logger logger = LoggerFactory.getLogger( DemoData.class );

    private static final Environment environment = Environment.PRODUCTION;

    private static final DateTimeHelper dtHelper = new DateTimeHelper( TimeZones.America_NewYork, "MM/dd/YY", "MM/dd/yy" );

    public static void main( String[] args ) throws InterruptedException {

        //        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        //        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        System.out.println( "Hello World" );

        final String demoPath = args[ 0 ];
        final String jwtToken = args[ 1 ];

        SimplePayload payload = new SimplePayload( demoPath );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );
        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edmApi = retrofit.create( EdmApi.class );
        //        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );


        /*
         * load edm.yaml and ensure all EDM elements exist
         */
        //
        //        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
        //                .loadConfiguration( RequiredEdmElements.class );
        //
        //        if ( requiredEdmElements != null ) {
        //            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
        //            manager.ensureEdmElementsExist( requiredEdmElements );
        //        }

        Flight demoflight = Flight.newFlight()
                    .createEntities()

                        .addEntity("ArrOfficer" )
                            //.ofType("general.Officer")
                            .to("DemoOfficers_v2")
                            .addProperty("publicsafety.officerid", "ArrestingOfficerBadgeID" )
//                            .addProperty("person.SurName", "ArrOfficerLastName" )  //shortcut if returning strings
//                            .addProperty("person.GivenName", "ArrOfficerFirstName")
                            .endEntity()
                        .addEntity("TrOfficer")
                            .to("DemoOfficers_v2")
                            .addProperty("publicsafety.officerid", "TranspOfficerBadgeID")
//                            .addProperty("person.SurName", "TranspOfficerLastName")
//                            .addProperty("person.GivenName", "TranspOfficerFirstName")
                            .endEntity()
                        .addEntity("RelOfficer")
                            .to("DemoOfficers_v2")
                            .addProperty("publicsafety.officerid", "ReleaseOfficerBadgeID" )
//                            .addProperty("person.GivenName", "RelOfficerFirstName")
//                            .addProperty("person.SurName", "RelOfficerLastName")
                            .endEntity()
                        .addEntity("Person")
                            .to("DemoSuspects")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("nc.PersonGivenName", "FirstName")
                            .addProperty("nc.PersonSurName", "LastName")
                            .addProperty("nc.PersonSex", "Sex")
                            .addProperty("nc.PersonRace", "Race")
                            .addProperty("nc.PersonEthnicity", "Ethnicity")
                            .addProperty("nc.PersonBirthDate")
                                .value( row -> dtHelper.parseDate(row.getAs("BirthDate")) ).ok()
                            .addProperty("nc.PersonEyeColorText", "EyeColorText")
                            .addProperty("nc.SSN", "SocialSecurityNumber")
                            .addProperty("j.SentenceRegisterSexOffenderIndicator", "RegisteredSexOffender")
                            .endEntity()
                        .addEntity("Incident")
                            .to("DemoIncidents_v2")                     //DEL OLD, CREATE NEW
                            .addProperty("criminaljustice.incidentid", "IncidentID")
                            .addProperty("incident.reporteddatetime")
                                .value( row -> dtHelper.parseDate(row.getAs("IncidentDate")) ).ok()
                            .addProperty("location.address")
                                .value( row -> row.getAs( "IncidentStreet" ) + ", " + row.getAs( "IncidentCity" )).ok()
                            .addProperty("publicsafety.drugspresent", "DrugsPresent")
                            .addProperty("publicsafety.weapons", "WeaponPresent")
                            .endEntity()
                    .addEntity("Offense")
                            .to("DemoOffenses")
                            .addProperty("criminaljustice.offenseid", "ChargeID")
                            .addProperty("criminaljustice.localstatute", "OffenseLocalStatute")
                            .addProperty("event.comments", "OffenseLocalText")
                            .endEntity()
//                    .addEntity("Booking Records")
//                            .to("DemoBookingRecords")
//                            .addProperty("j.CaseNumberText", "BookingID")
//                            .addProperty("person.ageatevent", "AgeAtBooking")
//                            .addProperty("person.releaseofficer", "ReleaseOfficerBadgeID")
//                            .endEntity()
                    .addEntity("Sentence")
                            .to("DemoSentences")
                            .entityIdGenerator( row -> row.get("SentenceID") )
                            .addProperty("ol.convictionresults", "ConvictionResults")
                            .addProperty("event.SentenceTermYears").value( row -> parseNumber( row.getAs( "SentenceDurationYrs" ) ) ).ok()
                            .addProperty("publicsafety.SentenceTermDays").value( row -> parseNumber( row.getAs( "SentenceTermDays" ) ) ).ok()
                            .addProperty( "date.projectedrelease" )
                                .value( row -> dtHelper.parse( "ReleaseDate" ) ).ok()
                            .addProperty("person.ageatevent", "AgeAtBooking")
                            .addProperty("person.releaseofficer", "ReleaseOfficerBadgeID")
                    .endEntity()
                .endEntities()

                .createAssociations()
                    .addAssociation("Arrested In")
                            .to("DemoArrestedIn_v2")
                            .fromEntity("Person")
                            .toEntity("Incident")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("arrestedin.id", "ArrestNumber")
                            .addProperty("arrest.date")
                                .value( row -> dtHelper.parse( "ArrestDate" ) ).ok()
                            .addProperty("arrest.category", "ArrestCategory")
                            .addProperty("criminaljustice.warranttype", "WarrantType")
                            .addProperty("criminaljustice.arrestagency", "ArrestingAgencyName")
                    .endAssociation()
                    .addAssociation("Charged With")
                            .to("DemoChargedWith")                                     //DEL OLD, CREATE NEW
                                .fromEntity("Person")
                                .toEntity("Offense")
                                .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                                .addProperty("general.stringid", "ChargeID")
                                .addProperty("ol.chargelevel", "ChargeLevel")
                                .addProperty("ol.chargelevelstate", "ChargeLevelState")
                            .endAssociation()
                //DELETE?
//                    .addAssociation("AppearsInIncidents")
//                            .to("DemoAppearsIn")
//                            .fromEntity("Person")
//                            .toEntity("Incident")
//                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
//                            .addProperty("general.stringid", "IncidentID")
//                            .endAssociation()
//                    .addAssociation("AppearsInBookings")
//                            .to("DemoAppearsIn")
//                            .fromEntity("Person")
//                            .toEntity("Booking Records")
//                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
//                            .addProperty("general.stringid", "BookingID")
//                    .endAssociation()
                    .addAssociation("DemoLeadsTo")
                            .to("DemoLeadsToSentence")
                            .fromEntity("Offense")
                            .toEntity("Sentence")
                            .addProperty("general.stringid", "SentenceID")
                            .addProperty("ol.convictionresults", "ConvictionResults")
                    .endAssociation()
                    .addAssociation("Demo Arrested By")
                            .to("DemoArrestedBy")       //name of entity set belonging to
                            .fromEntity("Person")
                            .toEntity("ArrOfficer")
                            .addProperty("general.StringID", "ArrestingOfficerBadgeID")
                    .endAssociation()
                    .addAssociation("Demo Released By")
                            .to("DemoReleasedBy")       //name of entity set belonging to
                            .fromEntity("Person")
                            .toEntity("RelOfficer")
                            .addProperty("general.StringID", "ReleaseOfficerBadgeID")
                            .endAssociation()
                    .addAssociation("Demo Transported By")
                            .to("DemoTransportedBy")       //name of entity set belonging to
                            .fromEntity("Person")
                            .toEntity("TrOfficer")
                            .addProperty("general.StringID", "TranspOfficerBadgeID")
                            .endAssociation()
                        .endAssociations()
                        .done();


        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( demoflight, payload );

        shuttle.launchPayloadFlight( flights );
    }

    public static Integer parseNumber( String num ) {
        if (num == null) return null;
        try {
            Double d = Double.parseDouble( num );
            return d.intValue();
        } catch (NumberFormatException e) {}

        try {
            Integer i = Integer.parseInt( num );
            return i;
        } catch ( NumberFormatException e) {}

        return null;
    }

}



