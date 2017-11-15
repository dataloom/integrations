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

package com.openlattice.integrations.demodata;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.io.Resources;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.config.JdbcIntegrationConfig;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

public class DemoData {

    private static final Logger logger = LoggerFactory.getLogger( DemoData.class );

    private static final Environment environment = Environment.LOCAL;

    public static void main( String[] args ) throws InterruptedException {

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        System.out.println("Hello World");

        final String jwtToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImtpbUBvcGVubGF0dGljZS5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwiYXBwX21ldGFkYXRhIjp7InJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iLCJ1c2VyIl19LCJuaWNrbmFtZSI6ImtpbSIsInJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iLCJ1c2VyIl0sInVzZXJfaWQiOiJnb29nbGUtb2F1dGgyfDEwNDg0NjI1NDY0OTE3NTg1OTUwOCIsImlzcyI6Imh0dHBzOi8vb3BlbmxhdHRpY2UuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA0ODQ2MjU0NjQ5MTc1ODU5NTA4IiwiYXVkIjoibzhZMlUyemI1SXdvMDFqZHhNTjFXMmFpTjhQeHdWamgiLCJpYXQiOjE1MTA2ODYyMDAsImV4cCI6MTUxMDcyMjIwMH0.J2C0iL_IxWpgEWdmJ5yjiI2VakwRbLQA0gqEw1PMrF0";
        final SparkSession sparkSession = MissionControl.getSparkSession();

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );

        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );


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

        /*
         * all EDM elements should now exist, and we should be safe to proceed with the integration
         */

        Dataset<Row> payload = getPayloadFromCsv( sparkSession );
        Flight flight = getFlight();

        /*
         * after creating the flight-to-payload mapping, we are ready to go! we just need an instance of Shuttle,
         * and we can launch the data integration ("flight")
         */

        Map<Flight, Dataset<Row>> flights = new HashMap<>( 1 );
        flights.put( flight, payload );

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        shuttle.launch( flights );
    }

    private static Dataset<Row> getPayloadFromCsv(final SparkSession sparkSession ) {

        String csvPath = Resources.getResource( "megatable9-26.csv" ).getPath();

        Dataset<Row> payload = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( csvPath );

        return payload;
    }

    private static Flight getFlight() {

        // @formatter:off
        Flight flight = Flight
                .newFlight()
                    .createEntities()
                        .addEntity("ArrOfficer" )   //variable name within flight. Doesn't have to match anything in edm.yaml
                            //.key("general.SubjectIdentification")
                            //.ofType("general.Officer")    //type of entity set belonging to
                            .to("DemoOfficers")         //name of entity set belonging to
                            .addProperty("person.OfficerBadgeIdentification", "ArrestingOfficerBadgeID" )
                            .addProperty("person.SurName", "ArrOfficerLastName" )  //shortcut if returning strings
                            .addProperty("person.GivenName", "ArrOfficerFirstName")
                            .endEntity()
                        .addEntity("TrOfficer")
                            .to("DemoOfficers")
                            .addProperty("person.OfficerBadgeIdentification", "TranspOfficerBadgeID")
                            .addProperty("person.SurName", "TranspOfficerLastName")
                            .addProperty("person.GivenName", "TranspOfficerFirstName")
                            .endEntity()
                        .addEntity("RelOfficer")
                            .to("DemoOfficers")
                            .addProperty("person.OfficerBadgeIdentification", "ReleaseOfficerBadgeID" )
                            .addProperty("person.GivenName", "RelOfficerFirstName")
                            .addProperty("person.SurName", "RelOfficerLastName")
                            .endEntity()
                        .addEntity("Person")
                            .to("DemoSuspects")
                            .addProperty("general.SubjectIdentification", "SubjectIdentification")
                            .addProperty("person.GivenName", "FirstName")
                            .addProperty("person.SurName", "LastName")
                            .addProperty("person.Sex", "Sex")
                            .addProperty("person.Race", "Race")
                            .addProperty("person.Ethnicity", "Ethnicity")
                            .addProperty("date.BirthDate", "BirthDate")
                            .addProperty("person.EyeColorText", "EyeColorText")
                            .addProperty("person.SocialSecurityNumber", "SocialSecurityNumber")
                            .addProperty("person.RegisteredSexOffenderIndicator", "RegisteredSexOffender")
                            .endEntity()
                        .addEntity("Incident")
                            .to("DemoIncidents")
                            .addProperty("general.StringID", "IncidentID")
                            .addProperty("date.IncidentDate", "IncidentDate")
                            .addProperty("place.StreetAddress", "IncidentStreet")
                            .addProperty("place.City", "IncidentCity")
                            .addProperty("event.DrugsPresentAtArrest", "DrugsPresent")
                            .addProperty("event.WeaponsPresentAtArrest", "WeaponPresent")
                            .endEntity()
                    .addEntity("Charge")
                            .to("DemoCharges")
                            .addProperty("justice.ArrestTrackingNumber", "ChargeID")
                            .addProperty("event.OffenseLocalCodeSection", "OffenseLocalStatute")
                            .addProperty("event.OffenseLocalDescription", "OffenseLocalText")
                            .addProperty("event.ChargeLevel", "ChargeLevel")
                            .addProperty("event.ChargeLevelState", "ChargeLevelState")
                            .endEntity()
                    .addEntity("Arrest")
                            .to("DemoArrests")
                            .addProperty("general.ArrestSequenceID", "ArrestNumber")
                            .addProperty("event.ArrestNumber", "ArrestNumber")
                            .addProperty("date.ArrestDate", "ArrestDate")
                            .addProperty("event.ArrestCategory", "ArrestCategory")
                            .addProperty("event.WarrantType", "WarrantType")
                            .addProperty("place.ArrestingAgency", "ArrestingAgencyName")
                            .addProperty("person.ArrestingOfficer", "ArrestingOfficerBadgeID")
                            .addProperty("person.TransportingOfficer", "TranspOfficerBadgeID")
                            .endEntity()
                    .addEntity("Booking Records")
                            .to("DemoBookingRecords")
                            .addProperty("j.CaseNumberText", "BookingID")
                            .addProperty("person.ageatevent", "AgeAtBooking")
                            .addProperty("person.releaseofficer", "ReleaseOfficerBadgeID")
                            .endEntity()
                    .addEntity("Sentence")
                            .to("DemoSentences")
                            //.addProperty("j.CaseNumberText", "CaseID")
                            .addProperty("event.ConvictionResults", "ConvictionResults")
                            .addProperty("event.SentenceTermYears").value( row -> parseNumber( row.getAs( "SentenceDurationYrs" ) ) ).ok()
                            .addProperty("publicsafety.SentenceTermDays").value( row -> parseNumber( row.getAs( "SentenceTermDays" ) ) ).ok()
                            .endEntity()
                        .endEntities()
                .createAssociations()
                    .addAssociation("Arrested In")
                            .to("DemoArrestedIn")
                            .fromEntity("Person")       //entity type title
                            .toEntity("Arrest")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("j.ArrestSequenceID", "ArrestNumber")
                            .endAssociation()
                    .addAssociation("Charged With")
                            .to("DemoChargedWith")
                            .fromEntity("Person")
                            .toEntity("Charge")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.stringid", "ChargeID")
                            .endAssociation()
                    .addAssociation("AppearsInIncidents")
                            .to("DemoAppearsIn")
                            .fromEntity("Person")
                            .toEntity("Incident")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.stringid", "IncidentID")
                            .endAssociation()
                    .addAssociation("AppearsInBookings")
                            .to("DemoAppearsIn")
                            .fromEntity("Person")
                            .toEntity("Booking Records")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.stringid", "BookingID")
                            .endAssociation()
                    .addAssociation("DemoLeadsTo")
                            .to("DemoLeadsToSentence")
                            .fromEntity("Charge")
                            .toEntity("Sentence")
                            .addProperty("general.stringid", "SentenceID")
                            .endAssociation()
                    .addAssociation("Demo Arrested By")
                            .to("Demo Arrested By")       //name of entity set belonging to
                            .fromEntity("Person")
                            .toEntity("ArrOfficer")
                            .addProperty("general.StringID", "ArrestingOfficerBadgeID")
                            .endAssociation()
//                    .addAssociation("Demo Released By")
//                            .to("Demo Released By")       //name of entity set belonging to
//                            .fromEntity("Person")
//                            .toEntity("RelOfficer")
//                            .addProperty("general.StringID", "ReleaseOfficerBadgeID")
//                            .endAssociation()
//                    .addAssociation("Demo Transported By")
//                            .to("Demo Transported By")       //name of entity set belonging to
//                            .fromEntity("Person")
//                            .toEntity("TrOfficer")
//                            .addProperty("general.StringID", "TranspOfficerBadgeID")
//                            .endAssociation()
                        .endAssociations()
                        .done();




        // @formatter:on

        return flight;
    }
}