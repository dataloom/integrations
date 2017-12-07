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
import com.openlattice.integrations.cruft.FormattedDateTime;
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

    private static final Environment environment = Environment.PRODUCTION;

    public static void main( String[] args ) throws InterruptedException {

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        System.out.println("Hello World");

        final String jwtToken = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJlbWFpbCI6ImtpbUBvcGVubGF0dGljZS5jb20iLCJlbWFpbF92ZXJpZmllZCI6dHJ1ZSwidXNlcl9pZCI6Imdvb2dsZS1vYXV0aDJ8MTA0ODQ2MjU0NjQ5MTc1ODU5NTA4IiwiYXBwX21ldGFkYXRhIjp7InJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iLCJ1c2VyIl19LCJuaWNrbmFtZSI6ImtpbSIsInJvbGVzIjpbIkF1dGhlbnRpY2F0ZWRVc2VyIiwiYWRtaW4iLCJ1c2VyIl0sImlzcyI6Imh0dHBzOi8vb3BlbmxhdHRpY2UuYXV0aDAuY29tLyIsInN1YiI6Imdvb2dsZS1vYXV0aDJ8MTA0ODQ2MjU0NjQ5MTc1ODU5NTA4IiwiYXVkIjoibzhZMlUyemI1SXdvMDFqZHhNTjFXMmFpTjhQeHdWamgiLCJpYXQiOjE1MDg1MTg3NTAsImV4cCI6MTUwODU1NDc1MH0.Od10SqlHQvIuzMgsV35lkwdu42GEl4nbP5yND4glp7g";
        final SparkSession sparkSession = MissionControl.getSparkSession();

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );

        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );


        /*
         * load edm.yaml and ensure all EDM elements exist
         */

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }

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

        String csvPath = Resources.getResource( "DemoJustice9-28.csv" ).getPath();

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
                            .addProperty("nc.PersonSurName", "ArrOfficerLastName" )  //shortcut if returning strings
                            .addProperty("nc.PersonGivenName", "ArrOfficerFirstName")
                            .endEntity()
                        .addEntity("TrOfficer")
                            .to("DemoOfficers")
                            .addProperty("person.OfficerBadgeIdentification", "TranspOfficerBadgeID")
                            .addProperty("nc.PersonSurName", "TranspOfficerLastName")
                            .addProperty("nc.PersonGivenName", "TranspOfficerFirstName")
                            .endEntity()
                        .addEntity("RelOfficer")
                            .to("DemoOfficers")
                            .addProperty("person.OfficerBadgeIdentification", "ReleaseOfficerBadgeID" )
                            .addProperty("nc.PersonGivenName", "RelOfficerFirstName")
                            .addProperty("nc.PersonSurName", "RelOfficerLastName")
                            .endEntity()
                        .addEntity("Person")
                            .to("DemoSuspects")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("nc.PersonGivenName", "FirstName")
                            .addProperty("nc.PersonSurName", "LastName")
                            .addProperty("nc.PersonSex", "Sex")
                            .addProperty("nc.PersonRace", "Race")
                            .addProperty("nc.PersonEthnicity", "Ethnicity")
                            .addProperty("nc.PersonBirthDate").value( row -> fixDate(row.getAs("BirthDate")) ).ok()
                            .addProperty("nc.PersonEyeColorText", "EyeColorText")
                            .addProperty("nc.SSN", "SocialSecurityNumber")
                            .addProperty("j.SentenceRegisterSexOffenderIndicator", "RegisteredSexOffender")
                            .endEntity()
                        .addEntity("Incident")
                            .to("DemoIncidents")
                            .addProperty("general.StringID", "IncidentID")
                            .addProperty("date.IncidentDate").value( row -> fixDate(row.getAs("IncidentDate")) ).ok()
                            .addProperty("place.StreetAddress", "IncidentStreet")
                            .addProperty("place.City", "IncidentCity")
                            .addProperty("event.DrugsPresentAtArrest", "DrugsPresent")
                            .addProperty("event.WeaponsPresentAtArrest", "WeaponPresent")
                            .endEntity()
                    .addEntity("Charge")
                            .to("DemoCharges")
                            .addProperty("general.ChargeSequenceID", "ChargeID")
                            .addProperty("event.OffenseLocalCodeSection", "OffenseLocalStatute")
                            .addProperty("event.OffenseLocalDescription", "OffenseLocalText")
                            .addProperty("event.ChargeLevel", "ChargeLevel")
                            .addProperty("event.ChargeLevelState", "ChargeLevelState")
                            .endEntity()
                    .addEntity("Arrest")
                            .to("DemoArrests")
                            .addProperty("general.ArrestSequenceID", "ArrestNumber")
                            .addProperty("event.ArrestNumber", "ArrestNumber")
                            .addProperty("date.ArrestDate").value( row -> fixDate(row.getAs("ArrestDate")) ).ok()
                            .addProperty("event.ArrestCategory", "ArrestCategory")
                            .addProperty("event.WarrantType", "WarrantType")
                            .addProperty("place.ArrestingAgency", "ArrestingAgencyName")
                            .addProperty("person.ArrestingOfficer", "ArrestingOfficerBadgeID")
                            .addProperty("person.TransportingOfficer", "TranspOfficerBadgeID")
                            .endEntity()
                    .addEntity("Booking Records")
                            .to("DemoBookingRecords")
                            .addProperty("general.StringID", "BookingID")
                            .addProperty("person.AgeAtEvent", "AgeAtBooking")
                            .addProperty("person.ReleaseOfficer", "ReleaseOfficerBadgeID")
                            .endEntity()
                    .addEntity("Case")
                            .to("DemoCases")
                            .addProperty("general.StringID", "CaseID")
                            .addProperty("event.ConvictionResults", "ConvictionResults")
                            .addProperty("event.SentenceTermYears").value( row -> parseNumber( row.getAs( "SentenceDurationYrs" ) ) ).ok()
                            .addProperty("event.SentenceTermDays").value( row -> parseNumber( row.getAs( "SentenceTermDays" ) ) ).ok()
                            .endEntity()
                        .endEntities()
                .createAssociations()
                    .addAssociation("Arrested In")
                            .to("DemoArrestedIn")
                            .fromEntity("Person")       //entity type title
                            .toEntity("Arrest")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.ArrestSequenceID", "ArrestNumber")
                            .endAssociation()
                    .addAssociation("Charged With")
                            .to("DemoChargedWith")
                            .fromEntity("Person")
                            .toEntity("Charge")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.ChargeSequenceID", "ChargeID")
                            .endAssociation()
                    .addAssociation("AppearsInIncidents")
                            .to("DemoAppearsIn")
                            .fromEntity("Person")
                            .toEntity("Incident")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.StringID", "IncidentID")
                            .endAssociation()
                    .addAssociation("AppearsInBookings")
                            .to("DemoAppearsIn")
                            .fromEntity("Person")
                            .toEntity("Booking Records")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.StringID", "BookingID")
                            .endAssociation()
                    .addAssociation("AppearsInCases")
                            .to("DemoAppearsIn")
                            .fromEntity("Person")
                            .toEntity("Case")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("general.StringID", "CaseID")
                            .endAssociation()
                    .addAssociation("Demo Arrested By")
                            .to("DemoArrestedBy")       //name of entity set belonging to, must match yaml.
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
                    .addAssociation("DemoTransportedBy")
                            .to("DemoTransportedBy")       //name of entity set belonging to
                            .fromEntity("Person")
                            .toEntity("TrOfficer")
                            .addProperty("general.StringID", "TranspOfficerBadgeID")
                            .endAssociation()
                        .endAssociations()
                        .done();




        // @formatter:on

        return flight;
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

    public static String fixDate( Object obj ) {
        if ( obj == null ) {
            return null;

        }
        String checkme = obj.toString();
        if ( checkme.equals( "0.00" ) ) {
        logger.info( "OMG ITS THAT NUMBER -----------------------------" );
        return null;
    }
        if ( obj != null ) {
        String d = obj.toString();
        FormattedDateTime date = new FormattedDateTime( d, null, "MM/dd/yyyy", "HH:mm:ss" );
        return date.getDateTime();
    }
        return null;
}

}
