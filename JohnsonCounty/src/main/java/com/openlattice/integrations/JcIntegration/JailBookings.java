package com.openlattice.integrations.JcIntegration;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class JailBookings {

    private static final Logger logger = LoggerFactory
            .getLogger(JailBookings.class);
    private static final Environment environment = Environment.LOCAL;
    private static final DateTimeHelper dtHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");
    private static final DateTimeHelper bdHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");

    public static void main(String[] args) throws InterruptedException {

        final String bookingPath = args[0];
        final String chargesPath = args[1];
        final String dispatchPath = args[2];
        final String disPersonPath = args[3];
        final String officerPath = args[4];
        final String jwtToken = args[5];
        //final String username = args[ 1 ];
        //final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info("Using the following idToken: Bearer {}", jwtToken);

        Retrofit retrofit = RetrofitFactory.newClient(environment, () -> jwtToken);
        EdmApi edm = retrofit.create(EdmApi.class);

        Dataset<Row> bookings = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(bookingPath);

        Dataset<Row> offense = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(chargesPath);

        Dataset<Row> dispatch = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(dispatchPath);

        Dataset<Row> disPerson = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(disPersonPath);

        Dataset<Row> officer = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(officerPath);

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration(RequiredEdmElements.class);
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());
        if (requiredEdmElements != null) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager(edm,
                    retrofit.create(PermissionsApi.class));
            reem.ensureEdmElementsExist(requiredEdmElements);
        }

        logger.info("ER Field names: {}", Arrays.asList(bookings.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(offense.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(dispatch.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(disPerson.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(officer.schema().fieldNames()));


        Flight bookingMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Suspects")
                .to("Suspects")
                .ofType(new FullQualifiedName("general.person"))
                .key(new FullQualifiedName("nc.SubjectIdentification"))
                .addProperty("nc.SubjectIdentification")
                .value(JailBookings::getSubjectIdentification)
                .ok()
                .addProperty("nc.PersonGivenName", "JName")
                .addProperty("nc.PersonMiddleName", "JName")
                .addProperty("nc.PersonSurName", "JName")
                .addProperty("im.PersonNickName", "Alias")
                .addProperty("nc.PersonSex", "Sex")
                .addProperty("nc.PersonRace", "Race")
                .addProperty("person.SafetyConcerns", "Caution")
                .addProperty("nc.PersonBirthDate")
                .value(JailBookings::safeDOBParse)
                .ok()
                .endEntity()
                .addEntity("Arrests")
                .to("Arrests")
                .ofType("general.Arrest")
                .key("general.ArrestSequenceID")
                .addProperty("general.ArrestSequenceID")
                .value(JailBookings::getArrestSequenceID)
                .ok()
                .addProperty("date.ArrestDate")
                .value(JailBookings::safeArrestDateParse)
                .ok()
                .addProperty("place.ArrestingAgency", "Arrest_Agency")
                .addProperty("place.TransportingAgency", "Transp_Agency")
                .addProperty("person.ArrestingOfficer", "AO")
                .addProperty("person.SearchOfficer", "Search_Officer")
                .addProperty("person.TransportingOfficer", "Transp_Officer")
                .endEntity()
                .addEntity("BookingRecords")
                .to("BookingRecords")
                .ofType("general.BookingRecords")
                .key("general.StringID")
                .addProperty("general.StringID", "Jail_ID")
                .addProperty("date.BookingDate")
                .value(JailBookings::safeBookingDateParse)
                .ok()
                .addProperty("date.ExpectedReleaseDateFromCustody")
                .value(JailBookings::safeERDParse)
                .ok()
                .addProperty("person.AgeAtEvent", "Age")
                .addProperty("nc.PersonHairColorText", "OHair")
                .addProperty("nc.PersonWeightMeasure", "OWeight")
                .addProperty("nc.PersonHeightMeasure", "OHeight")
                .addProperty("place.HoldingFacility", "Held_At")
                .addProperty("event.BookingComments", "Remarks")
                .addProperty("place.ReleaseToFacility", "Released_To")
                .addProperty("person.ReleaseOfficer", "Rel_Officer")
                .addProperty("person.ReleaseUserName", "Release_UserName")
                .addProperty("event.ReleaseComments", "ReleaseNotes")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("ArrestedIn")
                .ofType("general.ArrestedIn").to("ArrestedIn")
                .fromEntity("Suspects")
                .toEntity("Arrests")
                .key("nc.SubjectIdentification", "general.ArrestSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(JailBookings::getSubjectIdentification)
                .ok()
                .addProperty("general.ArrestSequenceID")
                .value(JailBookings::getArrestSequenceID)
                .ok()
                .endAssociation()
                .addAssociation("BookedIn")
                .ofType("justice.bookedin").to("BookedIn")
                .fromEntity("Suspects")
                .toEntity("BookingRecords")
                .key("nc.SubjectIdentification", "general.stringid")
                .addProperty("nc.SubjectIdentification")
                .value(JailBookings::getSubjectIdentification)
                .ok()
                .addProperty("general.stringid", "Jail_ID")
                .endAssociation()
                .endAssociations()
                .done();

        Flight offenseMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Charges")
                .to("Charges")
                .ofType(new FullQualifiedName("general.Charge"))
                .key(new FullQualifiedName("general.ChargeSequenceID"))
                .addProperty("general.ChargeSequenceID")
                .value(JailBookings::getChargeSequenceID).ok()
                .addProperty("event.OffenseStateCodeSection", "State")
                .addProperty("event.OffenseLocalCodeSection", "Local")
                .addProperty("event.OffenseNCICCode", "NCIC")
                .addProperty("event.OffenseLocalDescription", "Charge")
                .addProperty("event.ChargeLevel", "Severity")
                .addProperty("place.ChargingAgency", "Charging_Agency")
                .addProperty("date.ChargeReleaseDate")
                .value(JailBookings::safeReleaseDateParse).ok()
                .endEntity()
                .addEntity("Offenses")
                .to("Offenses")
                .ofType("general.Offense")
                .key("general.CrimeSequenceID")
                .addProperty("general.OffenseSequenceID", "Jail_ID")
                .addProperty("date.OffenseDate")
                .value(JailBookings::safeOffenseDateParse).ok()
                .addProperty("date.OffenseEntryDateToSystem")
                .value(JailBookings::safeEntryDateParse).ok()
                //.addProperty("date.OffenseDayOfWeek")
                //.value(JailBookings::safeDOBParse).ok()
                .endEntity()
                .addEntity("Suspects")
                .to("Suspects")
                .useCurrentSync()
                .ofType("general.person")
                .key("nc.SubjectIdentification")
                .addProperty("nc.SubjectIdentification")
                .value(JailBookings::getSubjectIdentification).ok()
                .addProperty("j.SentenceRegisterSexOffenderIndicator", "SexOff")
                .endEntity()
                .addEntity("Arrests")
                .to("Arrests")
                .useCurrentSync()
                .ofType("general.Arrest")
                .key("general.ArrestSequenceID")
                .addProperty("general.ArrestSequenceID")
                .value(JailBookings::getArrestSequenceID).ok()
                .addProperty("event.WarrantNumber", "Warr_No")
                .endEntity()
                .addEntity("BookingRecords")
                .to("BookingRecords")
                .useCurrentSync()
                .ofType("general.BookingRecords")
                .key("general.StringID")
                .addProperty("general.StringID", "Jail_ID")
                .addProperty("event.ReasonHeldCode", "ReasonHeld")
                .endEntity()
                .addEntity("Cases")
                .to("Cases")
                .ofType("general.Case")
                .key("general.StringID")
                .addProperty("general.StringID", "Court")
                .addProperty("event.CourtCaseNumber", "State")
                .addProperty("date.CourtReferralDate", "NCIC")
                .addProperty("event.NumberOfCounts", "NoCounts")
                .addProperty("event.SentenceTermDays", "SentenceDays")
                .addProperty("event.SentenceTermHours", "SentenceHrs")
                .endEntity()
                .addEntity("Sentencing")
                .to("SentencingRecords")
                .ofType("general.SentencingRecords")
                .key("general.StringID")
                .addProperty("general.StringID", "Jail_ID")
                .addProperty("date.SentenceStartDate")
                .value(JailBookings::safeSentenceStartDateParse).ok()
                .addProperty("date.SentenceReleaseDate")
                .value(JailBookings::safeReleaseDateParse).ok()
                .addProperty("event.ServeConcurrentOffense", "Concurrent")
                .addProperty("event.ServeConsecutiveOffense", "ConsecWith")
                .addProperty("event.TimeServedDays", "TSrvdDays")
                .addProperty("event.TimeServedHours", "TSrvdHrs")
                .addProperty("event.TimeServedMinutes", "TSrvdMins")
                .addProperty("event.GoodTimeDays", "GTDays")
                .addProperty("event.GoodTimeHours", "GTHrs")
                .addProperty("event.GoodTimeMinutes", "GTMins")
                .addProperty("event.GoodTimePercentage", "GTPct")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("ChargedWith")
                .ofType("general.ChargedWith").to("ChargedWith")
                .fromEntity("Suspects")
                .toEntity("Charges")
                .key("nc.SubjectIdentification", "general.ChargeSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(JailBookings::getSubjectIdentification).ok()
                .addProperty("general.ChargeSequenceID")
                .value(JailBookings::getChargeSequenceID).ok()
                .endAssociation()
                .endAssociations()
                .done();

        Flight disPersonMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Persons")
                .to("Persons")
                .ofType(new FullQualifiedName("general.person"))
                .key(new FullQualifiedName("nc.SubjectIdentification"))
                .addProperty("nc.SubjectIdentification", "ID")
                .addProperty("nc.PersonGivenName", "OName")
                .addProperty("nc.PersonMiddleName", "OName")
                .addProperty("nc.PersonSurName", "OName")
                .addProperty("nc.PersonSex", "OSex")
                .addProperty("nc.PersonRace", "ORace")
                .addProperty("nc.PersonEthnicity", "Ethnicity")
                .addProperty("nc.PersonEyeColorText", "Eyes")
                .addProperty("nc.PersonHairColorText", "Hair")
                .addProperty("nc.PersonBirthDate")
                .value(JailBookings::safeDOBParse)
                .ok()
                .endEntity()
                .addEntity("Places")
                .to("Places")
                .ofType("general.Place")
                .key("general.StringID")
                .addProperty("general.StringID", "OAddress")
                .addProperty("general.StreetAddress", "OAddress")
                .addProperty("place.City", "OCity")
                .addProperty("place.State", "OState")
                .addProperty("place.ZipCode", "OZip")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("LivedAt")
                .ofType("general.LivedAt").to("LivedAt")
                .fromEntity("Persons")
                .toEntity("Places")
                .key("nc.SubjectIdentification", "general.StringID")
                .addProperty("nc.SubjectIdentification", "ID")
                .addProperty("general.StringID", "OAddress")
                .endAssociation()
                .endAssociations()
                .done();

        Flight officerMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Officers")
                .to("Officers")
                .ofType(new FullQualifiedName("j.EnforcementOfficial"))
                .key(new FullQualifiedName("justice.xref"))
                .addProperty("justice.xref", "id")
                .addProperty("publicsafety.officerID", "officerid")
                .addProperty("person.OfficerBadgeIdentification", "mfrbadgenumber")
                .addProperty("nc.PersonGivenName", "FirstName")
                .addProperty("nc.PersonSurName", "LastName")
                .endEntity()
                .endEntities()
                .done();


        Flight dispatchMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Dispatches")
                .to("Dispatches")
                .ofType(new FullQualifiedName("general.Dispatch"))
                .key(new FullQualifiedName("general.DispatchSequenceID"))
                .addProperty("general.DispatchSequenceID", "Dis_ID")
                .addProperty("date.DispatchDateTime")
                .value(JailBookings::safeDispatchDateTimeParse).ok()
                .addProperty("date.DispatchAlertedDateTime")
                .value(JailBookings::safDispatchAlertedParse).ok()
                .addProperty("place.DispatchZone", "Dis_Zone")
                .addProperty("place.DispatchZoneID", "ZONE_ID")
                .addProperty("place.DispatchZSubZone", "SubZone")
                .addProperty("place.NoSubZoneOK", "NoSubZoneOK")
                .addProperty("person.Operator", "Operator")
                .addProperty("event.HowReported", "HowReported")
                .addProperty("event.CallNumber911", "CallNumber_911")
                .addProperty("event.ClearedBy", "ClearedBy")
                .endEntity()
                .addEntity("Places")
                .to("Places")
                .ofType("general.Place")
                .key("general.StringID")
                .addProperty("general.StringID", "LAddress")
                .addProperty("place.Location", "Location")
                .addProperty("place.City", "LCity")
                .addProperty("place.State", "LState")
                .addProperty("place.ZipCode", "LZip")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("OccurredAt")
                .ofType("justice.occurredat").to("OccurredAt")
                .fromEntity("Dispatches")
                .toEntity("Places")
                .key("general.DispatchSequenceID", "general.StringID")
                .addProperty("general.DispatchSequenceID", "Dis_ID")
                .addProperty("general.StringID", "LAddress")
                .endAssociation()
                .endAssociations()
                .done();


        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>(2);
        flights.put(bookingMapping, bookings);
        flights.put(offenseMapping, offense);
        flights.put(dispatchMapping, dispatch);
        flights.put(disPersonMapping, disPerson);
        flights.put(officerMapping, officer);


        shuttle.launch(flights);
    }

    public static String safeDOBParse(Row row) {
        String dob = row.getAs("DOB");
        return bdHelper.parse(dob);
    }

    public static String safeArrestDateParse(Row row) {
        String arrestDate = row.getAs("Arrest_Date");
        return dtHelper.parse(arrestDate);
    }

    public static String safeBookingDateParse(Row row) {
        String bookingDate = row.getAs("Date_In");
        return dtHelper.parse(bookingDate);
    }

    public static String safeOffenseDateParse(Row row) {
        String OffDate = row.getAs("Off_Date");
        return dtHelper.parse(OffDate);
    }

    public static String safeERDParse(Row row) {
        String expReleaseDate = row.getAs("Est_Rel_Date");
        return dtHelper.parse(expReleaseDate);
    }

    public static String safeReleaseDateParse(Row row) {
        String releaseDate = row.getAs("Release_Date");
        return dtHelper.parse(releaseDate);
    }

    public static String safeEntryDateParse(Row row) {
        String entryDate = row.getAs("EntryDate");
        return dtHelper.parse(entryDate);
    }

    public static String safeSentenceStartDateParse(Row row) {
        String startDate = row.getAs("Start_Date");
        return dtHelper.parse(startDate);
    }

    public static String safDispatchAlertedParse(Row row) {
        String AlertedTime = row.getAs("AlertedTime");
        return dtHelper.parse(AlertedTime);
    }

    public static String safeDispatchDateTimeParse(Row row) {
        String DateTimeJanet = row.getAs("CFS_DateTimeJanet");
        return dtHelper.parse(DateTimeJanet);
    }

    //public static String safeSentenceStartDateParse(Row row) {
    //    String startDate = row.getAs("Start_Date");
    //    return dtHelper.parse(startDate);
    //}

    public static String getSubjectIdentification(Row row) {

        return "SUSPECT-" + row.getAs("MNI_No");
    }

    public static String getArrestSequenceID(Row row) {

        return "ARREST-" + row.getAs("Jail_ID").toString().trim();
    }

    public static String getBookingSequenceID(Row row) {

        return "BOOKING-" + row.getAs("Jail_ID").toString().trim();
    }

    public static String getChargeSequenceID(Row row) {

        return "CHARGE-" + row.getAs("Jail_ID").toString().trim();
    }

    public static String getIncidentSequenceID(Row row) {

        return "INCIDENT-" + row.getAs("Jail_ID").toString().trim();
    }


}
