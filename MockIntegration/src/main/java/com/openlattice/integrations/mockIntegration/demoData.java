package com.openlattice.integrations.mockIntegration;

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

public class demoData {

    private static final Logger logger = LoggerFactory
            .getLogger(demoData.class);
    private static final Environment environment = Environment.LOCAL;
    private static final DateTimeHelper dtHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");
    private static final DateTimeHelper bdHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");

    public static void main(String[] args) throws InterruptedException {

        final String arrestPath = args[0];
        final String healthPath = args[1];
        final String jwtToken = args[2];
        //final String username = args[ 1 ];
        //final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //final String jwtToken = MissionControl.getIdToken( username, password );

        logger.info("Using the following idToken: Bearer {}", jwtToken);

        Retrofit retrofit = RetrofitFactory.newClient(environment, () -> jwtToken);
        EdmApi edm = retrofit.create(EdmApi.class);

        Dataset<Row> arrest = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(arrestPath);

        Dataset<Row> health = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(healthPath);

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration(RequiredEdmElements.class);
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());
        if (requiredEdmElements != null) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager(edm,
                    retrofit.create(PermissionsApi.class));
            reem.ensureEdmElementsExist(requiredEdmElements);
        }

        logger.info("ER Field names: {}", Arrays.asList(arrest.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(health.schema().fieldNames()));

        Flight arrestMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Suspects")
                .to("Suspects")
                .ofType(new FullQualifiedName("general.person"))
                .key(new FullQualifiedName("nc.SubjectIdentification"))
                .addProperty("nc.SubjectIdentification")
                .value(demoData::getSubjectIdentification).ok()
                .addProperty("nc.PersonGivenName", "FirstName")
                .addProperty("nc.PersonSurName", "LastName")
                .addProperty("nc.PersonSex", "Sex")
                .addProperty("nc.PersonRace", "Race")
                .addProperty("nc.PersonEthnicity", "Ethnicity")
                .addProperty("nc.PersonBirthDate", "BirthDate")
                .endEntity()
                .addEntity("Incidents")
                .to("Incidents")
                .ofType("general.Incident")
                .key("general.IncidentSequenceID")
                .addProperty("general.IncidentSequenceID")
                .value(demoData::getIncidentSequenceID).ok()
                .addProperty("date.OffenseDate")
                .value(demoData::safeOffenseDateParse).ok()
                .addProperty("event.DrugPresentAtArrest", "DrugsPresent")
                .addProperty("event.WeaponsPresentAtArrest", "WeaponPresent")
                .addProperty("place.StreetAddress", "IncidentStreet")
                .addProperty("place.City", "IncidentCity")
                //.addProperty("event.Disposition", "DIS_Submit")
                .endEntity()
                .addEntity("Arrests")
                .to("Arrests")
                .ofType("general.Arrest")
                .key("general.ArrestSequenceID")
                .addProperty("general.ArrestSequenceID")
                .value(demoData::getArrestSequenceID).ok()
                .addProperty("date.ArrestDate")
                .value(demoData::safeArrestDateParse).ok()
                .addProperty("person.EyeColorText", "EyeColorText")
                .addProperty("person.RegisteredSexOffender", "RegisteredSexOffender")
                .addProperty("event.WarrantType", "WarrantType")
                .addProperty("event.ArrestCategory", "ArrestCategory")
                .addProperty("place.ArrestingAgency", "ArrestingAgencyName")
                .addProperty("place.TransportingAgency", "ArrestingAgencyName")
                .addProperty("person.ArrestingOfficer", "AO")
                .addProperty("person.SearchOfficer", "RelOfficer")
                .addProperty("person.TransportingOfficer", "TranspOfficer")
                .endEntity()
                .addEntity("Charges")
                .to("Charges")
                .ofType("general.Charge")
                .key("general.ChargeSequenceID")
                .addProperty("general.ChargeSequenceID")
                .value(demoData::getChargeSequenceID).ok()
                .addProperty("event.OffenseStateCodeSection", "ChargeLevelState")
                .addProperty("event.OffenseLocalCodeSection", "OffenseLocalStatute")
                .addProperty("event.OffenseDescription", "OffenseLocalText")
                .addProperty("event.OffenseSeverity", "ChargeLevel")
                .addProperty("date.ChargeReleaseDate")
                .value(demoData::safeReleaseDateParse).ok()
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("ArrestedIn")
                .ofType("general.ArrestedIn").to("ArrestedIn")
                .fromEntity("Suspects")
                .toEntity("Arrests")
                .key("nc.SubjectIdentification", "general.ArrestSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(demoData::getSubjectIdentification).ok()
                .addProperty("general.ArrestSequenceID")
                .value(demoData::getArrestSequenceID).ok()
                .endAssociation()
                .addAssociation("AppearsIn")
                .ofType("general.AppearsIn").to("AppearsIn")
                .fromEntity("Suspects")
                .toEntity("Incidents")
                .key("nc.SubjectIdentification", "general.IncidentSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(demoData::getSubjectIdentification)
                .ok()
                .addProperty("general.IncidentSequenceID")
                .value(demoData::getIncidentSequenceID).ok()
                .endAssociation()
                .addAssociation("ChargedWith")
                .ofType("general.ChargedWith").to("ChargedWith")
                .fromEntity("Suspects")
                .toEntity("Charges")
                .key("nc.SubjectIdentification", "general.ChargeSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(demoData::getSubjectIdentification).ok()
                .addProperty("general.ChargeSequenceID")
                .value(demoData::getChargeSequenceID).ok()
                .endAssociation()
                .endAssociations()
                .done();

    Flight healthMapping = Flight.newFlight()
            .createEntities()
            .addEntity("Patients")
            .to("Patients")
            .ofType(new FullQualifiedName("general.person"))
            .key(new FullQualifiedName("nc.SubjectIdentification"))
            .addProperty("nc.SubjectIdentification")
            .value(demoData::getSubjectIdentification).ok()
            .addProperty("nc.PersonGivenName", "FirstName")
            .addProperty("nc.PersonSurName", "LastName")
            .addProperty("nc.PersonSex", "Sex")
            .addProperty("nc.PersonRace", "Race")
            .addProperty("nc.PersonEthnicity", "Ethnicity")
            .addProperty("nc.PersonBirthDate", "BirthDate")
            .endEntity()
            .addEntity("Providers")
            .to("Providers")
            .ofType("general.person")
            .key("nc.SubjectIdentification")
            .addProperty("nc.SubjectIdentification")
            .value(demoData::getSubjectIdentification).ok()
            .addProperty("nc.PersonGivenName", "providerFNames")
            .addProperty("nc.PersonSurName", "providerLNames")
            .endEntity()
            .addEntity("Appointments")
            .to("Appointments")
            .ofType("general.Appointment")
            .key("general.StringID")
            .addProperty("general.StringID", "id")
            .addProperty("date.UpcomingVisit", "upcomingAppt")
            .addProperty("date.MissedAppointment", "missedAppt")
            .addProperty("date.MissedAppointment", "missedApptFirst")
            .addProperty("event.MissedAppointmentCount", "NumMissedAppt")
            .addProperty("event.AppointmentType", "ApptTypes")
            .endEntity()
            .addEntity("AdmissionRecords")
            .to("AdmissionRecords")
            .ofType("general.AdmissionRecord")
            .key("general.StringID")
            .addProperty("general.StringID", "id")
            .addProperty("date.AdmissionDate")
            .value(demoData::safeAdmissionDateParse).ok()
            .addProperty("date.LastUsed")
            .value(demoData::safeLastUsedParse).ok()
            .addProperty("event.SubstanceType", "DrugType")
            .addProperty("event.RouteOfAdministration", "DrugRoute")
            .addProperty("event.Frequency", "DrugFreq")
            .addProperty("event.Seizure", "Seizure")
            .addProperty("event.ChestPain", "ChestPain")
            .addProperty("event.AbdominalPain", "AbdominalPain")
            .addProperty("person.ProviderName", "providerNames")
            .addProperty("person.NationalProviderNumber", "NPI")
            .addProperty("event.CurrentMedicationName", "currentMedication")
            .addProperty("person.AgeAtEvent", "Age")
            .endEntity()
            .addEntity("MedicalAssessments")
            .to("MedicalAssessments")
            .ofType("general.MedicalAssessment")
            .key("general.AssessmentSequenceID")
            .addProperty("general.AssessmentSequenceID")
            .value(demoData::getAssessmentSequenceID).ok()
            .addProperty("date.MedicalAssessmentDate")
            .value(demoData::safeAdmissionDateParse).ok()
            .addProperty("event.VisitReason", "VisitReason")
            .addProperty("event.Symptoms", "Symptoms")
            .addProperty("event.DrugPresentAtVisit", "DrugPresent")
            .addProperty("event.Temperature", "Temperature")
            .addProperty("event.BloodPressure", "BloodPressure")
            .addProperty("person.ProviderName", "providerNames")
            .endEntity()
            .endEntities()
            .createAssociations()
            .addAssociation("AppearsIn")
            .ofType("general.AppearsIn").to("AppearsIn")
            .fromEntity("Patients")
            .toEntity("Appointments")
            .key("nc.SubjectIdentification", "general.StringID")
            .addProperty("nc.SubjectIdentification")
            .value(demoData::getSubjectIdentification)
            .ok()
            .addProperty("general.StringID", "id")
            .endAssociation()
            .addAssociation("VisitedIn")
            .ofType("general.VisitedIn").to("VisitedIn")
            .fromEntity("Patients")
            .toEntity("MedicalAssessments")
            .key("nc.SubjectIdentification", "general.AssessmentSequenceID")
            .addProperty("nc.SubjectIdentification")
            .value(demoData::getSubjectIdentification)
            .ok()
            .addProperty("general.AssessmentSequenceID")
            .value(demoData::getAssessmentSequenceID).ok()
            .endAssociation()
            .endAssociations()
            .done();

        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>(2);
        flights.put(arrestMapping, arrest);
        flights.put(healthMapping, health);

        shuttle.launch(flights);
    }

    public static String getSubjectIdentification(Row row) {

        return "PERSON-" + row.getAs("SubjectIdentification");
    }

    public static String getArrestSequenceID(Row row) {

        return "ARREST-" + row.getAs("X");
    }

    public static String getIncidentSequenceID(Row row) {

        return "INCIDENT-" + row.getAs("X");
    }

    public static String getChargeSequenceID(Row row) {

        return "CHARGE-" + row.getAs("X");
    }

    public static String safeDOBParse(Row row) {
        String dob = row.getAs("BirthDate");
        return bdHelper.parse(dob);
    }

    public static String safeArrestDateParse(Row row) {
        String arrestDate = row.getAs("ArrestDate");
        return dtHelper.parse(arrestDate);
    }


    public static String safeOffenseDateParse(Row row) {
        String OffDate = row.getAs("IncidentDate");
        return dtHelper.parse(OffDate);
    }

    public static String safeERDParse(Row row) {
        String expReleaseDate = row.getAs("Exp_Release_Date");
        return dtHelper.parse(expReleaseDate);
    }

    public static String safeReleaseDateParse(Row row) {
        String releaseDate = row.getAs("ReleaseDate");
        return dtHelper.parse(releaseDate);
    }

    public static String safeLastUsedParse(Row row) {
        String LastUsed = row.getAs("LastUsed");
        return dtHelper.parse(LastUsed);
    }

    public static String safeAdmissionDateParse(Row row) {
        String visitDate = row.getAs("visitDate");
        return dtHelper.parse(visitDate);
    }

    public static String getAssessmentSequenceID(Row row) {

        return "ASSESSMENT-" + row.getAs("id");
    }

}
