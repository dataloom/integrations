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
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class DemoHealth {

    private static final Logger logger = LoggerFactory
            .getLogger(DemoHealth.class);
    private static final Environment environment = Environment.LOCAL;

    private static final DateTimeHelper dateHelper0 = new DateTimeHelper( DateTimeZone.forOffsetHours( -8 ), "yyyy-MM-dd HH:mm:ss.S" );

    public static void main(String[] args) throws InterruptedException {

        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());

        final String healthPath = args[0];
        final String jwtToken = args[1];

        final SparkSession sparkSession = MissionControl.getSparkSession();

        logger.info("Using the following idToken: Bearer {}", jwtToken);

        Retrofit retrofit = RetrofitFactory.newClient(environment, () -> jwtToken);
        EdmApi edm = retrofit.create(EdmApi.class);

        Dataset<Row> health = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(healthPath);

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration(RequiredEdmElements.class);

        if (requiredEdmElements != null) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager(edm,
                    retrofit.create(PermissionsApi.class));
            reem.ensureEdmElementsExist(requiredEdmElements);
        }

        logger.info("ER Field names: {}", Arrays.asList(health.schema().fieldNames()));

        Flight healthMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Patients")
                .to("DemoPatients")
                .ofType(new FullQualifiedName("general.person"))
                .key(new FullQualifiedName("nc.SubjectIdentification"))
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification).ok()
                .addProperty("nc.PersonGivenName", "FirstName")
                .addProperty("nc.PersonSurName", "LastName")
                .addProperty("nc.PersonSex", "Sex")
                .addProperty("nc.PersonRace", "Race")
                .addProperty("nc.PersonEthnicity", "Ethnicity")
                .addProperty("nc.PersonBirthDate")
                .value(DemoHealth::safeDOBParse).ok()
                .addProperty("nc.PersonEyeColorText", "EyeColorText")
                .endEntity()
                .addEntity("Providers")
                .to("DemoProviders")
                .ofType("general.person")
                .key("nc.SubjectIdentification")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification).ok()
                .addProperty("nc.PersonGivenName", "providerFNames")
                .addProperty("nc.PersonSurName", "providerLNames")
                .endEntity()
                .addEntity("Appointments")
                .to("DemoAppointments")
                .ofType("general.Appointment")
                .key("general.AppointmentSequenceID")
                .addProperty("general.AppointmentSequenceID")
                .value(DemoHealth::getAppointmentSequenceID).ok()
                .addProperty("date.UpcomingVisit")
                .value(DemoHealth::safeupcomingApptParse).ok()
                .addProperty("date.MissedAppointment")
                .value(DemoHealth::safemissedApptParse).ok()
                .addProperty("event.MissedAppointmentCount", "NumMissedAppt")
                .addProperty("event.AppointmentType", "ApptTypes")
                .endEntity()
                .addEntity("AdmissionRecords")
                .to("DemoAdmissionRecords")
                .ofType("general.AdmissionRecord")
                .key("general.AdmissionSequenceID")
                .addProperty("general.AdmissionSequenceID")
                .value(DemoHealth::getAdmissionSequenceID).ok()
                .addProperty("date.AdmissionDate")
                .value(DemoHealth::safeAdmissionDateParse).ok()
                .addProperty("date.LastUsed")
                .value(DemoHealth::safeLastUsedParse).ok()
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
                .to("DemoMedicalAssessments")
                .ofType("general.MedicalAssessment")
                .key("general.AssessmentSequenceID")
                .addProperty("general.AssessmentSequenceID")
                .value(DemoHealth::getAssessmentSequenceID).ok()
                .addProperty("date.MedicalAssessmentDate")
                .value(DemoHealth::safeAdmissionDateParse).ok()
                .addProperty("event.VisitReason", "VisitReason")
                .addProperty("event.Symptoms", "Symptoms")
                .addProperty("event.DrugPresentAtVisit", "DrugPresent")
                .addProperty("event.Temperature", "Temperature")
                .addProperty("event.BloodPressure", "BloodPressure")
                .addProperty("person.ProviderName", "providerNames")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("AppearsInAppointment")
                .ofType("general.AppearsIn").to("HealthAppearsIn")
                .fromEntity("Patients")
                .toEntity("Appointments")
                .key("nc.SubjectIdentification", "general.AppointmentSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification)
                .ok()
                .addProperty("general.AppointmentSequenceID")
                .value(DemoHealth::getAppointmentSequenceID)
                .ok()
                .endAssociation()
                .addAssociation("AppearsInAdmission")
                .ofType("general.AppearsIn").to("HealthAppearsIn")
                .fromEntity("Patients")
                .toEntity("AdmissionRecords")
                .key("nc.SubjectIdentification", "general.AdmissionSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification)
                .ok()
                .addProperty("general.AdmissionSequenceID")
                .value(DemoHealth::getAdmissionSequenceID)
                .ok()
                .endAssociation()
                .addAssociation("VisitedIn")
                .ofType("general.VisitedIn").to("VisitedIn")
                .fromEntity("Patients")
                .toEntity("MedicalAssessments")
                .key("nc.SubjectIdentification", "general.AssessmentSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification)
                .ok()
                .addProperty("general.AssessmentSequenceID")
                .value(DemoHealth::getAssessmentSequenceID).ok()
                .endAssociation()
                .addAssociation("AppearsInAssessment")
                .ofType("general.AppearsIn").to("HealthAppearsIn")
                .fromEntity("Providers")
                .toEntity("AdmissionRecords")
                .key("nc.SubjectIdentification", "general.AdmissionSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification)
                .ok()
                .addProperty("general.AdmissionSequenceID")
                .value(DemoHealth::getAdmissionSequenceID)
                .ok()
                .endAssociation()
                .addAssociation("AppearsInVisit")
                .ofType("general.AppearsIn").to("HealthAppearsIn")
                .fromEntity("Providers")
                .toEntity("MedicalAssessments")
                .key("nc.SubjectIdentification", "general.AssessmentSequenceID")
                .addProperty("nc.SubjectIdentification")
                .value(DemoHealth::getSubjectIdentification)
                .ok()
                .addProperty("general.AssessmentSequenceID")
                .value(DemoHealth::getAssessmentSequenceID)
                .ok()
                .endAssociation()
                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>(2);
        flights.put(healthMapping, health);

        shuttle.launch(flights);
    }

    public static String getSubjectIdentification(Row row) {

        return "PERSON-" + row.getAs("SubjectIdentification");
    }

    public static String safeLastUsedParse(Row row) {
        String LastUsed = row.getAs("LastUsed");
        return dateHelper0.parse(LastUsed);
    }

    public static String safeAdmissionDateParse(Row row) {
        String visitDate = row.getAs("visitDate");
        return dateHelper0.parse(visitDate);
    }

    public static String getAssessmentSequenceID(Row row) {

        return "ASSESSMENT-" + row.getAs("id");
    }

    public static String getAdmissionSequenceID(Row row) {

        return "ADMISSION-" + row.getAs("id");
    }

    public static String getAppointmentSequenceID(Row row) {

        return "APPOINTMENT-" + row.getAs("id");
    }

    public static String safeDOBParse(Row row) {
        String dob = row.getAs("BirthDate");
        return dateHelper0.parse(dob);
    }

    public static String safeupcomingApptParse(Row row) {
        String upcomingAppt = row.getAs("upcomingAppt");
        return dateHelper0.parse(upcomingAppt);
    }

    public static String safemissedApptParse(Row row) {
        String missedAppt = row.getAs("missedAppt");
        return dateHelper0.parse(missedAppt);
    }
}
