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

public class DemoHealth {

    private static final Logger logger = LoggerFactory
            .getLogger(DemoHealth.class);
    private static final Environment environment = Environment.LOCAL;
    private static final DateTimeHelper dtHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");
    private static final DateTimeHelper bdHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");

    public static void main(String[] args) throws InterruptedException {

        final String healthPath = args[0];
        final String jwtToken = args[1];
        //final String username = args[ 1 ];
        //final String password = args[ 2 ];
        final SparkSession sparkSession = MissionControl.getSparkSession();
        //final String jwtToken = MissionControl.getIdToken( username, password );

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
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());
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
            .endEntity()
            .addEntity("Providers")
            .to("DemoProviders")
            .ofType("general.Provider")
            .key("nc.ProviderIdentification")
            .addProperty("nc.ProviderIdentification")
            .value(DemoHealth::getProviderIdentification).ok()
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
            .addProperty("event.SubstanceFrequency", "DrugFreq")
            .addProperty("event.Seizure", "Seizure")
            .addProperty("event.ChestPain", "ChestPain")
            .addProperty("event.AbdominalPain", "AbdominalPain")
            .addProperty("event.CurrentMedicationName", "currentMedication")
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
            .addAssociation("AssessedBy")
            .ofType("general.AssessedBy").to("AssessedBy")
            .fromEntity("Providers")
            .toEntity("MedicalAssessments")
            .key("nc.ProviderIdentification", "general.AssessmentSequenceID")
            .addProperty("nc.ProviderIdentification")
            .value(DemoHealth::getProviderIdentification)
            .ok()
            .addProperty("general.AssessmentSequenceID")
            .value(DemoHealth::getAdmissionSequenceID)
            .ok()
            .endAssociation()
            .endAssociations()
            .done();

        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>(1);
        flights.put(healthMapping, health);

        shuttle.launch(flights);
    }

    public static String getSubjectIdentification(Row row) {

        return "PATIENT-" + row.getAs("SubjectIdentification");
    }

    public static String getProviderIdentification(Row row) {

        return "PROVIDER-" + row.getAs("NPI");
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

    public static String getAdmissionSequenceID(Row row) {

        return "ADMISSION-" + row.getAs("id");
    }

    public static String getAppointmentSequenceID(Row row) {

        return "APPOINTMENT-" + row.getAs("id");
    }

    public static String safeDOBParse(Row row) {
        String dob = row.getAs("BirthDate");
        return bdHelper.parse(dob);
    }

    public static String safeupcomingApptParse(Row row) {
        String upcomingAppt = row.getAs("upcomingAppt");
        return dtHelper.parse(upcomingAppt);
    }

    public static String safemissedApptParse(Row row) {
        String missedAppt = row.getAs("missedAppt");
        return dtHelper.parse(missedAppt);
    }

}
