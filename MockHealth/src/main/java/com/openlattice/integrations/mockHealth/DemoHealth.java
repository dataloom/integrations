package com.openlattice.integrations.mockHealth;

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
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import com.openlattice.shuttle.util.Parsers;
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
    private static final Environment environment = Environment.STAGING;

    private static final DateTimeHelper dateHelper0 = new DateTimeHelper( DateTimeZone.forOffsetHours( -8 ), "MM/dd/yyyy" );

    public static void main(String[] args) throws InterruptedException {

        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());

        final String healthPath = args[0];
        final String jwtToken = args[1];

        final SparkSession sparkSession = MissionControl.getSparkSession();

        logger.info("Using the following idToken: Bearer {}", jwtToken);

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );

        Dataset<Row> health = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(healthPath);

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }

        logger.info("ER Field names: {}", Arrays.asList(health.schema().fieldNames()));

        // @formatter:off
        Flight healthMapping = Flight
                .newFlight()
                    .createEntities()
                        .addEntity("Patients")
                            .to("Patients")
                            .addProperty("nc.SubjectIdentification", "SubjectIdentification")
                            .addProperty("nc.PersonGivenName", "FirstName")
                            .addProperty("nc.PersonSurName", "LastName")
                            .addProperty("nc.PersonSex", "Sex")
                            .addProperty("nc.PersonRace", "Race")
                            .addProperty("nc.PersonEthnicity", "Ethnicity")
                            .addProperty("nc.PersonBirthDate")
                                .value(row -> dateHelper0.parse( row.getAs( "BirthDate" ) ) ).ok()
                        .endEntity()
                        .addEntity("Insurance")
                            .to("Insurance")
                            .addProperty("insurance.SequenceID", "id")
                            .addProperty("general.FullName", "Insurance")
                            .addProperty("insurance.Status", "Status")
                            .addProperty("insurance.MemberIdentification", "MemberID")
                            .addProperty("insurance.GroupNumber", "GroupNumber")
                            .addProperty("insurance.PlanType", "PlanType")
                        .endEntity()
                        .addEntity("Providers")
                            .to("Providers")
                            .addProperty("provider.NationalProviderIdentification", "NPI")
                            .addProperty("general.Title")
                                .value(row -> getTitle( row.getAs( "providerNames" ) ) ).ok()
                            .addProperty("general.FirstName")
                                .value(row -> getFName( row.getAs( "providerNames" ) ) ).ok()
                            .addProperty("general.LastName")
                                .value(row -> getLName( row.getAs( "providerNames" ) ) ).ok()
                        .endEntity()
                        .addEntity( "Substance" )
                            .to( "Substance" )
                            .addProperty("substance.SequenceID", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                            .addProperty("substance.Type", "DrugType")
                            .addProperty("general.FullName", "DrugType")
                            .addProperty("substance.Route", "DrugRoute")
                            .addProperty("substance.Frequency", "DrugFreq")
                            .addProperty("substance.LastUsed", "LastUsed")
                        .endEntity()
                        .addEntity("MedicalHistory")
                            .to("MedicalHistory")
                            .addProperty("medical.HistoryID", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                            .addProperty("neurological.HistoryOfSeizure", "Seizure")
                            .addProperty("screening.LastSeizureDate", "LastSeizureDate")
                            .addProperty("neurological.DeliriumTremens")
                                .value(row -> getDT( row.getAs( "MedicalConditions" ) ) ).ok()
                            .addProperty("cardiovascular.ChestPain", "ChestPain")
                            .addProperty("screening.AbdominalPain", "AbdominalPain")
                            .addProperty("respiratory.ShortnessOfBreathe")
                                .value(row -> getSOB( row.getAs( "MedicalConditions" ) ) ).ok()
                            .addProperty("screening.Pregnant")
                                .value(row -> getPregnancy( row.getAs( "MedicalConditions" ) ) ).ok()
                        .endEntity()
                        .addEntity("BioMedicalAssessment")
                            .to("BioMedicalAssessment")
                            .addProperty("biomedical.SequenceID", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                            .addProperty("vitals.Temperature", "Temperature")
                            .addProperty("vitals.BloodPressure", "BloodPressure")
                        .endEntity()
                        .addEntity("Medication")
                            .to("Medication")
                            .addProperty("medication.SequenceID", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                            .addProperty("general.FullName", "currentMedication")
                            .addProperty("medication.Dosage", "Strength")
                            .addProperty("medication.Regimen", "Amount")
                        .endEntity()
                    .endEntities()
                    .createAssociations()
                        .addAssociation("InsuredBy")
                            .ofType("general.InsuredBy").to("InsuredBy")
                            .fromEntity("Patients")
                            .toEntity("Insurance")
                            .addProperty("general.stringid", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                        .endAssociation()
                        .addAssociation("Used")
                            .ofType("general.Used").to("Used")
                            .fromEntity("Patients")
                            .toEntity("Substance")
                            .addProperty("nc.SubjectIdentification", "id")
                        .endAssociation()
                        .addAssociation("Taking")
                            .ofType("medical.Taking").to("Taking")
                            .fromEntity("Patients")
                            .toEntity("Medication")
                            .addProperty("nc.SubjectIdentification", "id")
                        .endAssociation()
                        .addAssociation("Reported")
                            .ofType("medical.Reported").to("Reported")
                            .fromEntity("Patients")
                            .toEntity("MedicalHistory")
                            .addProperty("general.stringid", "id")
                        .endAssociation()
                        .addAssociation("Assessed")
                            .ofType("general.Assessed").to("Assessed")
                            .fromEntity("Patients")
                            .toEntity("BioMedicalAssessment")
                            .addProperty("medical.InitialAssessmentID", "id")
                        .endAssociation()
                        .addAssociation("AssessedBy")
                            .ofType("general.AssessedBy").to("AssessedBy")
                            .fromEntity("BioMedicalAssessment")
                            .toEntity("Providers")
                            .addProperty("provider.NationalProviderIdentification", "id")
                            .addProperty("date.CompletedDateTime")
                                .value(row -> dateHelper0.parse( row.getAs( "visitDate" ) ) ).ok()
                        .endAssociation()
                    .endAssociations()
                .done();

        // @formatter:on
        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>();
        flights.put(healthMapping, health);

        shuttle.launch(flights);
    }
    

    public static String getTitle( Object obj ) {
        String title = Parsers.getAsString( obj );
        if ( title != null ) {
            if ( title.contains(",") ) {
                return title.split(",")[ 1 ].trim();
            }
            return "";
        }
        return null;
    }

    public static String getFName( Object obj ) {
        String fName = Parsers.getAsString( obj );
        if ( fName != null ) {
            return fName.split( "," )[ 0 ].split(" ")[ 0 ].trim();
        }
        return null;
    }

    public static String getLName( Object obj ) {
        String lName = Parsers.getAsString( obj );
        if ( lName != null ) {
            return lName.split( "," )[ 0 ].split(" ")[ 1 ].trim();
        }
        return null;
    }

    public static Boolean getSOB( Object obj ) {
        String SOB = Parsers.getAsString( obj );
        if ( SOB != null ) {
            if ( SOB.contains( "ShorlnessOfBreath" ) ) {
                return true;
            }
            return false;
        }
        return null;
    }

    public static Boolean getDT( Object obj ) {
        String DT = Parsers.getAsString( obj );
        if ( DT != null ) {
            if ( DT.contains( "Delirium Tremens" ) ) {
                return true;
            }
            return false;
        }
        return null;
    }

    public static Boolean getPregnancy( Object obj ) {
        String pregnancy = Parsers.getAsString( obj );
        if ( pregnancy != null ) {
            if ( pregnancy.contains( "Pregnancy" ) ) {
                return true;
            }
            return false;
        }
        return null;
    }
}
