package com.openlattice.integrations.jcDispatch;

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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Dispatch {

    private static final Logger logger = LoggerFactory
            .getLogger(Dispatch.class);

    private static final Environment environment = Environment.LOCAL;
    private static final DateTimeHelper dtHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");
    private static final DateTimeHelper bdHelper = new DateTimeHelper(TimeZones.America_NewYork,
            "MM/dd/yyyy");
    public static void main(String[] args) throws InterruptedException {

        final String disPersonPath = args[0];
        final String officerPath = args[1];
        final String dispatchPath = args[2];
        final String disTypePath = args[3];
        final String jwtToken = args[4];

        final SparkSession sparkSession = MissionControl.getSparkSession();


        logger.info("Using the following idToken: Bearer {}", jwtToken);
        Retrofit retrofit = RetrofitFactory.newClient(environment, () -> jwtToken);
        EdmApi edm = retrofit.create(EdmApi.class);

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

        Dataset<Row> dispatch = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(dispatchPath);

        Dataset<Row> disType = sparkSession
                .read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load(disTypePath);

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration(RequiredEdmElements.class);
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getYamlMapper());
        FullQualifedNameJacksonDeserializer.registerWithMapper(ObjectMappers.getJsonMapper());
        if (requiredEdmElements != null) {
            RequiredEdmElementsManager reem = new RequiredEdmElementsManager(edm,
                    retrofit.create(PermissionsApi.class));
            reem.ensureEdmElementsExist(requiredEdmElements);
        }

        logger.info("ER Field names: {}", Arrays.asList(disPerson.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(officer.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(dispatch.schema().fieldNames()));
        logger.info("ER Field names: {}", Arrays.asList(disType.schema().fieldNames()));

        Flight disPersonMapping = Flight.newFlight()
                .createEntities()
                .addEntity("Caller")
                .to("Caller")
                .ofType(new FullQualifiedName("general.person"))
                .key(new FullQualifiedName("nc.SubjectIdentification"))
                .addProperty("nc.SubjectIdentification")
                .value(Dispatch::getSubjectIdentification).ok()
                .addProperty("nc.PersonGivenName")
                .value(row -> getFirstName(row.getAs("OName"))).ok()
                .addProperty("nc.PersonMiddleName")
                .value(row -> getMiddleName(row.getAs("OName"))).ok()
                .addProperty("nc.PersonSurName")
                .value(row -> getLastName(row.getAs("OName"))).ok()
                .addProperty("nc.PersonSex", "OSex")
                .addProperty("nc.PersonRace", "ORace")
                .addProperty("nc.PersonEthnicity", "Ethnicity")
                .addProperty("nc.PersonHeightMeasure", "Height")
                .addProperty("nc.PersonWeightMeasure", "Weight")
                .addProperty("nc.PersonEyeColorText", "Eyes")
                .addProperty("nc.PersonHairColorText", "Hair")
                .addProperty("nc.PersonBirthDate")
                .value(Dispatch::safeDOBParse).ok()
                .addProperty("person.age", "Age")
                .endEntity()
                //.addEntity("ReporterLocation")
                //.to("ReporterLocation")
                //.ofType("general.Location")
                //.key("general.StringID")
                //.addProperty("general.StringID")
                //.value(Dispatch::getAddress).ok()
                //.addProperty("place.Street", "OAddress")
                //.addProperty("place.Apartment", "OAddress_Apt")
                //.addProperty("place.City", "OCity")
                //.addProperty("place.State", "OState")
                //.addProperty("place.ZipCode", "OZip")
                //.addProperty("place.PhoneNumber", "OPhone")
                //.endEntity()
                //.addEntity("Vehicle")
                //.to("Vehicle")
                //.ofType("general.Vehicle")
                //.key("general.StringID")
                //.addProperty("general.StringID", "Dis_ID")
                //.addProperty("object.VehicleMake", "MAKE")
                //.addProperty("object.VehicleModel", "MODEL")
                //.addProperty("object.VehicleStyle", "Style")
                //.addProperty("object.VehicleYear", "VehYear")
                //.addProperty("object.VehicleColor", "Color")
                //.addProperty("object.VehicleVIN", "VIN")
                //.addProperty("object.LicensePlateNumber", "LIC")
                //.addProperty("object.LicensePlateState", "LIS")
                //.addProperty("object.LicensePlateType", "LIT")
                //.addProperty("object.LicensePlateYear", "LIY")
                //.endEntity()
                .endEntities()
                //.createAssociations()
                //.addAssociation("ReporterLocatedAt")
                //.ofType("general.LocatedAt").to("ReporterLocatedAt")
                //.fromEntity("Caller")
                //.toEntity("ReporterLocation")
                //.key("nc.SubjectIdentification", "general.StringID")
                //.addProperty("nc.SubjectIdentification")
                //.value(Dispatch::getSubjectIdentification).ok()
                //.addProperty("general.StringID")
                //.value(Dispatch::getAddress).ok()
                //.endAssociation()
                //.endAssociations()
                .done();

        //Flight officerMapping = Flight.newFlight()
        //        .createEntities()
        //        .addEntity("Officer")
        //        .to("Officer")
        //        .ofType(new FullQualifiedName("general.Officer"))
        //        .key(new FullQualifiedName("general.OfficerSequenceID"))
        //        .addProperty("general.OfficerSequenceID", "officerid")
        //        .addProperty("person.EmployeeID")
        //        .value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
        //        .addProperty("person.FirstName", "FirstName")
        //        .addProperty("person.LastName", "LastName")
        //        .addProperty("person.Title", "Title")
        //        .addProperty("person.Active")
        //        .value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
        //        .addProperty("place.OriginatingAgencyIdentifier", "ori")
        //        .endEntity()
        //        .endEntities()
        //        .done();
//
        //Flight dispatchMapping = Flight.newFlight()
        //        .createEntities()
        //        .addEntity("CallForService")
        //        .to("CallForService")
        //        .ofType(new FullQualifiedName("general.CallForService"))
        //        .key(new FullQualifiedName("general.CFSSequenceID"))
        //        .addProperty("general.CFSSequenceID", "Dis_ID")
        //        .addProperty("date.ReceivedDateTime")
        //        .value(Dispatch::safeRSVDDateTimeParse).ok()
        //        .addProperty("date.AlertedDateTime")
        //        .value( row -> getAsDateTime( row.getAs( "AlertedTime" ) ) ).ok()
        //        .addProperty("date.DayOfWeek")
        //        .value( row -> getAsDate( row.getAs( "Dis_Date" ) ) ).ok()
        //        //.addProperty("date.TimeOfDay")
        //        //.value( row -> getAsTime( row.getAs( "DIS_TIME" ) ) ).ok()
        //        .addProperty("event.DispatchCaseNumber", "Case_Number")
        //        .addProperty("person.Operator", "Operator")
        //        .addProperty("event.DispatchTypeText", "TYPE_ID")
        //        .addProperty("event.DispatchTypeClass", "TYPE_CLASS")
        //        .addProperty("event.ProQA", "PROQA")
        //        .addProperty("event.ProQALevel", "PROQA_LEVEL")
        //        .addProperty("event.ProQAType", "PROQA_TYPE")
        //        .addProperty("event.HowReported", "HowReported")
        //        .addProperty("event.ESN", "ESN")
        //        .addProperty("event.CFS_Fire", "CFS_Fire")
        //        .addProperty("event.CFS_EMS", "CFS_EMS")
        //        .addProperty("event.CFS_LEA", "CFS_LEA")
        //        .addProperty("event.FireDispatchLevel", "FireDispatchLevel")
        //        .addProperty("event.DispatchPriority", "Priority")
        //        .addProperty("event.Disposition", "ClearedBy")
        //        .addProperty("event.CITDisposition", "ClearedBy2")
        //        .endEntity()
        //        .addEntity("DispatchLocation")
        //        .to("DispatchLocation")
        //        .ofType("general.Location")
        //        .key("general.StringID")
        //        .addProperty("general.StringID")
        //        .value(Dispatch::getDispatchAddress).ok()
        //        .addProperty("place.Location", "Location")
        //        .addProperty("place.Street", "LAddress")
        //        .addProperty("place.Apartment", "LAddress_Apt")
        //        .addProperty("place.City", "LCity")
        //        .addProperty("place.State", "LState")
        //        .addProperty("place.ZipCode", "LZip")
        //        .addProperty("place.PhoneNumber", "LPhone")
        //        .addProperty("place.DispatchZoneID", "ZONE_ID")
        //        .addProperty("place.DispatchZone", "Dis_Zone")
        //        .addProperty("place.DispatchSubZone", "SubZone")
        //        .addProperty("place.MedicalZone", "Medical_Zone")
        //        .addProperty("place.FireDistrict", "FireDistrict")
        //        .endEntity()
        //        .addEntity("Caller")
        //        .to("Caller")
        //        .useCurrentSync()
        //        .ofType("general.person")
        //        .key("nc.SubjectIdentification")
        //        .addProperty("nc.SubjectIdentification")
        //        .value(Dispatch::getSubjectIdentification).ok()
        //        .endEntity()
        //        .endEntities()
        //        .createAssociations()
        //        .addAssociation("Initiated")
        //        .ofType("general.Initiated").to("Initiated")
        //        .fromEntity("Caller")
        //        .toEntity("DispatchRecord")
        //        .key("nc.SubjectIdentification", "general.DispatchSequenceID")
        //        .addProperty("nc.SubjectIdentification")
        //        .value(Dispatch::getSubjectIdentification).ok()
        //        .addProperty("general.DispatchSequenceID", "Dis_ID")
        //        .endAssociation()
        //        .addAssociation("OccurredAt")
        //        .ofType("justice.occurredat").to("OccurredAt")
        //        .fromEntity("DispatchRecord")
        //        .toEntity("DispatchLocation")
        //        .key("general.DispatchSequenceID", "general.StringID")
        //        .addProperty("general.DispatchSequenceID", "Dis_ID")
        //        .addProperty("general.StringID")
        //        .value(Dispatch::getDispatchAddress).ok()
        //        .endAssociation()
        //        .endAssociations()
        //        .done();
//
        //Flight disTypeMapping = Flight.newFlight()
        //        .createEntities()
        //        .addEntity("CallForService")
        //        .to("CallForService")
        //        .ofType(new FullQualifiedName("general.CallForService"))
        //        .key(new FullQualifiedName("general.CFSSequenceID"))
        //        .addProperty("general.CFSSequenceID", "Dis_ID")
        //        .addProperty("date.ReceivedDateTime")
        //        .value( row -> getAsDateTime( row.getAs( "Timercvd" ) ) ).ok()
        //        .addProperty("date.EnRouteDateTime")
        //        .value( row -> getAsDateTime( row.getAs( "TimeEnroute" ) ) ).ok()
        //        .addProperty("date.CompletedDateTime")
        //        .value( row -> getAsDateTime( row.getAs( "TimeComp" ) ) ).ok()
        //        .addProperty("event.DispatchTypeID", "Dispatch_Type_ID")
        //        .addProperty("event.DispatchTypeText", "Type_ID")
        //        .addProperty("event.CFSPriority", "Type_Priority")
        //        .addProperty("event.TripNumber", "TripNumber")
        //        .addProperty("event.Disposition", "Disposition")
        //        .endEntity()
        //        .addEntity("Officer")
        //        .to("Officer")
        //        .useCurrentSync()
        //        .ofType("general.Officer")
        //        .key("general.OfficerSequenceID")
        //        .addProperty("general.OfficerSequenceID", "CallForServiceOfficerId")
        //        .endEntity()
        //        .addEntity("Vehicle")
        //        .to("Vehicle")
        //        .useCurrentSync()
        //        .ofType("general.Vehicle")
        //        .key("general.StringID")
        //        .addProperty("general.StringID", "Dis_ID")
        //        .endEntity()
        //        .endEntities()
        //        .createAssociations()
        //        .addAssociation("AssignedTo")
        //        .ofType("general.AssignedTo").to("AssignedTo")
        //        .fromEntity("Officer")
        //        .toEntity("CallForService")
        //        .key("general.OfficerSequenceID", "general.CFSSequenceID")
        //        .addProperty("general.OfficerSequenceID", "CallForServiceOfficerId")
        //        .addProperty("general.CFSSequenceID", "Dis_ID")
        //        .endAssociation()
        //        .addAssociation("InvolvedIn")
        //        .ofType("general.InvolvedIn").to("InvolvedIn")
        //        .fromEntity("Vehicle")
        //        .toEntity("CallForService")
        //        .key("general.StringID", "general.CFSSequenceID")
        //        .addProperty("general.StringID", "Dis_ID")
        //        .addProperty("general.CFSSequenceID", "Dis_ID")
        //        .endAssociation()
        //        .endAssociations()
        //        .done();


        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Dataset<Row>> flights = new HashMap<>(1);
        flights.put(disPersonMapping, disPerson);
        //flights.put(officerMapping, officer);
        //flights.put(disTypeMapping, disType);
        //flights.put(dispatchMapping, dispatch);

        shuttle.launch(flights);


    }

    public static String getSubjectIdentification(Row row) {

        return "PARTY-" + row.getAs("ID");
    }

    public static String cleanText( Object obj ) {
        String name = obj.toString().trim();
        name = name.replaceAll("[\\d]+", "").trim();
        return name;
    }

    public static String getFirstName( Object obj ) {
        if (obj != null) {
            String name = cleanText(obj);
            if (!(name.contains("UNIVERSITY") ||
                    name.contains("INC") ||
                    name.contains("IOWA") ||
                    name.contains("ADT") ||
                    name.contains("ALL") ||
                    name.contains("VERIZON") ||
                    name.contains("SPRINT") ||
                    name.contains("SANDWICHES") ||
                    name.contains("LC") ||
                    name.contains("KEY") ||
                    name.contains("AT&T") ||
                    name.contains("BLDG") ||
                    name.contains("CENTER") ||
                    name.contains("CELLULAR"))) {
                String[] strNames = name.split(",");
                if (strNames.length > 1) {
                    if (strNames[1].length() > 1) {
                        String fName = strNames[1].trim();
                        String[] fNames = fName.split(" ");
                        String firstName = fNames[0].trim();
                        return firstName.toUpperCase().trim();
                    }
                    return null;
                }
                String[] fullNames = strNames[0].split(" ");
                String cName = fullNames[0].trim();
                return cName.toUpperCase().trim();
            }
            return null;
        }
        return null;
    }

    public static String getLastName( Object obj ) {
        if (obj != null) {
            String name = cleanText(obj);
            if (!( name.contains("UNIVERSITY") ||
                    name.contains("INC") ||
                    name.contains("IOWA") ||
                    name.contains("ADT") ||
                    name.contains("ALL") ||
                    name.contains("VERIZON") ||
                    name.contains("SPRINT") ||
                    name.contains("SANDWICHES") ||
                    name.contains("LC")) ||
                    name.contains("KEY") ||
                    name.contains("AT&T") ||
                    name.contains("BLDG") ||
                    name.contains("CENTER") ||
                    name.contains("CELLULAR") ) {
                String[] strNames = name.split(",");
                if (strNames.length > 1) {
                    String lastName = strNames[0].trim();
                    return lastName.toUpperCase().trim();
                }
                String[] fullNames = strNames[0].split(" ");
                String lName = fullNames[fullNames.length-1].trim();
                return lName.toUpperCase().trim();
            }
            return null;
        }
        return null;
    }

    public static String getMiddleName( Object obj ) {
        if (obj != null) {
            String name = cleanText(obj);
            if (!(name.contains("UNIVERSITY") ||
                    name.contains("INC") ||
                    name.contains("IOWA") ||
                    name.contains("ADT") ||
                    name.contains("ALL") ||
                    name.contains("VERIZON") ||
                    name.contains("SPRINT") ||
                    name.contains("SANDWICHES") ||
                    name.contains("LC") ||
                    name.contains("KEY") ||
                    name.contains("AT&T") ||
                    name.contains("BLDG") ||
                    name.contains("CENTER") ||
                    name.contains("CELLULAR"))) {
                String[] strNames = name.split(",");
                if (strNames.length > 1) {
                    String fName = strNames[1].trim();
                    String[] fNames = fName.split(" ");
                    if (fNames.length > 2) {
                        String mName = fNames[fNames.length-2].trim();
                        return mName.toUpperCase().trim();
                    }
                    return null;
                }
                String[] middleNames = strNames[0].split(" ");
                if (middleNames.length > 2) {
                    String dName = middleNames[1].trim();
                    return dName.toUpperCase().trim();
                }
                return null;
            }
            return null;
        }
        return null;
    }

    public static String safeDOBParse(Row row) {
        String dob = row.getAs("DOB");
        return bdHelper.parse(dob);
    }
}
