package com.openlattice.integrations.jcDispatch;

import com.dataloom.authorization.PermissionsApi;
import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Maps;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import static lib.nameParsing.*;

public class Dispatch {

    private static final Logger logger = LoggerFactory
            .getLogger( Dispatch.class );

    private static final Environment environment = Environment.LOCAL;

    private static final DateTimeHelper dateHelper0 = new DateTimeHelper( DateTimeZone.forOffsetHours( -8 ), "yyyy-MM-dd HH:mm:ss.S" );

    public static void main( String[] args ) throws InterruptedException {

        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getJsonMapper() );

        final String personPath = args[ 0 ];
        final String unitPath = args[ 1 ];
        final String dispatchPath = args[ 2 ];
        final String disTypePath = args[ 3 ];
        final String jwtToken = args[ 4 ];

        final SparkSession sparkSession = MissionControl.getSparkSession();

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edmApi = retrofit.create( EdmApi.class );
        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );

        Dataset<Row> person = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( personPath );

        Dataset<Row> Unit = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( unitPath );

        Dataset<Row> dispatch = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( dispatchPath );

        Dataset<Row> disType = sparkSession
                .read()
                .format( "com.databricks.spark.csv" )
                .option( "header", "true" )
                .load( disTypePath );

        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
                .loadConfiguration( RequiredEdmElements.class );

        if ( requiredEdmElements != null ) {
            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
            manager.ensureEdmElementsExist( requiredEdmElements );
        }

        logger.info( "ER Field names: {}", Arrays.asList( person.schema().fieldNames() ) );
        logger.info( "ER Field names: {}", Arrays.asList( Unit.schema().fieldNames() ) );
        logger.info( "ER Field names: {}", Arrays.asList( dispatch.schema().fieldNames() ) );
        logger.info( "ER Field names: {}", Arrays.asList( disType.schema().fieldNames() ) );


        Flight UnitMapping = Flight
                .newFlight()
                .createEntities()
                .addEntity("Unit")
                .to("Unit")
                .useCurrentSync()
                .addProperty("general.UnitSequenceID", "officerid")
                .addProperty("place.OriginatingAgencyIdentifier", "ori")
                .addProperty("person.EmployeeID")
                .value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
                .addProperty("person.Active")
                .value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
                .addProperty("person.Title", "Title")
                .addProperty("person.FirstName")
                .value( row -> formatText( row.getAs( "FirstName" ) ) ).ok()
                .addProperty("person.LastName")
                .value( row -> formatText( row.getAs( "LastName" ) ) ).ok()
                .endEntity()
                .endEntities()
                .done();


        Flight dispatchMapping = Flight
                .newFlight()
                .createEntities()
                .addEntity("CallForService")
                .to("CallForService")
                .useCurrentSync()
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .addProperty("date.ReceivedDateTime")
                .value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                .addProperty("date.AlertedDateTime")
                .value( row -> dateHelper0.parse( row.getAs( "AlertedTime" ) ) ).ok()
                .addProperty( "date.DayOfWeek" )
                .value( row -> getDayOfWeek( ( dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).substring( 0, 10 ) ) ).ok()
                //.addProperty( "date.TimeOfDay" )
                //.value( row -> getTimeOfDay( ( dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).substring( 0, 19 ) ) ).ok()
                .addProperty("person.Operator")
                .value( row -> formatText( row.getAs( "Operator" ) ) ).ok()
                .addProperty("event.CFSTypeClass")
                .value( row -> getIntFromDouble( row.getAs( "TYPE_CLASS" ) ) ).ok()
                .addProperty("event.ProQA")
                .value( row -> getStringFromDouble( row.getAs( "PROQA" ) ) ).ok()
                .addProperty("event.ProQALevel", "PROQA_LEVEL")
                .addProperty("event.ProQAType", "PROQA_TYPE")
                .addProperty("event.HowReported", "HowReported")
                .addProperty("event.ESN")
                .value( row -> getIntFromDouble( row.getAs( "ESN" ) ) ).ok()
                .addProperty("event.CFS_Fire", "CFS_Fire")
                .addProperty("event.CFS_EMS", "CFS_EMS")
                .addProperty("event.CFS_LEA", "CFS_LEA")
                .addProperty("event.FireDispatchLevel", "FireDispatchLevel")
                .addProperty("event.Disposition", "ClearedBy")
                .addProperty("event.CITDisposition", "ClearedBy2")
                .addProperty("event.911CallNumber", "CallNumber_911")
                .addProperty("event.CFSCaseNumber", "Case_Number")
                .endEntity()
                .addEntity("Location")
                .to("Location")
                .useCurrentSync()
                .addProperty("general.LocationSequenceID")
                .value( row -> row.getAs( "Dis_ID" ) ).ok()
                .addProperty("location.name", "Location")
                .addProperty("location.PhoneNumber")
                .value( row -> getPhoneNumber( row.getAs( "LPhone" ) ) ).ok()
                .addProperty("location.DispatchZoneID")
                .value( row -> getIntFromDouble( row.getAs( "ZONE_ID" ) ) ).ok()
                .addProperty("location.DispatchZone", "Dis_Zone")
                .addProperty("location.DispatchSubZone", "SubZone")
                .addProperty("location.MedicalZone", "Medical_Zone")
                .addProperty("location.FireDistrict", "FireDistrict")
                .endEntity()
                .addEntity( "Address" )
                .to("Address")
                .useCurrentSync()
                .addProperty("location.address")
                .value( row -> getAddressID( formatText( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + " " +  formatText( row.getAs( "LCity" ) ) + row.getAs( "LState" ) + " " +  getZipCode( row.getAs( "LZip" ) ) ) ).ok()
                .addProperty("location.street")
                .value( row -> formatText( row.getAs( "LAddress" ) ) ).ok()
                .addProperty("location.apartment", "LAddress_Apt")
                .addProperty("location.city")
                .value( row -> formatText( row.getAs( "LCity" ) ) ).ok()
                .addProperty("location.state", "LState")
                .addProperty("location.zip")
                .value( row -> getZipCode( row.getAs( "LZip" ) ) ).ok()
                .endEntity()
                .addEntity("Unit")
                .to("Unit")
                .useCurrentSync()
                .addProperty( "general.UnitSequenceID" , "AssignedOfficerID")
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("LocatedAt")
                .ofType("general.LocatedAt").to("LocatedAt")
                .useCurrentSync()
                .fromEntity("CallForService")
                .toEntity("Location")
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .addProperty("general.LocationSequenceID")
                .value( row -> row.getAs( "Dis_ID" ) ).ok()
                .endAssociation()
                .addAssociation("Has")
                .ofType("general.Has").to("Has")
                .useCurrentSync()
                .fromEntity("Location")
                .toEntity("Address")
                .addProperty("general.LocationSequenceID")
                .value( row -> row.getAs( "Dis_ID" ) ).ok()
                .addProperty("location.address")
                .value( row -> getAddressID( formatText( row.getAs( "LAddress" ) ) + row.getAs( "LAddress_Apt" ) + formatText( row.getAs( "LCity" ) ) + row.getAs( "LState" ) + getZipCode( row.getAs( "LZip" ) ) ) ).ok()
                .endAssociation()
                .addAssociation("AssignedTo")
                .ofType("general.AssignedTo").to("AssignedTo")
                .useCurrentSync()
                .fromEntity("CallForService")
                .toEntity("Unit")
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .addProperty("general.UnitSequenceID", "AssignedOfficerID")
                .endAssociation()
                .endAssociations()
                .done();


        Flight disTypeMapping = Flight
                .newFlight()
                .createEntities()
                .addEntity("DispatchType")
                .to("DispatchType")
                .useCurrentSync()
                .addProperty("general.DispatchSequenceID", "Type_ID")
                .addProperty("event.DispatchTypeText", "Type_ID")
                .addProperty("event.DispatchTypePriority")
                .value( row -> getIntFromDouble( row.getAs( "Type_Priority" ) ) ).ok()
                .addProperty("event.TripNumber")
                .value( row -> getStringFromDouble( row.getAs( "TripNumber" ) ) ).ok()
                .endEntity()
                .addEntity("CallForService")
                .to("CallForService")
                .useCurrentSync()
                .addProperty( "general.CFSSequenceID" , "Dis_ID")
                .addProperty("date.EnRouteDateTime")
                .value( row -> dateHelper0.parse( row.getAs( "TimeEnroute" ) ) ).ok()
                .addProperty("date.CompletedDateTime")
                .value( row -> dateHelper0.parse( row.getAs( "TimeComp" ) ) ).ok()
                .endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("Includes")
                .ofType("general.Includes").to("Includes")
                .useCurrentSync()
                .fromEntity("CallForService")
                .toEntity("DispatchType")
                .addProperty( "general.CFSSequenceID" , "Dis_ID")
                .addProperty("general.DispatchSequenceID", "Type_ID")
                .endAssociation()
                .endAssociations()
                .done();


        Flight personMapping = Flight
                .newFlight()
                .createEntities()
                .addEntity( "Party" )
                .to( "Party" )
                .useCurrentSync()
                .addProperty( "nc.SubjectIdentification" , "ID")
                .addProperty( "nc.PersonGivenName" )
                .value( row -> getFirstName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonMiddleName" )
                .value( row -> getMiddleName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonSurName" )
                .value( row -> getLastName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonBirthDate" )
                .value( row -> dateHelper0.parse( row.getAs( "DOB" ) ) ).ok()
                .addProperty( "person.age" )
                .value( row -> getIntFromDouble( row.getAs( "Age" ) ) ).ok()
                .addProperty( "nc.PersonSex", "OSex" )
                .addProperty( "nc.PersonRace", "ORace" )
                .addProperty( "nc.PersonEthnicity", "Ethnicity" )
                .addProperty( "nc.PersonEyeColorText", "Eyes" )
                .addProperty( "nc.PersonHairColorText", "Hair" )
                .addProperty( "nc.PersonHeightMeasure" )
                .value( row -> getHeightInch( row.getAs( "Height" ) ) ).ok()
                .addProperty( "nc.PersonWeightMeasure" )
                .value( row -> getIntFromDouble( row.getAs( "Weight" ) ) ).ok()
                //.addProperty( "person.Juvenile", "Juv" )
                //.addProperty( "person.CellPhoneNumber", "CellPhone" )
                .endEntity()
                .addEntity("Location")
                .to("Location")
                .useCurrentSync()
                .addProperty("general.LocationSequenceID")
                .value( row -> row.getAs( "ID" ) ).ok()
                .addProperty( "location.name" )
                .value( row -> getAliasName( row.getAs( "OName" ) ) ).ok()
                .addProperty("location.PhoneNumber")
                .value( row -> getPhoneNumber( row.getAs( "OPhone" ) ) ).ok()
                .endEntity()
                .addEntity( "Address" )
                .to("Address")
                .useCurrentSync()
                .addProperty("location.address")
                .value( row -> getAddressID( formatText( row.getAs( "OAddress" ) ) + row.getAs( "OAddress_Apt" ) + formatText( row.getAs( "OCity" ) ) + row.getAs( "OState" ) + getZipCode( row.getAs( "OZip" ) ) ) ).ok()
                .addProperty("location.street")
                .value( row -> formatText( row.getAs( "OAddress" ) ) ).ok()
                .addProperty("location.apartment", "OAddress_Apt")
                .addProperty("location.city")
                .value( row -> formatText( row.getAs( "OCity" ) ) ).ok()
                .addProperty("location.state", "OState")
                .addProperty("location.zip")
                .value( row -> getZipCode( row.getAs( "OZip" ) ) ).ok()
                .endEntity()
                .addEntity("Vehicle")
                .to("Vehicle")
                .useCurrentSync()
                .addProperty("general.VehicleSequenceID", "ID")
                .addProperty("object.VehicleMake", "MAKE")
                .addProperty("object.VehicleModel", "MODEL")
                .addProperty("object.VehicleStyle", "Style")
                .addProperty("object.VehicleYear")
                .value( row -> getStrYear( row.getAs( "VehYear" ) ) ).ok()
                .addProperty("object.VehicleColor", "Color")
                .addProperty("object.VehicleVIN", "VIN")
                .addProperty("object.LicensePlateNumber", "LIC")
                .addProperty("object.LicensePlateState", "LIS")
                .addProperty("object.LicensePlateType", "LIT")
                .addProperty("object.LicensePlateYear")
                .value( row -> getStrYear( row.getAs( "LIY" ) ) ).ok()
                .endEntity()
                .addEntity("CallForService")
                .to("CallForService")
                .useCurrentSync()
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .addProperty("event.TransferVehicle", "TransferVehicle")
                .endEntity()
                //.addEntity("DispatchType")
                //.to("DispatchType")
                //.useCurrentSync()
                //.addProperty("general.DispatchSequenceID", "Dis_ID")
                //.addProperty("event.DispatchPersonType", "Type")
                //.endEntity()
                .endEntities()
                .createAssociations()
                .addAssociation("LocatedAt")
                .ofType("general.LocatedAt").to("LocatedAt")
                .fromEntity("Party")
                .toEntity("Location")
                .useCurrentSync()
                .addProperty( "nc.SubjectIdentification" , "ID")
                .addProperty("general.LocationSequenceID")
                .value( row -> row.getAs( "ID" ) ).ok()
                .endAssociation()
                .addAssociation("Has")
                .ofType("general.Has").to("Has")
                .useCurrentSync()
                .fromEntity("Location")
                .toEntity("Address")
                .addProperty("general.LocationSequenceID")
                .value( row -> "LocationOfParty" + row.getAs( "ID" ) ).ok()
                .addProperty("location.address")
                .value( row -> getAddressID( formatText( row.getAs( "OAddress" ) ) + row.getAs( "OAddress_Apt" ) + formatText( row.getAs( "OCity" ) ) + row.getAs( "OState" ) + getZipCode( row.getAs( "OZip" ) ) ) ).ok()
                .endAssociation()
                .addAssociation("Initiated")
                .ofType("general.Initiated").to("Initiated")
                .fromEntity("Party")
                .toEntity("CallForService")
                .addProperty("nc.SubjectIdentification", "ID")
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .endAssociation()
                .addAssociation("InvolvedIn")
                .ofType("general.InvolvedIn").to("InvolvedIn")
                .fromEntity("Vehicle")
                .toEntity("CallForService")
                .addProperty("general.VehicleSequenceID", "ID")
                .addProperty("general.CFSSequenceID", "Dis_ID")
                .endAssociation()
                .endAssociations()
                .done();


        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>();
        flights.put( UnitMapping, Unit );
        flights.put( dispatchMapping, dispatch );
        flights.put( disTypeMapping, disType );
        flights.put( personMapping, person );


        shuttle.launch( flights );

    }

    public static String getNotNullDate( Object obj ) {
        if ( obj != null ) {
            String date = obj.toString().trim();
            return date.substring( 0, 19 );
        }
        return null;
    }

    public static String getDateOfBirth( Object obj ) {
        if ( obj != null ) {
            String date = obj.toString().trim();
            return date.substring( 0, 10 );
        }
        return null;
    }

    public static Integer getHeightInch( Object obj ) {
        if ( obj != null ) {
            String height = obj.toString().trim();
            if (height.length() > 2) {
                String three = height.substring( 0, 3 );
                Integer feet = Integer.parseInt( String.valueOf( three.substring( 0, 1 ) ) );
                Integer inch = Integer.parseInt( String.valueOf( three.substring( 1 ) ) );
                return ( feet * 12 ) + inch;
            }

            return Integer.parseInt( String.valueOf( height ) );
        }
        return null;
    }

    public static String getEmployeeId( Object obj ) {
        if ( obj != null ) {
            String employeeId = obj.toString().trim();
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                return employeeId.substring( 2 ).trim();
            }
            return employeeId.trim();
        }
        return null;
    }

    public static Boolean getActive( Object obj ) {
        if ( obj != null ) {
            String active = obj.toString().trim();
            if ( active.toLowerCase().startsWith( "x_" ) ) {
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }
        return null;
    }

    public static String getDayOfWeek( Object obj ) {
        List<String> days = Arrays.asList( "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday" );
        if ( obj != null ) {
            String dateStr = obj.toString().trim();
            SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd");
            Date date;
            try {
                date = dateformat.parse( dateStr );
                return days.get( date.getDay() );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return dateStr;
        }
        return null;
    }

    public static String getTimeOfDay( Object obj ) {
        if ( obj != null ) {
            String dateStr = obj.toString().trim();
            SimpleDateFormat dateformat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date date;
            try {
                date = dateformat.parse( dateStr );
                System.out.println(String.valueOf( date.getHours() ));
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return dateStr;
        }
        return null;
    }

    public static Integer getIntFromDouble( Object obj ) {
        if ( obj != null ) {
            String s = obj.toString().trim();
            double d = Double.parseDouble(s);
            return (int) d;
        }
        return null;
    }

    public static String getStringFromDouble( Object obj ) {
        if ( obj != null ) {
            String s = obj.toString().trim();
            int d = getIntFromDouble( s );
            return Integer.toString(d);
        }
        return null;
    }

    public static String getZipCode( Object obj ) {
        if ( obj != null ) {
            String str = obj.toString().trim();
            String[] strDate = str.split( " " );
            if ( strDate.length > 1 ) {
                return getStringFromDouble( strDate[ strDate.length - 1 ]).trim();
            }
            String[] lZip = str.split( "-" );
            if ( lZip.length > 1 ) {
                return str;
            }
            return getStringFromDouble( strDate[ 0 ]).trim();
        }
        return null;
    }

    public static String getPhoneNumber( Object obj ) {
        if ( obj != null ) {
            String str = obj.toString().trim();
            str = str.replaceAll( "[()-]", "" );
            str = str.substring( 0, 10 );
            return str;
        }
        return null;
    }

    public static String getStrYear( Object obj ) {
        if ( obj != null ) {
            String str = obj.toString().trim();
            String[] strDate = str.split( "/" );
            if ( strDate.length > 1 ) {
                return getStringFromDouble( strDate[ strDate.length - 1 ]).trim();
            }
            if (str.contains( "DOB" )) {
                return "";
            }
            return getStringFromDouble( strDate[ 0 ]).trim();
        }
        return null;
    }

    public static String getVehicleID( Object obj ) {
        String id = obj.toString().trim();
        if ( id.contains( "null" ) ) {
            id = id.replaceAll( "null", "" );
            String[] idList = id.split( " " );
            return String.join("" , idList).trim();
        }
        return id;
    }
}
