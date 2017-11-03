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

import java.text.SimpleDateFormat;
import java.util.*;

import static com.openlattice.integrations.jcDispatch.lib.NameParsing.*;

public class Dispatch {

    private static final Logger logger = LoggerFactory
            .getLogger( Dispatch.class );

    private static final Environment environment = Environment.STAGING;

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

        Dataset<Row> unit = sparkSession
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
        logger.info( "ER Field names: {}", Arrays.asList( unit.schema().fieldNames() ) );
        logger.info( "ER Field names: {}", Arrays.asList( dispatch.schema().fieldNames() ) );
        logger.info( "ER Field names: {}", Arrays.asList( disType.schema().fieldNames() ) );

        // @formatter:off
        Flight unitMapping = Flight
                .newFlight()
                    .createEntities()
                        .addEntity("Unit")
                            .to("Unit")
                            .useCurrentSync()
                            .addProperty("general.unitID", "officerid")
                            .addProperty("place.originatingAgencyIdentifier", "ori")
                            .addProperty("general.active")
                            .value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
                            .addProperty("person.employeeID")
                            .value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
                            .addProperty("person.title", "Title")
                            .addProperty("nc.PersonGivenName")
                            .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "FirstName" ) ) ).ok()
                            .addProperty("nc.PersonSurName")
                            .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "LastName" ) ) ).ok()
                        .endEntity()
                    .endEntities()
                .done();
        // @formatter:on


        // @formatter:off
        Flight dispatchMapping = Flight
                .newFlight()
                    .createEntities()
                        .addEntity("CallForService")
                            .to("CallForService")
                            .useCurrentSync()
                            .addProperty("general.cfsID", "Dis_ID")
                            .addProperty("date.receivedDatetime")
                                .value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                            .addProperty("date.alertedDatetime")
                                .value( row -> dateHelper0.parse( row.getAs( "AlertedTime" ) ) ).ok()
                            .addProperty( "date.dayOfWeek" )
                                .value( row -> getDayOfWeek( ( dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).substring( 0, 10 ) ) )
                                .ok()
                            .addProperty( "date.timeOfDay" )
                                .value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ).substring( 12, 19 ) )
                                .ok()
                            .addProperty("person.operator")
                                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "Operator" ) ) )
                                .ok()
                            .addProperty("cfs.class")
                                .value( row -> getIntFromDouble( row.getAs( "TYPE_CLASS" ) ) )
                                .ok()
                            .addProperty("dispatch.proQa")
                                .value( row -> getStringFromDouble( row.getAs( "PROQA" ) ) )
                                .ok()
                            .addProperty("dispatch.proQaLevel", "PROQA_LEVEL")
                            .addProperty("dispatch.proQaType", "PROQA_TYPE")
                            .addProperty("dispatch.howReported", "HowReported")
                            .addProperty("dispatch.esn")
                                .value( row -> getIntFromDouble( row.getAs( "ESN" ) ) )
                                .ok()
                            .addProperty("dispatch.cfsFire", "CFS_Fire")
                            .addProperty("dispatch.cfsEMS", "CFS_EMS")
                            .addProperty("dispatch.cfsLEA", "CFS_LEA")
                            .addProperty("dispatch.fireLevel", "FireDispatchLevel")
                            .addProperty("dispatch.disposition", "ClearedBy")
                            .addProperty("dispatch.citDisposition", "ClearedBy2")
                            .addProperty("dispatch.911callNumber", "CallNumber_911")
                            .addProperty("cfs.caseNumber", "Case_Number")
                        .endEntity()
                        .addEntity("Place")
                            .to("Place")
                            .useCurrentSync()
                            .addProperty("place.uniqueID")
                            .value( row -> row.getAs( "Location" ) ).ok()
                            .addProperty("general.FullName", "Location")
                            .addProperty("contact.PhoneNumber")
                                .value( row -> getPhoneNumber( row.getAs( "LPhone" ) ) ).ok()
                        .endEntity()
                        .addEntity("DispatchZone")
                            .to("DispatchZone")
                            .useCurrentSync()
                            .addProperty("dispatch.uniqueZoneID", "Dis_ID")
                            .addProperty("dispatch.zoneID")
                                .value( row -> getIntFromDouble( row.getAs( "ZONE_ID" ) ) ).ok()
                            .addProperty("dispatch.zone", "Dis_Zone")
                            .addProperty("dispatch.subZone", "SubZone")
                            .addProperty("dispatch.medicalZone", "Medical_Zone")
                            .addProperty("dispatch.fireDistrict", "FireDistrict")
                        .endEntity()
                        .addEntity( "Address" )
                            .to("Address")
                            .useCurrentSync()
                            .addProperty("location.address")
                                .value( row -> getAddressID( getStreet( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + " " +  addSpaceAfterCommaUpperCase( row.getAs( "LCity" ) ) + row.getAs( "LState" ) + " " +  getZipCode( row.getAs( "LZip" ) ) ) )
                                .ok()
                            .addProperty("location.street")
                                .value( row -> getStreet( row.getAs( "LAddress" ) ) ).ok()
                            .addProperty("location.intersection")
                                .value( row -> getIntersection( row.getAs( "LAddress" ) ) ).ok()
                            .addProperty("location.apartment", "LAddress_Apt")
                            .addProperty("location.city")
                                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "LCity" ) ) ).ok()
                            .addProperty("location.state", "LState")
                            .addProperty("location.zip")
                                .value( row -> getZipCode( row.getAs( "LZip" ) ) ).ok()
                        .endEntity()
                        .addEntity("Unit")
                            .to("Unit")
                            .useCurrentSync()
                            .addProperty( "general.unitID" , "AssignedOfficerID")
                        .endEntity()
                            .addEntity("Party")
                            .to("Party")
                            .useCurrentSync()
                            .addProperty( "nc.SubjectIdentification" , "Dis_ID")
                        .endEntity()
                    .endEntities()
                    .createAssociations()
                        .addAssociation("OccurredAt1")
                            .ofType("general.occurredat").to("OccurredAt")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("Place")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("OccurredAt2")
                            .ofType("general.occurredat").to("OccurredAt")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("Address")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("AppearsIn")
                            .ofType("general.appearsin").to("AppearsIn")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("DispatchZone")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("Has")
                            .ofType("general.Has").to("Has")
                            .useCurrentSync()
                            .fromEntity("Place")
                            .toEntity("Address")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("AssignedTo")
                            .ofType("general.assignedto").to("AssignedTo")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("Unit")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on


        // @formatter:off
        Flight disTypeMapping = Flight
                .newFlight()
                    .createEntities()
                        .addEntity("DispatchType")
                            .to("DispatchType")
                            .useCurrentSync()
                            .addProperty("general.dispatchID", "Type_ID")
                            .addProperty("dispatch.typeText", "Type_ID")
                            .addProperty("dispatch.typePriority")
                                .value( row -> getIntFromDouble( row.getAs( "Type_Priority" ) ) ).ok()
                            .addProperty("dispatch.tripNumber")
                                .value( row -> getStringFromDouble( row.getAs( "TripNumber" ) ) ).ok()
                        .endEntity()
                        .addEntity("CallForService")
                            .to("CallForService")
                            .useCurrentSync()
                            .addProperty( "general.cfsID" , "Dis_ID")
                            .addProperty("date.enRouteDatetime")
                                .value( row -> dateHelper0.parse( row.getAs( "TimeEnroute" ) ) ).ok()
                            .addProperty("date.completedDatetime")
                                .value( row -> dateHelper0.parse( row.getAs( "TimeComp" ) ) ).ok()
                        .endEntity()
                    .endEntities()
                    .createAssociations()
                        .addAssociation("Includes")
                            .ofType("general.Includes").to("Includes")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("DispatchType")
                            .addProperty( "general.stringid" , "Dis_ID")
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on


        // @formatter:off
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
                            .addProperty( "im.PersonNickName" )
                                .value( row -> getName( row.getAs( "OName" ) ) ).ok()
                            .addProperty( "nc.PersonBirthDate" )
                                .value( row -> dateHelper0.parse( row.getAs( "DOB" ) ) ).ok()
                            .addProperty( "nc.PersonSex", "OSex" )
                            .addProperty( "nc.PersonRace", "ORace" )
                            .addProperty( "nc.PersonEthnicity", "Ethnicity" )
                            .addProperty( "nc.PersonEyeColorText", "Eyes" )
                            .addProperty( "nc.PersonHairColorText", "Hair" )
                            .addProperty( "person.age" )
                                .value( row -> getIntFromDouble( row.getAs( "Age" ) ) ).ok()
                            .addProperty( "nc.PersonHeightMeasure" )
                                .value( row -> getHeightInch( row.getAs( "Height" ) ) ).ok()
                            .addProperty( "nc.PersonWeightMeasure" )
                                .value( row -> getIntFromDouble( row.getAs( "Weight" ) ) ).ok()
                        .endEntity()
                        .addEntity("ContactInformation1")
                            .to("ContactInformation")
                            .useCurrentSync()
                            .addProperty( "general.stringid", "ID" )
                            .addProperty( "contact.PhoneNumber", "CellPhone" )
                            .endEntity()
                        .addEntity("ContactInformation2")
                            .to("ContactInformation")
                            .useCurrentSync()
                            .addProperty( "general.stringid", "ID" )
                            .addProperty( "contact.PhoneNumber", "OPhone" )
                        .endEntity()
                        .addEntity( "Address" )
                            .to("Address")
                            .useCurrentSync()
                            .addProperty("location.address")
                                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + row.getAs( "OAddress_Apt" ) + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + row.getAs( "OState" ) + getZipCode( row.getAs( "OZip" ) ) ) )
                                .ok()
                            .addProperty("location.street")
                                .value( row -> getStreet( row.getAs( "OAddress" ) ) ).ok()
                            .addProperty("location.intersection")
                                .value( row -> getIntersection( row.getAs( "OAddress" ) ) ).ok()
                            .addProperty("location.apartment", "OAddress_Apt")
                            .addProperty("location.city")
                                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) ).ok()
                            .addProperty("location.state", "OState")
                            .addProperty("location.zip")
                                .value( row -> getZipCode( row.getAs( "OZip" ) ) ).ok()
                        .endEntity()
                        .addEntity("Vehicle")
                            .to("Vehicle")
                            .useCurrentSync()
                            .addProperty("general.vehicleID", "ID")
                            .addProperty("vehicle.make", "MAKE")
                            .addProperty("vehicle.model", "MODEL")
                            .addProperty("vehicle.style", "Style")
                            .addProperty("vehicle.year")
                                .value( row -> getStrYear( row.getAs( "VehYear" ) ) ).ok()
                            .addProperty("vehicle.color", "Color")
                            .addProperty("vehicle.vin", "VIN")
                            .addProperty("vehicle.licensePlateNumber", "LIC")
                            .addProperty("vehicle.licensePlateState", "LIS")
                            .addProperty("vehicle.licensePlateType", "LIT")
                            .addProperty("vehicle.licensePlateYear")
                                .value( row -> getStrYear( row.getAs( "LIY" ) ) ).ok()
                        .endEntity()
                        .addEntity("CallForService")
                            .to("CallForService")
                            .useCurrentSync()
                            .addProperty("general.cfsID", "Dis_ID")
                            .addProperty("dispatch.transferVehicle", "TransferVehicle")
                        .endEntity()
                        .addEntity("DispatchType")
                            .to("DispatchType")
                            .useCurrentSync()
                            .addProperty("general.dispatchID", "Dis_ID")
                            .addProperty("dispatch.personType", "Type")
                        .endEntity()
                    .endEntities()
                    .createAssociations()
                        //.addAssociation("Has1")
                        //    .ofType("general.Has").to("Has")
                        //    .useCurrentSync()
                        //    .fromEntity("Party")
                        //    .toEntity("ContactInformation")
                        //    .addProperty( "general.stringid" , "ID")
                        //.endAssociation()
                        .addAssociation("LocatedAt1")
                            .ofType("general.LocatedAt").to("LocatedAt")
                            .useCurrentSync()
                            .fromEntity("Party")
                            .toEntity("Address")
                            .addProperty( "general.stringid" , "ID")
                        .endAssociation()
                        .addAssociation("Initiated")
                            .ofType("general.Initiated").to("Initiated")
                            .useCurrentSync()
                            .fromEntity("Party")
                            .toEntity("CallForService")
                            .addProperty("general.stringid", "ID")
                        .endAssociation()
                        .addAssociation("InvolvedIn")
                            .ofType("general.InvolvedIn").to("InvolvedIn")
                            .useCurrentSync()
                            .fromEntity("Vehicle")
                            .toEntity("CallForService")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on


        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Dataset<Row>> flights = new HashMap<>();
        flights.put( unitMapping, unit );
        flights.put( dispatchMapping, dispatch );
        flights.put( disTypeMapping, disType );
        flights.put( personMapping, person );


        shuttle.launch( flights );

    }

    public static Integer getHeightInch( Object obj ) {
        String height = Parsers.getAsString( obj );
        if ( obj != null ) {
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
        String employeeId = Parsers.getAsString( obj );
        if ( obj != null ) {
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                return employeeId.substring( 2 ).trim();
            }
            return employeeId.trim();
        }
        return null;
    }

    public static Boolean getActive( Object obj ) {
        String active = Parsers.getAsString( obj );
        if ( obj != null ) {
            if ( active.toLowerCase().startsWith( "x_" ) ) {
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }
        return null;
    }

    public static String getDayOfWeek( Object obj ) {
        List<String> days = Arrays.asList( "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY" );
        String dateStr = Parsers.getAsString( obj );
        if ( obj != null ) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            Date date;
            try {
                date = dateFormat.parse( dateStr );
                return days.get( date.getDay() );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return dateStr;
        }
        return null;
    }

    public static Integer getIntFromDouble( Object obj ) {
        String s = Parsers.getAsString( obj );
        if ( obj != null ) {
            double d = Double.parseDouble(s);
            return (int) d;
        }
        return null;
    }

    public static String getStringFromDouble( Object obj ) {
        String s = Parsers.getAsString( obj );
        if ( obj != null ) {
            int d = getIntFromDouble( s );
            return Integer.toString(d);
        }
        return null;
    }

    public static String getZipCode( Object obj ) {
        String str = Parsers.getAsString( obj );
        if ( obj != null ) {
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
        String str = Parsers.getAsString( obj );
        if ( obj != null ) {
            str = str.replaceAll( "[()-]", "" );
            str = str.substring( 0, 10 );
            return str;
        }
        return null;
    }

    public static String getStrYear( Object obj ) {
        String str = Parsers.getAsString( obj );
        if ( obj != null ) {
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

    public static String getStreet( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( obj != null ) {
            if ( !( address.contains( "/" ) ) ) {
                return addSpaceAfterCommaUpperCase( address );
            }
            return "";
        }
        return null;
    }

    public static String getAddressID( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( obj != null ) {
            if ( address.contains( "null" ) ) {
                address = address.replace( "null", "" );
                return String.join( "" , Arrays.asList( address.split( " " ) ) );
            }
            return String.join( "" , Arrays.asList( address.split( " " ) ) );
        }
        return null;
    }

    public static String getIntersection( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( obj != null ) {
            if ( address.contains( "/" ) ) {
                return address.replace( "/", " & " );
            }
            return "";
        }
        return null;
    }
}
