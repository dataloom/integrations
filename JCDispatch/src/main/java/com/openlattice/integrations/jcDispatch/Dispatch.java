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
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.config.IntegrationConfig;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import com.openlattice.shuttle.payload.FilterablePayload;
import com.openlattice.shuttle.payload.JdbcPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import com.zaxxer.hikari.HikariDataSource;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.openlattice.shuttle.dates.TimeZones;
import retrofit2.Retrofit;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.openlattice.integrations.jcDispatch.lib.NameParsing.*;

public class Dispatch {

    private static final Logger logger = LoggerFactory
            .getLogger( Dispatch.class );

    private static final Environment environment = Environment.STAGING;

    private static final DateTimeHelper dateHelper0 = new DateTimeHelper( TimeZones.America_Chicago,
            "YYYY-MM-DD HH:mm:ss", "YYYY-MM-DD HH:mm:ss.S" );

    public static void main( String[] args ) throws InterruptedException, IOException {

//        final String personPath = args[ 0 ];
//        final String sysuserbasePath = args[ 1 ];
//        final String dispatchPath = args[ 2 ];
//        final String disTypePath = args[ 3 ];
        final String jwtToken = args[ 0 ];

        //final SparkSession sparkSession = MissionControl.getSparkSession();
        //logger.info( "Using the following idToken: Bearer {}", jwtToken );

        //        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        //        EdmApi edmApi = retrofit.create( EdmApi.class );
        //        PermissionsApi permissionApi = retrofit.create( PermissionsApi.class );

        HikariDataSource hds=
        ObjectMappers.getYamlMapper()
                .readValue( new File( "/Users/kimengie/integration.yaml" ), IntegrationConfig.class )
        .getHikariDatasource( "jciowa" );

        // includes vehicle info, need date from dispatch table for association.
        Payload personPayload = new JdbcPayload( hds,
                "SELECT 'dispatch.CFS_DateTimeJanet', 'dispatch_person.ID', 'dispatch_person.OName', 'dispatch_person.DOB', 'dispatch_person.SSN', 'dispatch_person.OSQ', "
                        + "'dispatch_person.OAddress', 'dispatch_person.OCity', 'dispatch_person.OState', 'dispatch_person.OZip', 'dispatch_person.OPhone', 'dispatch_person.OSex', "
                        + "'dispatch_person.Type', 'dispatch_person.MAKE', 'dispatch_person.MODEL', 'dispatch_person.LIC', 'dispatch_person.LIS', 'dispatch_person.OAddress_Apt', "
                        + "'dispatch_person.DL_No', 'dispatch_person.DL_State', 'dispatch_person.NCIC', 'dispatch_person.ORace', 'dispatch_person.VIN', 'dispatch_person.CellPhone', "
                        + "'dispatch_person.VehYear', 'dispatch_person.CallForServicePersonId', 'dispatch_person.BadgeNumber', 'dispatch_person.Style', 'dispatch_person.OfficerID', "
                        + "'dispatch_person.Height', 'dispatch_person.Weight', 'dispatch_person.Eyes', 'dispatch_person.Hair', 'dispatch_person.Age', 'dispatch_person.TransferVehicle', "
                        + "'dispatch_person.Ethnicity', 'dispatch_person.Juv', 'dispatch_person.LIT', 'dispatch_person.LIY' "
                        + "FROM dispatch INNER JOIN dispatch_person ON 'dispatch.Dis_ID' = 'dispatch_person.Dis_ID'");

        //Payload sysuserbasePayload = new JdbcPayload( hds, "select * from dispatch_person" ) ; //TABLE NOT INCLUDED IN TEST RUN
        Payload dispatchPayload = new JdbcPayload( hds, "select * from dispatch limit 100000" ); //has correct dates
        Payload disTypePayload = new JdbcPayload( hds, "select * from dispatch_type limit 100000" ) ;

        //Payload dispatchPayload = new FilterablePayload( dispatchPath );
//        Map<String, String> caseIdToTime = dispatchPayload.getPayload()
//                .collect( Collectors.toMap( row -> row.get( "Dis_ID" ), row -> ( dateHelper0.parse( row.get( "CFS_DateTimeJanet" ) )) ) );

        //        RequiredEdmElements requiredEdmElements = ConfigurationService.StaticLoader
        //                .loadConfiguration( RequiredEdmElements.class );
        //
        //        if ( requiredEdmElements != null ) {
        //            RequiredEdmElementsManager manager = new RequiredEdmElementsManager( edmApi, permissionApi );
        //            manager.ensureEdmElementsExist( requiredEdmElements );
        //        }
        //
        //        logger.info( "ER Field names: {}", Arrays.asList( person.schema().fieldNames() ) );
        //        logger.info( "ER Field names: {}", Arrays.asList( unit.schema().fieldNames() ) );
        //        logger.info( "ER Field names: {}", Arrays.asList( dispatch.schema().fieldNames() ) );
        //        logger.info( "ER Field names: {}", Arrays.asList( disType.schema().fieldNames() ) );

        // @formatter:off
//        Flight sysuserbaseMapping = Flight     //entities = personnel, person. associations = person is personnel
//                .newFlight()
//                    .createEntities()
//                        .addEntity("Personnelsysuserbase")
//                            .to("JohnsonCoJusticeInvolvedPersonnel")
//                            .useCurrentSync()
//                            .addProperty("personnel.id", "officerid")
//                            .addProperty("personnel.title", "Title")
//                            .addProperty("criminaljustice.agencyid", "ori")
//                            .addProperty("personnel.status")
//                               .value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
//                            .addProperty("criminaljustice.employeeid")
//                               .value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
//                        .endEntity()
//                        .addEntity( "Peoplesysuserbase" )
//                        .to("JohnsonCoPeople")
//                        .useCurrentSync()
//                        .addProperty("nc.PersonGivenName")
//                            .value( row -> getFirstName( row.getAs( "FirstName" ) ) ).ok()       //REVISIT
//                            .addProperty("nc.PersonSurName")
//                            .value( row -> getLastName( row.getAs( "LastName" ) ) ).ok()
//                        .endEntity()
//                    .endEntities()
//                .createAssociations()
//                    .addAssociation( "WorksAsCJemployee" )
//                    .useCurrentSync()
//                    .to("JohnsonCoWorksAsCJemployee")
//                    .fromEntity( "Peoplesysuserbase" )
//                    .toEntity( "Personnelsysuserbase" )
//                    .addProperty( "general.stringid", "officerid" )
//                    .endAssociation()
//                .endAssociations()
//                .done();
        // @formatter:on

        // @formatter:off
        Flight dispatchMapping = Flight         //entities = dispatch, JI personnel (operators), address, dispatch zone, CFS origin
                .newFlight()
                    .createEntities()
                        .addEntity("CallForService")
                            .to("JohnsonCoCallForService")
                            .useCurrentSync()
                            .addProperty("criminaljustice.cfsid", "CallForServiceID")
                            .addProperty( "dispatch.number", "Dis_No" )
                            .addProperty( "criminaljustice.dispatchid", "Dis_ID" )
                            .addProperty("callforservice.casenumber", "Case_Number")
                            .addProperty( "callforservice.caseid", "Case_ID" )
                            .addProperty("dispatch.howreported", "HowReported")
                            .addProperty("datetime.received")
                                .value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                            .addProperty("datetime.alerted")
                                .value( row -> dateHelper0.parse( row.getAs( "AlertedTime" ) ) ).ok()
                            .addProperty( "date.dayofweek" )
                                .value( row -> getDayOfWeek( ( dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ) ) )
                                .ok()
                            .addProperty( "cad.masterbusinessnumber", "MBI_No" )
                            .addProperty("dispatch.911callnumber", "CallNumber_911")
                            .addProperty( "criminaljustice.agencyid", "Dis_ORI" )
                            .addProperty("criminaljustice.disposition", "ClearedBy")
                            .addProperty("criminaljustice.disposition2", "ClearedBy2")
                            .addProperty( "criminaljustice.assignedofficer", "ASSIGNED_OFFICER" )
                            .addProperty( "criminaljustice.assignedofficerid", "AssignedOfficerID" )
                            .addProperty( "dispatch.priority", "Priority" )
                            .addProperty("dispatch.typeclass", "TYPE_CLASS" )
                            .addProperty( "dispatch.type", "TYPE_ID" )
                            .addProperty("dispatch.emd", "PROQA")
                            .addProperty("dispatch.emdlevel", "PROQA_LEVEL")
                            .addProperty("dispatch.emdtype", "PROQA_TYPE")
                            .addProperty( "criminaljustice.ncic", "NCIC_Code" )
                            .addProperty("dispatch.fireflag", "CFS_Fire")
                            .addProperty("dispatch.emsflag", "CFS_EMS")
                            .addProperty("dispatch.lea", "CFS_LEA")
                            .addProperty("dispatch.firelevel", "FireDispatchLevel")
                        .endEntity()
                        .addEntity("DispatchZone")
                            .to("JohnsonDispatchZone")
                            .addProperty("dispatch.zoneid", "ZONE_ID")
                            .addProperty("dispatch.zonename", "Dis_Zone")
                            .addProperty("dispatch.subzone", "SubZone")
                            .addProperty("dispatch.medicalzone", "Medical_Zone")
                            .addProperty("dispatch.firedistrictname", "FireDistrict")
                            .addProperty("dispatch.firedistrictcode", "ESN")
                        .endEntity()
                         .addEntity( "Officers" )
                            .to("JohnsonCoPeople")
                            .addProperty( "nc.SubjectIdentification" )
                               .value( row -> UUID.randomUUID().toString() ).ok()
                            .addProperty("nc.PersonGivenName")
                                .value( row -> getFirstName( row.getAs( "ASSIGNED_OFFICER" ) ) ).ok()       //CHECK FOR MIDDLE NAMES
                            .addProperty("nc.PersonSurName")
                                .value( row -> getLastName( row.getAs( "ASSIGNED_OFFICER" ) ) ).ok()
                        .endEntity()
                        .addEntity("Operator")
                            .to("JohnsonCoPeople")
                            .useCurrentSync()
                            .addProperty( "nc.SubjectIdentification" )
                               .value( row -> UUID.randomUUID().toString() ).ok()
                            .addProperty("nc.PersonGivenName")
                                .value( row -> getFirstName( row.getAs( "Operator" ) ) ).ok()
                            .addProperty("nc.PersonSurName")
                                .value( row -> getLastName( row.getAs( "Operator" ) ) ).ok()
                            .addProperty( "nc.PersonMiddleName" )
                                .value( row -> getMiddleName( row.getAs( "Operator" ) ) ).ok()
                        .endEntity()

                        .addEntity("Personneldispatch")
                            .to("JohnsonCoJusticeInvolvedPersonnel")
                            .useCurrentSync()
                            .addProperty( "personnel.id" , "AssignedOfficerID")
                            .addProperty( "criminaljustice.officerbadgeid" ).value( row -> getBadgeNumber( row.getAs("ASSIGNED_OFFICER") ) ).ok()
                            .addProperty( "personnel.title" ).value(row -> "Officer" ).ok()
                //add operators - later?
                        .endEntity()
                        .addEntity("Origindispatch")
                            .to("JohnsonCoCFSOrigin")
                            .useCurrentSync()
                            .addProperty( "criminaljustice.cfsoriginid" , "Dis_ID")
                        .endEntity()
                        .addEntity( "Address" )
                            .to("JohnsonCoAddresses")
                            .useCurrentSync()
                            .addProperty("location.Address")
                                .value( row -> getAddressID( getStreet( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + ", "
                                        +  ( row.getAs( "LCity" ) ) + ", " + row.getAs( "LState" ) + " " +  row.getAs( "LZip" ) ) )
                                .ok()
                            .addProperty("location.street")
                                .value( row -> getStreet( row.getAs( "LAddress" ) ) ).ok()
                            .addProperty("location.intersection")
                                .value( row -> getIntersection( row.getAs( "LAddress" ) ) ).ok()
                            .addProperty("location.apartment", "LAddress_Apt")
                            .addProperty("location.city", "LCity")
                            .addProperty("location.state", "LState")
                            .addProperty("location.zip", "LZip" )
                            .addProperty( "location.name", "Location" )
                            .addProperty( "location.latitude", "Latitude" )
                            .addProperty( "location.longitude", "Longitude" )
                        .endEntity()
                        .addEntity( "contactinfo" )
                            .to("JohnsonCoCFSContactInfo")
                            .addProperty( "contact.id", "Dis_ID" )
                            .addProperty("contact.phonenumber")
                                .value( row -> getPhoneNumber( row.getAs( "LPhone" ) ) ).ok()
                        .endEntity()
                    .endEntities()

                //6 assns. CFS occurred at address, CFS contacted at contactinfo, origin initiated service, CFS call received by operator,
                // person works as JI-employee (Operator), CFS located at zone
                    .createAssociations()
                        .addAssociation("OccurredAtdispatch")
                            .ofType("justice.occurredat").to("JohnsonOccurredAt")
                           .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("Address")
                            .addProperty("general.stringid", "Dis_ID")
                            .addProperty( "location.address" )
                               .value( row -> getAddressID( getStreet( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + ", "
                                        +  ( row.getAs( "LCity" ) ) + ", " + row.getAs( "LState" ) + " " +  row.getAs( "LZip" ) ) )
                                .ok()
                        .endAssociation()
                        .addAssociation( "contactinfogiven" )
                            .to("JohnsonCFSContactInfoGiven")
                            .useCurrentSync()
                            .fromEntity( "contactinfo" )
                            .toEntity( "CallForService" )
                            .addProperty( "contact.id", "Dis_ID" )
                        .endAssociation()
                        .addAssociation("ZonedWithin")
                            .ofType("criminaljustice.zonedwithin").to("JohnsonZonedWithin")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("DispatchZone")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("Initiateddispatch")
                            .ofType("criminaljustice.initiated").to("JohnsonInitiated")
                            .useCurrentSync()
                            .fromEntity("Origindispatch")
                            .toEntity("CallForService")
                            .addProperty("datetime.received").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                        .endAssociation()
                        .addAssociation( "worksas" )
                            .ofType( "criminaljustice.worksas" ).to("JohnsonCoWorksAsCJemployee")
                            .useCurrentSync()
                            .fromEntity( "Officers" )
                            .toEntity( "Personneldispatch" )
                            .addProperty( "general.stringid", "AssignedOfficerID" )
                        .endAssociation()
                        .addAssociation( "callreceivedby" )
                            .to("JohnsonReceivedBy")
                            .useCurrentSync()
                            .fromEntity( "CallForService" )
                            .toEntity("Operator")
                            .addProperty( "datetime.received" ).value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                        .endAssociation()

                //Operators are filled in with job title

//                         .addAssociation("OccurredAt1")
//                            .ofType("general.occurredat").to("OccurredAt")
//                            .useCurrentSync()
//                            .fromEntity("CallForService")
//                            .toEntity("Place")
//                            .addProperty("general.stringid", "Dis_ID")
//                        .endAssociation()
//                        .addAssociation("Has")
//                            .ofType("general.Has").to("Has")
//                            .useCurrentSync()
//                            .fromEntity("Place")
//                            .toEntity("Address")
//                            .addProperty("general.stringid", "Dis_ID")
//                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        // @formatter:off
        Flight disTypeMapping = Flight     //entities = CFS, personnel, person. associations = appear in (personnel in CFS), person works as JI personnel
                .newFlight()
                    .createEntities()
                        .addEntity("CallForServiceDistype")
                            .to("JohnsonCoCallForService")
                            .useCurrentSync()
                            .addProperty( "criminaljustice.cfsid" , "Dis_ID")
                            .addProperty("datetime.enroute")
                                .value( row -> dateHelper0.parseTime( row.getAs( "TimeEnroute" ) ) ).ok()
                            .addProperty("date.completeddatetime")
                                .value( row -> dateHelper0.parseTime( row.getAs( "TimeComp" ) ) ).ok()
                            .addProperty("dispatch.typeid", "Dispatch_Type_ID")
                            .addProperty("dispatch.type", "Type_ID")
                            .addProperty("dispatch.typepriority", "Type_Priority")
                            .addProperty("dispatch.tripnumber", "TripNumber")
                            .addProperty( "callforservice.casenumber", "Case_Num" )
                            .addProperty( "callforservice.caseid", "Case_ID" )
                            .addProperty( "criminaljustice.disposition", "Disposition" )
                        .endEntity()
                        .addEntity( "PersonnelDistype" )
                            .to( "JohnsonCoJusticeInvolvedPersonnel" )
                            .useCurrentSync()
                            .addProperty( "criminaljustice.officerbadgeid")
                               .value( row -> getBadgeNumber( row.getAs("Unit") ) ).ok()
                            .addProperty( "personnel.id", "OfficerID" )
                        .endEntity()
                        .addEntity( "PeopleDistype" )
                        .to("JohnsonCoPeople")
                        .useCurrentSync()
                        .addProperty("nc.PersonGivenName")
                            .value( row -> getFirstName( row.getAs( "Unit" ) ) ).ok()
                        .addProperty("nc.PersonSurName")
                            .value( row -> getLastName( row.getAs( "Unit" ) ) ).ok()
                        .addProperty( "nc.PersonMiddleName" )
                             .value( row -> getMiddleName( row.getAs( "Unit" ) ) ).ok()
                        .endEntity()
                    .endEntities()

                    .createAssociations()    // associations = appear in (personnel in CFS), person works as JI personnel
                        .addAssociation("WorksasDistype")
                            .ofType("criminaljustice.worksas").to("JohnsonCoWorksAsCJemployee")
                            .useCurrentSync()
                            .fromEntity("PeopleDistype")
                            .toEntity("PersonnelDistype")
                            .addProperty( "general.stringid" , "OfficerID")
                        .endAssociation()
                        .addAssociation( "AppearsinDistype" )
                            .ofType( "general.appearsin" ).to("JohnsonCFSAppearsIn")
                            .useCurrentSync()
                            .fromEntity( "PersonnelDistype" )
                            .toEntity( "CallForServiceDistype" )
                            .addProperty( "general.stringid", "OfficerID" )
                            .addProperty( "nc.SubjectIdentification", "CallForServiceOfficerId" )
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        // @formatter:off
        Flight personMapping = Flight        //entitites = CFS origin, person, originating address, contact info, vehicle.  Associations = originating at, involved in (vehicle info), appears in (personnel in CFS)
                .newFlight()
                    .createEntities()
                       .addEntity( "Personperson" )
                             .to( "JohnsonCoPeople" )
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
                            .addProperty( "nc.SSN", "SSN" )
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
                       .addEntity( "Originperson" )
                            .to( "JohnsonCoCFSOrigin" )
                            .useCurrentSync()
                            .addProperty( "criminaljustice.cfsoriginid", "Dis_ID" )
                            .addProperty( "event.comments", "OSQ" )
                            .addProperty( "dispatch.persontype", "Type" )
                            .addProperty( "criminaljustice.officerbadgeid", "BadgeNumber" )
                            .addProperty( "person.ageatevent", "Age" )
                            .addProperty( "person.juvenile", "Juv" )
                       .endEntity()
                       .addEntity( "Addressperson" )
                            .to("JohnsonCoAddresses")
                            .useCurrentSync()
                            .addProperty("location.Address")
                                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + " " + row.getAs( "OAddress_Apt" )
                                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" ) + " "
                                        + row.getAs( "OZip" ) ) )
                                .ok()
                            .addProperty("location.street")
                                .value( row -> getStreet( row.getAs( "OAddress" ) ) ).ok()
                            .addProperty("location.intersection")
                                .value( row -> getIntersection( row.getAs( "OAddress" ) ) ).ok()
                            .addProperty("location.apartment", "OAddress_Apt")
                            .addProperty("location.city")
                                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) ).ok()
                            .addProperty("location.state", "OState")
                            .addProperty("location.zip", "OZip")
                        .endEntity()
                        .addEntity("ContactPerson1")
                            .to("JohnsonCoCFSContactInfo")
                            .useCurrentSync()
                            .addProperty( "contact.id", "ID" )
                            .addProperty( "contact.phonenumber", "OPhone" )
                        .endEntity()
                        .addEntity("ContactPerson2")
                            .to("JohnsonCoCFSContactInfo")
                            .useCurrentSync()
                            .addProperty( "contact.id", "ID" )
                            .addProperty( "contact.phonenumber", "CellPhone" )
                            .endEntity()
                        .addEntity("CallForServiceperson")
                            .to("JohnsonCoCallForService")
                            .useCurrentSync()
                            .addProperty("criminaljustice.cfsid", "Dis_ID")
                        .endEntity()
                        .addEntity("Vehicle")
                            .to("JohnsonCoVehicle")
                            .useCurrentSync()
                            .addProperty("vehicle.id")
                                .value( row ->  {
                        return Parsers.getAsString(row.getAs( "MAKE" )) + Parsers.getAsString( row.getAs( "MODEL" ))
                                        + Parsers.getAsString(row.getAs( "LIC" ))  + Parsers.getAsString(row.getAs( "LIS" ) );
                                    }).ok()
                            .addProperty("vehicle.make", "MAKE")
                            .addProperty("vehicle.model", "MODEL")
                            .addProperty("vehicle.licensenumber", "LIC")
                            .addProperty("vehicle.licensestate", "LIS")
                            .addProperty("vehicle.vin", "VIN")
                            .addProperty("vehicle.year")
                                .value( row -> getStrYear( row.getAs( "VehYear" ) ) ).ok()
                            .addProperty("vehicle.color", "Color")
                            .addProperty( "vehicle.secondarycolor", "ColorSecondary" )
                            .addProperty("vehicle.style", "Style")
                            .addProperty("vehicle.licenseplatetype", "LIT")
                            .addProperty("vehicle.licenseyear")
                                .value( row -> getStrYear( row.getAs( "LIY" ) ) ).ok()
                            .addProperty("dispatch.transfervehicle", "TransferVehicle")
                        .endEntity()
                    .endEntities()

                    .createAssociations()
                        .addAssociation("InvolvedIn")
                            .ofType("criminaljustice.involvedin")
                            .to("JohnsonCoCFSInvolvedIn")
                            .useCurrentSync()
                            .fromEntity("Vehicle")
                            .toEntity("CallForServiceperson")
                            .addProperty("datetime.received").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                                //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                        .endAssociation()
                        .addAssociation("Initiatedperson")
                            .ofType("criminaljustice.initiated").to("JohnsonInitiated")
                            .useCurrentSync()
                            .fromEntity("Originperson")
                            .toEntity("CallForServiceperson")
                            .addProperty("datetime.received").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                                //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                        .endAssociation()

                .addAssociation("contactedatPerson1")
                            .ofType("geo.contactedat").to("JohnsonCFSContactedAt")
                            .useCurrentSync()
                            .fromEntity("Originperson")
                            .toEntity("ContactPerson1")
                            .addProperty( "date.completeddatetime").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                                //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                        .endAssociation()
                        .addAssociation("contactedatPerson2")
                            .ofType("geo.contactedat").to("JohnsonCFSContactedAt")
                            .useCurrentSync()
                            .fromEntity("Originperson")
                            .toEntity("ContactPerson2")
                            .addProperty( "date.completeddatetime").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                                //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                            .addProperty( "contact.cellphone" ).value(Dispatch::isCellphone).ok()
                        .endAssociation()
                        .addAssociation("OccurredAtperson")
                            .ofType("justice.occurredat").to("JohnsonOccurredAt")
                            .useCurrentSync()
                            .fromEntity("Originperson")
                            .toEntity("Addressperson")
                            .addProperty( "general.stringid" , "ID")
                            .addProperty( "location.address" ).value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + "," + row.getAs( "OAddress_Apt" )
                                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                                        + " " + row.getAs( "OZip" ) ) )
                                .ok()
                        .endAssociation()
                        .addAssociation( "becomes" )
                           .ofType( "person.becomes" ).to("JohnsonCoBecomes")
                           .useCurrentSync()
                           .fromEntity( "Personperson" )
                           .toEntity( "Originperson" )
                           .addProperty( "nc.SubjectIdentification" , "ID")
                           .addProperty( "nc.SSN", "SSN" )
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>();
        //flights.put( sysuserbaseMapping, sysuserbasePayload );
        flights.put( dispatchMapping, dispatchPayload );
        flights.put( disTypeMapping, disTypePayload );
        flights.put( personMapping, personPayload );

        shuttle.launchPayloadFlight( flights );

    }

    public static Boolean isCellphone( Row row ) {
       String cellrow = row.getAs ( "CellPhone");
       if ( cellrow == null) {
           return false;
       }
       return true;
    }


    public static String getBadgeNumber( Object obj ) {
        String badgerow = Parsers.getAsString( obj );
        if ( badgerow != null && badgerow.length() > 0 ) {
            if ( Character.isDigit( badgerow.charAt( 0 ) )) {
                String[] strBadge = badgerow.split( " " );
                return strBadge[ 0 ].trim();
            }
        }
        return null;
    }

    public static Integer getHeightInch( Object obj ) {
        String height = Parsers.getAsString( obj );
        if ( height != null ) {
            if ( height.length() > 2 ) {
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
        if ( employeeId != null ) {
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                return employeeId.substring( 2 ).trim();
            }
            return employeeId.trim();
        }
        return null;
    }

    public static String getActive( Object obj ) {
        String active = Parsers.getAsString( obj );
        if ( active != null ) {
            if ( active.toLowerCase().startsWith( "x_" ) ) {
                return "inactive";
            }
            return "active";
        }
        return null;
    }

    public static String getDayOfWeek( Object obj ) {
        List<String> days = Arrays
                .asList( "SUNDAY", "MONDAY", "TUESDAY", "WEDNESDAY", "THURSDAY", "FRIDAY", "SATURDAY" );
        String dateStr = Parsers.getAsString( obj );
        if ( dateStr != null ) {
            SimpleDateFormat dateFormat = new SimpleDateFormat( "yyyy-MM-dd" );
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
        if ( s != null ) {
            double d = Double.parseDouble( s );
            return (int) d;
        }
        return null;
    }

    public static String getStringFromDouble( Object obj ) {
        String s = Parsers.getAsString( obj );
        if ( s != null ) {
            int d = getIntFromDouble( s );
            return Integer.toString( d );
        }
        return null;
    }

    //    public static String getZipCode( Object obj ) {
    //        String str = Parsers.getAsString( obj );
    //        if ( str != null ) {
    //            String[] strDate = str.split( " " );
    //            if ( strDate.length > 1 ) {
    //                return getStringFromDouble( strDate[ strDate.length - 1 ]).trim();
    //            }
    //            String[] lZip = str.split( "-" );
    //            if ( lZip.length > 1 ) {
    //                return str;
    //            }
    //            return getStringFromDouble( strDate[ 0 ]).trim();
    //        }
    //        return null;
    //    }

    public static String getPhoneNumber( Object obj ) {
        String str = Parsers.getAsString( obj );
        if ( str != null ) {
            str = str.replaceAll( "[()\\- ]", "" );
            //str = str.substring( 0, 10 );
            return str;
        }
        return null;
    }

    public static String getStrYear( Object obj ) {
        String str = Parsers.getAsString( obj );
        if ( str != null ) {
            String[] strDate = str.split( "/" );
            if ( strDate.length > 1 ) {
                return getStringFromDouble( strDate[ strDate.length - 1 ] ).trim();
            }
            if ( str.contains( "DOB" ) ) {
                return "";
            }
            return getStringFromDouble( strDate[ 0 ] ).trim();
        }
        return null;
    }

    public static String getStreet( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( address != null ) {
            if ( !( address.contains( "/" ) ) ) {
                return addSpaceAfterCommaUpperCase( address );
            }
            return "";
        }
        return null;
    }

    public static String getAddressID( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( address != null ) {
            if ( address.contains( "null" ) ) {
                address = address.replace( "null", "" );
                return String.join( "", Arrays.asList( address.split( " " ) ) );
            }
            return String.join( "", Arrays.asList( address.split( " " ) ) );
        }
        return null;
    }

    public static String getIntersection( Object obj ) {
        String address = Parsers.getAsString( obj );
        if ( address != null ) {
            if ( address.contains( "/" ) ) {
                return address.replace( "/", " & " );
            }
            return "";
        }
        return null;
    }
}
