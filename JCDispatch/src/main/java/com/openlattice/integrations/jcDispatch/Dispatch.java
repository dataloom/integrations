package com.openlattice.integrations.jcDispatch;

import com.dataloom.mappers.ObjectMappers;
import com.openlattice.client.RetrofitFactory.Environment;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.adapter.Row;
import com.openlattice.shuttle.config.IntegrationConfig;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.JdbcPayload;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

import static com.openlattice.integrations.jcDispatch.lib.NameParsing.addSpaceAfterCommaUpperCase;
import static com.openlattice.integrations.jcDispatch.lib.NameParsing.getFirstName;
import static com.openlattice.integrations.jcDispatch.lib.NameParsing.getLastName;
import static com.openlattice.integrations.jcDispatch.lib.NameParsing.getMiddleName;
import static com.openlattice.integrations.jcDispatch.lib.NameParsing.getName;
import static com.openlattice.integrations.jcDispatch.lib.NameParsing.p;
import static com.openlattice.shuttle.util.Parsers.getAsString;

public class Dispatch {

    private static final Logger logger = LoggerFactory
            .getLogger( Dispatch.class );

    private static final Environment environment = Environment.PRODUCTION;

    private static final DateTimeHelper dateHelper0 = new DateTimeHelper( TimeZones.America_Chicago,
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.S" );

    public static void main( String[] args ) throws InterruptedException, IOException {

        final String username = args[ 1 ];
        final String password = args[ 2 ];
        final String jwtToken = MissionControl.getIdToken( username, password );      //for running on Athena
        final String integrationFile = args[ 3 ];                                     //for running on Athena
//                final String jwtToken = args[ 0 ];              //for local testing
//                final String integrationFile = args[ 1 ];       //for local testing

        HikariDataSource hds =
                ObjectMappers.getYamlMapper()
                        .readValue( new File( integrationFile ), IntegrationConfig.class )
                        .getHikariDatasource( "jciowa" );

        Payload personPayload = new JdbcPayload( hds,
                "select * from dispatch_person_15m" );        // includes vehicle info
        Payload sysuserbasePayload = new JdbcPayload( hds,
                "select * from systemuserbase_table" ); //TABLE NOT INCLUDED IN TEST RUN
        Payload dispatchPayload = new JdbcPayload( hds, "select * from dispatch_15m" );
        Payload distypePayload = new JdbcPayload( hds, "select * from dispatch_type_15m" );

        List<Map<String, String>> fp = distypePayload.getPayload().collect( Collectors.toList() );
        Payload unitPayload = new SimplePayload( fp.stream().filter( row -> containsUnit( row ) ) );
        Payload nonUnitPayload = new SimplePayload( fp.stream().filter( row -> !containsUnit( row ) ) );

        List<Map<String, String>> fp2 = personPayload.getPayload().collect( Collectors.toList() );
        Payload vehiclePeepsPayload = new SimplePayload( fp2.stream()
                .filter( row -> containsVehicle( row ) && containsPerson( row ) ) );
        Payload vehicleNoPeepsPayload = new SimplePayload( fp2.stream()
                .filter( row -> containsVehicle( row ) && !containsPerson( row ) ) );
        Payload noVehiclePeepsPayload = new SimplePayload( fp2.stream()
                .filter( row -> !containsVehicle( row ) && containsPerson( row ) ) );
        Payload noVehicleNoPeepsPayload = new SimplePayload( fp2.stream()
                .filter( row -> !containsVehicle( row ) && !containsPerson( row ) ) );

        //Payload dispatchPayload = new FilterablePayload( dispatchPath );
        //        Map<String, String> caseIdToTime = dispatchPayload.getPayload()
        //                .collect( Collectors.toMap( row -> row.get( "Dis_ID" ), row -> ( dateHelper0.parse( row.get( "CFS_DateTimeJanet" ) )) ) );

        // @formatter:off
        Flight sysuserbaseMapping = Flight     //entities = personnel, person. associations = person is personnel
                .newFlight()
                    .createEntities()
                        .addEntity("Personnelsysuserbase")
                            .to("JohnsonCoJusticeInvolvedPersonnel")
                            .useCurrentSync()
                            .addProperty("publicsafety.personnelid", "OfficerId")
                            .addProperty("publicsafety.personneltitle", "Title")
                            .addProperty("publicsafety.agencyid", "ORI")
                            .addProperty("publicsafety.personnelstatus")
                               .value( row -> getActive( row.getAs( "EmployeeId" ) ) ).ok()
                            .addProperty("publicsafety.employeeid")
                               .value( row -> getEmployeeId( row.getAs( "EmployeeId" ) ) ).ok()
                        .endEntity()
                        .addEntity( "Peoplesysuserbase" )
                        .to("JohnsonCoPeople")
                        .useCurrentSync()
                        .addProperty("nc.PersonGivenName")
                            .value( row -> getFirstName( row.getAs( "FirstName" ) ) ).ok()
                            .addProperty("nc.PersonSurName")
                            .value( row -> getLastName( row.getAs( "LastName" ) ) ).ok()
                            .addProperty( "nc.SubjectIdentification", "SystemUserId" )
                        .endEntity()
                    .endEntities()
                .createAssociations()
                    .addAssociation( "WorksAsCJemployee" )
                    .useCurrentSync()
                    .to("JohnsonCoWorksAsCJemployee")
                    .fromEntity( "Peoplesysuserbase" )
                    .toEntity( "Personnelsysuserbase" )
                    .addProperty( "general.stringid", "OfficerId" )
                    .endAssociation()
                .endAssociations()
                .done();
        // @formatter:on

        // @formatter:off
        Flight dispatchMapping = Flight         //entities = dispatch, JI personnel (operators), dispatch zone, CFS origin
                .newFlight()
                    .createEntities()
                        .addEntity("CallForService")
                            .to("JohnsonCoCallForService")
                            .useCurrentSync()
                            .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                            .addProperty("publicsafety.cfsid", "CallForServiceID")
                            .addProperty( "publicsafety.dispatchnumber", "Dis_No" )
                            .addProperty("publicsafety.cfscasenumber", "Case_Number")
                            .addProperty( "publicsafety.cfscaseid", "Case_ID" )
                            .addProperty("publicsafety.dispatchhowreported", "HowReported")
                            .addProperty("datetime.received")
                                .value( row -> dateHelper0.parseDate( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                            .addProperty( "date.dayofweek" )
                                .value( row -> getDayOfWeek( ( dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ) ) )
                                .ok()
                            .addProperty( "publicsafety.cad_masterbusinessnumber", "MBI_No" )
                            .addProperty("publicsafety.dispatch911callnumber", "CallNumber_911")
                            .addProperty( "publicsafety.agencyid", "Dis_ORI" )
                            .addProperty("criminaljustice.disposition", "ClearedBy")
                            .addProperty("criminaljustice.disposition2", "ClearedBy2")
                            .addProperty( "publicsafety.assignedofficer", "ASSIGNED_OFFICER" )
                            .addProperty( "publicsafety.assignedofficerid", "AssignedOfficerID" )
                            .addProperty( "publicsafety.dispatchpriority", "Priority" )
                            .addProperty("publicsafety.dispatchtypeclass", "TYPE_CLASS" )
                            .addProperty( "publicsafety.dispatchtype", "TYPE_ID" )
                            .addProperty("publicsafety.dispatchemd", "PROQA")
                            .addProperty("publicsafety.dispatchemdlevel", "PROQA_LEVEL")
                            .addProperty("publicsafety.dispatchemdtype", "PROQA_TYPE")
                            .addProperty( "criminaljustice.ncic", "NCIC_Code" )
                            .addProperty("publicsafety.dispatchfireflag", "CFS_Fire")
                            .addProperty("publicsafety.dispatchemsflag", "CFS_EMS")
                            .addProperty("publicsafety.dispatchlea", "CFS_LEA")
                            .addProperty("publicsafety.dispatchfirelevel", "FireDispatchLevel")
                            //.addProperty( "event.comments" ).value(row -> "A" ).ok()
                        .endEntity()
                        .addEntity("DispatchZone")
                            .to("JohnsonDispatchZone")
                            .useCurrentSync()
                            //.addProperty("dispatch.zoneid").value( row -> Parsers.parseInt( row.getAs( "ZONE_ID" ) ) ).ok()
                            .addProperty( "publicsafety.dispatchzoneid", "ZONE_ID" )
                            .addProperty("publicsafety.dispatchzonename", "Dis_Zone")
                            .addProperty("publicsafety.dispatchsubzone", "SubZone")
                            .addProperty("publicsafety.medicalzone", "Medical_Zone")
                            .addProperty("publicsafety.firedistrictname", "FireDistrict")
                            .addProperty("publicsafety.firedistrictcode", "ESN")
                        .endEntity()
                         .addEntity( "Officers" )
                            .to("JohnsonCoPeople")
                            .useCurrentSync()
                            .addProperty( "nc.SubjectIdentification", "AssignedOfficerID" )
                               //.value( row -> UUID.randomUUID().toString() ).ok()
                            .addProperty("nc.PersonGivenName")
                                .value( row -> getFirstName( row.getAs( "ASSIGNED_OFFICER" ) ) ).ok()       //CHECK FOR MIDDLE NAMES
                            .addProperty("nc.PersonSurName")
                                .value( row -> getLastName( row.getAs( "ASSIGNED_OFFICER" ) ) ).ok()
                        .endEntity()
                        .addEntity("Operator")
                            .to("JohnsonCoPeople")
                            .useCurrentSync()
                            .addProperty( "nc.SubjectIdentification", "Operator" )
                             //  .value( row -> UUID.randomUUID().toString() ).ok()
                            .addProperty("nc.PersonGivenName")
                                .value( row -> getFirstName( row.getAs( "Operator" ) ) ).ok()
                            .addProperty("nc.PersonSurName")
                                .value( row -> getLastName( row.getAs( "Operator" ) ) ).ok()
                            .addProperty( "nc.PersonMiddleName" )
                                .value( row -> getMiddleName( row.getAs( "Operator" ) ) ).ok()
                        .endEntity()
                        .addEntity("Personneldispatch1")
                            .to("JohnsonCoJusticeInvolvedPersonnel")
                            .useCurrentSync()
                            .addProperty( "publicsafety.personnelid" , "AssignedOfficerID")
                            .addProperty( "publicsafety.officerbadgeid" ).value( row -> getBadgeNumber( row.getAs("ASSIGNED_OFFICER") ) ).ok()
                            .addProperty( "publicsafety.personneltitle" ).value(row -> "Officer" ).ok()
                        .endEntity()
                        .addEntity( "Personneldispatch2" )
                            .to("JohnsonCoJusticeInvolvedPersonnel")
                            .useCurrentSync()
                            .addProperty( "publicsafety.personnelid" , "Operator")      //no other identifying info for Operators in this table
                            .addProperty( "publicsafety.personneltitle" ).value(row -> "Operator" ).ok()
                        .endEntity()
//                        .addEntity("Origindispatch")
//                            .to("JohnsonCoCFSOrigin")
//                            .useCurrentSync()
//                            .addProperty( "criminaljustice.cfsoriginid" , "Dis_ID")
//                        .endEntity()
                        .addEntity( "Address" )
                            .to("JohnsonCoAddresses")
                            .useCurrentSync()
                            .addProperty("location.Address")
                                .value( row -> getAddressID( getStreet( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + ", "
                                        +  ( row.getAs( "LCity" ) ) + ", " + row.getAs( "LState" ) + " " +  row.getAs( "LZip" )
                                        + " " + getIntersection( row.getAs( "LAddress" ) )) )
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
                            .addProperty( "location.latitude").value( row -> Parsers.parseDouble(  row.getAs( "Latitude" )) ).ok()
                            .addProperty( "location.longitude").value( row -> Parsers.parseDouble(  row.getAs("Longitude" ) )).ok()
                        .endEntity()
                        .addEntity( "contactinfo" )
                            .to("JohnsonCoCFSContactInfo")
                            .useCurrentSync()
                            .addProperty( "contact.id", "Dis_ID" )
                            .addProperty("contact.phonenumber")
                                .value( row -> getPhoneNumber( row.getAs( "LPhone" ) ) ).ok()
                        .endEntity()
                    .endEntities()
//
//                //6 assns. CFS occurred at address, CFS contacted at contactinfo, CFS call received by operator,
//                // person works as JI-employee (Operator), CFS located at zone, JI-employee appears in CFS
                    .createAssociations()
                        .addAssociation("OccurredAtdispatch")
                            .ofType("general.occurredat").to("JohnsonOccurredAt")
                           .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("Address")
                            .addProperty("general.stringid", "Dis_ID")
                            .addProperty( "location.address" )
                               .value( row -> getAddressID( getStreet( row.getAs( "LAddress" ) ) + " " + row.getAs( "LAddress_Apt" ) + ", "
                                        +  ( row.getAs( "LCity" ) ) + ", " + row.getAs( "LState" ) + " " +  row.getAs( "LZip" )
                                        + " " + getIntersection( row.getAs( "LAddress" ) )) )
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
                            .ofType("geo.zonedwithin").to("JohnsonZonedWithin")
                            .useCurrentSync()
                            .fromEntity("CallForService")
                            .toEntity("DispatchZone")
                            .addProperty("general.stringid", "Dis_ID")
                        .endAssociation()
//                        .addAssociation("Initiateddispatch")
//                            .ofType("criminaljustice.initiated").to("JohnsonInitiated")
//                            .useCurrentSync()
//                            .fromEntity("Origindispatch")
//                            .toEntity("CallForService")
//                            .addProperty("datetime.received").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
//                        .endAssociation()
                        .addAssociation( "worksas1" )
                            .ofType( "economy.worksas" ).to("JohnsonCoWorksAsCJemployee")
                            .useCurrentSync()
                            .fromEntity( "Officers" )
                            .toEntity( "Personneldispatch1" )
                            .addProperty( "general.stringid", "AssignedOfficerID" )
                        .endAssociation()
                    .addAssociation( "worksas2" )
                            .ofType( "economy.worksas" ).to("JohnsonCoWorksAsCJemployee")
                            .useCurrentSync()
                            .fromEntity( "Operator" )
                            .toEntity( "Personneldispatch2" )
                            .addProperty( "general.stringid", "Operator" )
                        .endAssociation()
                        .addAssociation( "callreceivedby" )
                            .to("JohnsonReceivedBy")
                            .useCurrentSync()
                            .fromEntity( "CallForService" )
                            .toEntity("Operator")
                            .addProperty( "datetime.received" ).value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok()
                        .endAssociation()
                     .addAssociation( "AppearsinDispatch1" )
                            .ofType( "general.appearsin" ).to("JohnsonCFSAppearsIn")
                            .useCurrentSync()
                            .fromEntity( "Officers" )
                            .toEntity( "CallForService" )
                            .entityIdGenerator( row -> row.get("Dis_ID") + row.get("AssignedOfficerID")  )
                            .addProperty( "general.stringid", "Dis_ID" )
                            .addProperty( "nc.SubjectIdentification", "AssignedOfficerID" )
                        .endAssociation()
                        .addAssociation( "AppearsinDispatch2" )
                            .ofType( "general.appearsin" ).to("JohnsonCFSAppearsIn")
                            .useCurrentSync()
                            .fromEntity( "Operator" )
                            .toEntity( "CallForService" )
                            .entityIdGenerator( row -> row.get("Dis_ID") + row.get("Operator")  )
                            .addProperty( "general.stringid", "Dis_ID" )
                            .addProperty( "nc.SubjectIdentification", "Operator" )
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        // @formatter:off
        Flight nonUnitMapping = Flight     //entities = CFS, personnel, person. associations = appear in (personnel in CFS), person works as JI personnel
                .newFlight()
                    .createEntities()
                        .addEntity("CallForServiceDistype")
                            .to("JohnsonCoCallForService")
                            .useCurrentSync()
                            .addProperty( "publicsafety.dispatchid" , "Dis_ID")
                            .addProperty("datetime.alerted")
                                .value(  row -> dateHelper0.parseTime( row.getAs( "TimeDisp" ) ) ).ok()
                            .addProperty("datetime.enroute")
                                .value( row -> dateHelper0.parseTime( row.getAs( "TimeEnroute" ) ) ).ok()
                            .addProperty( "datetime.arrived" )
                                .value( row -> dateHelper0.parseTime( row.getAs( "TimeArr" ) ) ).ok()
                            .addProperty("date.completeddatetime")
                                .value( row -> dateHelper0.parseTime( row.getAs( "TimeComp" ) ) ).ok()
                            .addProperty("publicsafety.dispatchtypeid", "Dispatch_Type_ID")
                            .addProperty("publicsafety.dispatchtype", "Type_ID")
                            .addProperty("publicsafety.dispatchtypepriority").value( row -> Parsers.parseInt( row.getAs( "Type_Priority" ) ) ).ok()
                            .addProperty("publicsafety.dispatchtripnumber", "TripNumber")
                            .addProperty( "publicsafety.cfscasenumber", "Case_Num" )
                            .addProperty( "publicsafety.cfscaseid", "Case_ID" )
                            .addProperty( "criminaljustice.disposition", "Disposition" )
                            //.addProperty( "event.comments" ).value(row -> "B" ).ok()
                        .endEntity()
                        .addEntity( "PersonnelDistype" )
                            .to( "JohnsonCoJusticeInvolvedPersonnel" )
                            .useCurrentSync()
                            .addProperty( "publicsafety.officerbadgeid")
                               .value( row -> getBadgeNumber( row.getAs("Unit") ) ).ok()
                            .addProperty( "publicsafety.personnelid", "OfficerID" )     //in dispatch_type table, every person has an officerID.
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
                            .addProperty( "nc.SubjectIdentification", "OfficerID" )     //Works better than UUID, consistent. in dispatch_type table, every person has an officerID.
                               //  .value( row -> UUID.randomUUID().toString() ).ok()
                        .endEntity()

                    .endEntities()

                    .createAssociations()    // associations = appear in (personnel in CFS), person works as JI personnel
                        .addAssociation("WorksasDistype")
                            .ofType("economy.worksas").to("JohnsonCoWorksAsCJemployee")
                            .useCurrentSync()
                            .fromEntity("PeopleDistype")
                            .toEntity("PersonnelDistype")
                            .addProperty( "general.stringid" , "Unit")
                        .endAssociation()
                        .addAssociation( "AppearsinDistype1" )
                            .ofType( "general.appearsin" ).to("JohnsonCFSAppearsIn")
                            .useCurrentSync()
                            .fromEntity( "PeopleDistype" )
                            .toEntity( "CallForServiceDistype" )
                        //need to add name to the entity ID, or numerous people are getting conflated who appear in the same call, here and in the dispatch table.
                            .entityIdGenerator( row -> row.get("Dis_ID") + row.get("OfficerID")  + getLastName( row.get( "Unit" )) + ", " + getLastName( row.get( "Unit" )))
                            .addProperty( "general.stringid", "Dis_ID" )
                            .addProperty( "nc.SubjectIdentification", "OfficerID" )
                        .endAssociation()

                    .endAssociations()
                .done();
        // @formatter:on

        Flight unitMapping = Flight
                .newFlight()
                .createEntities()
                .addEntity( "Unit" )
                .to( "JohnsonCoUnit" )
                .addProperty( "publicsafety.unitid" )   //Dis+ID+time recvd+time comp
                .value( Dispatch::getUnitID ).ok()
                .addProperty( "publicsafety.unitname" ).value( row -> {
                    return Parsers.getAsString( row.getAs( "Unit" ));
                    }).ok()
                .endEntity()
                .addEntity( "cfsUnit" )
                .to( "JohnsonCoCallForService" )
                .useCurrentSync()
                .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .addProperty( "datetime.alerted" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeDisp" ) ) ).ok()
                .addProperty( "datetime.enroute" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeEnroute" ) ) ).ok()
                .addProperty( "datetime.arrived" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeArr" ) ) ).ok()
                .addProperty( "date.completeddatetime" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeComp" ) ) ).ok()
                .addProperty( "publicsafety.dispatchtypeid", "Dispatch_Type_ID" )
                .addProperty( "publicsafety.dispatchtype", "Type_ID" )
                .addProperty( "publicsafety.dispatchtypepriority" )
                .value( row -> Parsers.parseInt( row.getAs( "Type_Priority" ) ) ).ok()
                .addProperty( "publicsafety.dispatchtripnumber", "TripNumber" )
                .addProperty( "publicsafety.cfscasenumber", "Case_Num" )
                .addProperty( "publicsafety.cfscaseid", "Case_ID" )
                .addProperty( "criminaljustice.disposition", "Disposition" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "AppearsinDistype2" )
                .ofType( "general.appearsin" ).to( "JohnsonCFSAppearsIn" )
                .useCurrentSync()
                .fromEntity( "Unit" )
                .toEntity( "cfsUnit" )
                .addProperty( "general.stringid" ).value( Dispatch::getUnitID ).ok()
                .addProperty( "datetime.alerted" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeDisp" ) ) ).ok()
                .addProperty( "datetime.enroute" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeEnroute" ) ) ).ok()
                .addProperty( "datetime.arrived" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeArr" ) ) ).ok()
                .addProperty( "date.completeddatetime" )
                .value( row -> dateHelper0.parseTime( row.getAs( "TimeComp" ) ) ).ok()
                .endAssociation()
                .endAssociations()
                .done();

        // @formatter:off
        Flight noVehiclePeepsMapping = Flight        //entitites = CFS origin, person, originating address, contact info, vehicle.  Associations = originating at, involved in (vehicle info), appears in (personnel in CFS)
                .newFlight()
                    .createEntities()
                       .addEntity( "Personperson" )
                             .to( "JohnsonCoPeople" )
                             .useCurrentSync()
                             .addProperty( "nc.SubjectIdentification")
                                .value( Dispatch::getDispatchPersonID ).ok()        //use OfficerID if present (consistent for officers), if not use ID.
                             .addProperty( "nc.PersonGivenName" )
                                .value( row -> getFirstName( row.getAs( "OName" ) ) ).ok()
                             .addProperty( "nc.PersonMiddleName" )
                                .value( row -> getMiddleName( row.getAs( "OName" ) ) ).ok()
                             .addProperty( "nc.PersonSurName" )
                                .value( row -> getLastName( row.getAs( "OName" ) ) ).ok()
                            .addProperty( "im.PersonNickName" )
                                .value( row -> getName( row.getAs( "OName" ) ) ).ok()
                            .addProperty( "nc.PersonBirthDate" )
                                .value( row -> dateHelper0.parseLocalDate( row.getAs( "DOB" ) ) ).ok()
                            .addProperty( "nc.SSN", "SSN" )
                            .addProperty( "nc.PersonSex", "OSex" )
                            .addProperty( "nc.PersonRace", "ORace" )
                            .addProperty( "nc.PersonEthnicity", "Ethnicity" )
                            .addProperty( "nc.PersonEyeColorText", "Eyes" )
                            .addProperty( "nc.PersonHairColorText", "Hair" )
                            .addProperty( "nc.PersonHeightMeasure" )
                                //.value( row -> getHeightInch( row.getAs( "Height" ) ) ).ok()
                                .value( row -> Parsers.parseInt( row.getAs( "Height" ) ) ).ok()
                            .addProperty( "nc.PersonWeightMeasure" )
                                .value( row -> Parsers.parseInt( row.getAs( "Weight" ) ) ).ok()
                            .addProperty( "person.stateidnumber", "MNI_No" )
                       .endEntity()
                       .addEntity( "Personnelperson" )
                            .to( "JohnsonCoJusticeInvolvedPersonnel" )
                            .useCurrentSync()
                            .addProperty( "publicsafety.personnelid")
                                 .value( Dispatch::getDispatchPersonID ).ok()        //use OfficerID if present (consistent for officers), if not use ID.
                            .addProperty( "publicsafety.officerbadgeid", "BadgeNumber" )
                       .endEntity()
                       .addEntity( "Addressperson" )
                            .to("JohnsonCoAddresses")
                            .useCurrentSync()
                            .addProperty("location.Address")
                                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + " " + row.getAs( "OAddress_Apt" )
                                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" ) + " "
                                        + row.getAs( "OZip" )  + " " + getIntersection( row.getAs( "OAddress" ) )) )
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
                            .addProperty("publicsafety.dispatchid", "Dis_ID")
                            .addProperty( "event.comments", "OSQ" )
                        .endEntity()

                    .endEntities()
                    .createAssociations()

                        .addAssociation("AppearsInperson")
                            .ofType("general.appearsin")
                            .to("JohnsonCFSAppearsIn")
                            .useCurrentSync()
                            .entityIdGenerator( row -> row.get("Dis_ID") + getDispatchPersonID((Row) row)  )
                            .fromEntity("Personperson")
                            .toEntity("CallForServiceperson")
                           // .addProperty("general.datetime").value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) ).ok() //this field not in this table
                                //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                            .addProperty( "general.stringid", "Dis_ID" )
                            .addProperty( "nc.SubjectIdentification")
                                 .value( Dispatch::getDispatchPersonID ).ok()        //use OfficerID if present (consistent for officers), if not use ID.
                            .addProperty( "person.juvenile").value( row -> parseBool( row.getAs( "Juv" ) ) ).ok()
                            .addProperty( "person.age")
                                .value( row -> Parsers.parseInt( row.getAs( "Age" ) ) ).ok()
                                //.value( row -> getIntFromDouble( row.getAs( "Age" ) ) ).ok()
                            .addProperty( "publicsafety.dispatchpersontype", "Type" )
                            .addProperty( "publicsafety.dispatchpersontypedescription" ).value( Dispatch::getType ).ok()
                        .endAssociation()
                        .addAssociation("contactedatPerson1")
                            .ofType("geo.contactedat").to("JohnsonCFSContactedAt")
                            .useCurrentSync()
                            .fromEntity("Personperson")
                            .toEntity("ContactPerson1")
                            .addProperty( "general.stringid", "Dis_ID")
                        .endAssociation()
                        .addAssociation("contactedatPerson2")
                            .ofType("geo.contactedat").to("JohnsonCFSContactedAt")
                            .useCurrentSync()
                            .fromEntity("Personperson")
                            .toEntity("ContactPerson2")
                            .addProperty( "general.stringid", "Dis_ID")
                            .addProperty( "contact.cellphone" ).value(Dispatch::isCellphone).ok()
                        .endAssociation()
                        .addAssociation("AppearsInperson2")
                            .ofType("general.appearsin").to("JohnsonAppearsIn_address")
                            .useCurrentSync()
                            .fromEntity("Addressperson")
                            .toEntity("CallForServiceperson")
                            .addProperty( "general.stringid" , "ID")
                            .addProperty( "location.address" ).value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + "," + row.getAs( "OAddress_Apt" )
                                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                                        + " " + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) )) )
                                .ok()
                        .endAssociation()
                        .addAssociation( "worksasperson" )
                           .ofType( "economy.worksas" ).to("JohnsonCoWorksAsCJemployee")
                           .useCurrentSync()
                           .fromEntity( "Personperson" )
                           .toEntity( "Personnelperson" )
                           .addProperty( "general.stringid" )
                                 .value( Dispatch::getDispatchPersonID ).ok()        //use OfficerID if present (consistent for officers), if not use ID.
                        .endAssociation()
                    .endAssociations()
                .done();
        // @formatter:on

        Flight vehiclePeepsMapping = Flight        //entitites = CFS origin, person, originating address, contact info, vehicle.  Associations = originating at, involved in (vehicle info), appears in (personnel in CFS)
                .newFlight()
                .createEntities()
                .addEntity( "PersonvehicleP" )
                .to( "JohnsonCoPeople" )
                .useCurrentSync()
                .addProperty( "nc.SubjectIdentification" )
                .value( Dispatch::getDispatchPersonID )
                .ok()        //use OfficerID if present (consistent for officers), if not use ID.
                .addProperty( "nc.PersonGivenName" )
                .value( row -> getFirstName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonMiddleName" )
                .value( row -> getMiddleName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonSurName" )
                .value( row -> getLastName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "im.PersonNickName" )
                .value( row -> getName( row.getAs( "OName" ) ) ).ok()
                .addProperty( "nc.PersonBirthDate" )
                .value( row -> dateHelper0.parseLocalDate( row.getAs( "DOB" ) ) ).ok()
                .addProperty( "nc.SSN", "SSN" )
                .addProperty( "nc.PersonSex", "OSex" )
                .addProperty( "nc.PersonRace", "ORace" )
                .addProperty( "nc.PersonEthnicity", "Ethnicity" )
                .addProperty( "nc.PersonEyeColorText", "Eyes" )
                .addProperty( "nc.PersonHairColorText", "Hair" )
                .addProperty( "nc.PersonHeightMeasure" )
                //.value( row -> getHeightInch( row.getAs( "Height" ) ) ).ok()
                .value( row -> Parsers.parseInt( row.getAs( "Height" ) ) ).ok()
                .addProperty( "nc.PersonWeightMeasure" )
                .value( row -> Parsers.parseInt( row.getAs( "Weight" ) ) ).ok()
                .addProperty( "person.stateidnumber", "MNI_No" )
                .endEntity()
                .addEntity( "PersonnelvehicleP" )
                    .to( "JohnsonCoJusticeInvolvedPersonnel" )
                    .useCurrentSync()
                    .addProperty( "publicsafety.personnelid" )
                    .value( Dispatch::getDispatchPersonID )
                    .ok()        //use OfficerID if present (consistent for officers), if not use ID.
                    .addProperty( "publicsafety.officerbadgeid", "BadgeNumber" )
                .endEntity()
                .addEntity( "AddressvehicleP" )
                .to( "JohnsonCoAddresses" )
                .useCurrentSync()
                .addProperty( "location.Address" )
                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + " " + row.getAs( "OAddress_Apt" )
                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                        + " "
                        + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) ) ) )
                .ok()
                .addProperty( "location.street" )
                .value( row -> getStreet( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.intersection" )
                .value( row -> getIntersection( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.apartment", "OAddress_Apt" )
                .addProperty( "location.city" )
                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) ).ok()
                .addProperty( "location.state", "OState" )
                .addProperty( "location.zip", "OZip" )
                .endEntity()
                .addEntity( "ContactvehicleP1" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "OPhone" )
                .endEntity()
                .addEntity( "ContactvehicleP2" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "CellPhone" )
                .endEntity()
                .addEntity( "CallForServicevehicleP" )
                .to( "JohnsonCoCallForService" )
                .useCurrentSync()
                .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .addProperty( "event.comments", "OSQ" )
                .endEntity()
                .addEntity( "VehicleP" )
                .to( "JohnsonCoVehicle" )
                .useCurrentSync()
                .addProperty( "vehicle.id" )
                .value( row -> {
                    return getAsString( row.getAs( "MAKE" ) ) + getAsString( row.getAs( "MODEL" ) )
                            + getAsString( row.getAs( "LIC" ) ) + getAsString( row.getAs( "LIS" ) );
                } ).ok()
                .addProperty( "vehicle.make", "MAKE" )
                .addProperty( "vehicle.model", "MODEL" )
                .addProperty( "vehicle.licensenumber", "LIC" )
                .addProperty( "vehicle.licensestate", "LIS" )
                .addProperty( "vehicle.vin", "VIN" )
                .addProperty( "vehicle.year" )
                .value( row -> getStrYear( row.getAs( "VehYear" ) ) ).ok()
                .addProperty( "vehicle.color", "Color" )
                .addProperty( "vehicle.secondarycolor", "ColorSecondary" )
                .addProperty( "vehicle.style", "Style" )
                .addProperty( "vehicle.licenseplatetype", "LIT" )
                .addProperty( "vehicle.licenseyear" )
                .value( row -> getStrYear( row.getAs( "LIY" ) ) ).ok()
                .addProperty( "publicsafety.dispatchtransfervehicle" ).value( row -> parseBool( row.getAs( "TransferVehicle" ) ) )
                .ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "InvolvedInvehicleP" )
                    .ofType( "publicsafety.involvedin" )
                    .to( "JohnsonCoCFSInvolvedIn" )
                    .useCurrentSync()
                    .fromEntity( "VehicleP" )
                    .toEntity( "CallForServicevehicleP" )
                    .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .endAssociation()
                .addAssociation( "AppearsInvehicleP" )
                    .ofType( "general.appearsin" )
                    .to( "JohnsonCFSAppearsIn" )
                    .useCurrentSync()
                    .entityIdGenerator( row -> row.get( "Dis_ID" ) + row.get( "ID" ) )
                    .fromEntity( "PersonvehicleP" )
                    .toEntity( "CallForServicevehicleP" )
                    //.addProperty( "general.datetime" ).value( row -> dateHelper0.parse( row.getAs( "CFS_DateTimeJanet" ) ) )   //this field not in this table
                    //.ok()
                    //.value( row -> caseIdToTime.get( row.getAs( "Dis_ID" ) ) ).ok()
                    .addProperty( "general.stringid", "Dis_ID" )
                    .addProperty( "nc.SubjectIdentification" )
                    .value( Dispatch::getDispatchPersonID )
                    .ok()        //use OfficerID if present (consistent for officers), if not use ID.
                    .addProperty( "person.juvenile" ).value( row -> parseBool( row.getAs( "Juv" ) ) ).ok()
                    .addProperty( "person.age" )
                    .value( row -> Parsers.parseInt( row.getAs( "Age" ) ) ).ok()
                    //.value( row -> getIntFromDouble( row.getAs( "Age" ) ) ).ok()
                    .addProperty( "publicsafety.dispatchpersontype", "Type" )
                    .addProperty( "publicsafety.dispatchpersontypedescription" ).value( Dispatch::getType ).ok()
                .endAssociation()
                .addAssociation( "contactedatvehicleP1" )
                    .ofType( "geo.contactedat" ).to( "JohnsonCFSContactedAt" )
                    .useCurrentSync()
                    .fromEntity( "PersonvehicleP" )
                    .toEntity( "ContactvehicleP1" )
                    .addProperty( "general.stringid", "Dis_ID" )
                    .endAssociation()
                .addAssociation( "contactedatvehicleP2" )
                    .ofType( "geo.contactedat" ).to( "JohnsonCFSContactedAt" )
                    .useCurrentSync()
                    .fromEntity( "PersonvehicleP" )
                    .toEntity( "ContactvehicleP2" )
                    .addProperty( "general.stringid", "Dis_ID" )
                    .addProperty( "contact.cellphone" ).value( Dispatch::isCellphone ).ok()
                .endAssociation()
                .addAssociation( "AppearsInvehicle2P" )
                    .ofType( "general.appearsin" ).to( "JohnsonAppearsIn_address" )
                    .useCurrentSync()
                    .fromEntity( "AddressvehicleP" )
                    .toEntity( "CallForServicevehicleP" )
                    .addProperty( "general.stringid", "ID" )
                    .addProperty( "location.address" )
                    .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + "," + row.getAs( "OAddress_Apt" )
                            + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                            + " " + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) ) ) )
                    .ok()
                .endAssociation()
                .addAssociation( "worksasvehicleP" )
                    .ofType( "economy.worksas" ).to( "JohnsonCoWorksAsCJemployee" )
                    .useCurrentSync()
                    .fromEntity( "PersonvehicleP" )
                    .toEntity( "PersonnelvehicleP" )
                    .addProperty( "general.stringid" )
                    .value( Dispatch::getDispatchPersonID )
                    .ok()        //use OfficerID if present (consistent for officers), if not use ID.
                .endAssociation()
                .endAssociations()
                .done();

        Flight vehicleNoPeepsMapping = Flight        //entitites = CFS origin, person, originating address, contact info, vehicle.  Associations = originating at, involved in (vehicle info), appears in (personnel in CFS)
                .newFlight()
                .createEntities()

                .addEntity( "AddressvehicleNP" )
                .to( "JohnsonCoAddresses" )
                .useCurrentSync()
                .addProperty( "location.Address" )
                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + " " + row.getAs( "OAddress_Apt" )
                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                        + " "
                        + row.getAs( "OZip" ) + getIntersection( row.getAs( "OAddress" ) ) ) )
                .ok()
                .addProperty( "location.street" )
                .value( row -> getStreet( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.intersection" )
                .value( row -> getIntersection( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.apartment", "OAddress_Apt" )
                .addProperty( "location.city" )
                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) ).ok()
                .addProperty( "location.state", "OState" )
                .addProperty( "location.zip", "OZip" )
                .endEntity()
                .addEntity( "ContactvehicleNP1" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "OPhone" )
                .endEntity()
                .addEntity( "ContactvehicleNP2" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "CellPhone" )
                .endEntity()
                .addEntity( "CallForServicevehicleNP" )
                .to( "JohnsonCoCallForService" )
                .useCurrentSync()
                .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .addProperty( "event.comments", "OSQ" )
                .endEntity()
                .addEntity( "VehicleNP" )
                .to( "JohnsonCoVehicle" )
                .useCurrentSync()
                .addProperty( "vehicle.id" )
                .value( row -> {
                    return getAsString( row.getAs( "MAKE" ) ) + getAsString( row.getAs( "MODEL" ) )
                            + getAsString( row.getAs( "LIC" ) ) + getAsString( row.getAs( "LIS" ) );
                } ).ok()
                .addProperty( "vehicle.make", "MAKE" )
                .addProperty( "vehicle.model", "MODEL" )
                .addProperty( "vehicle.licensenumber", "LIC" )
                .addProperty( "vehicle.licensestate", "LIS" )
                .addProperty( "vehicle.vin", "VIN" )
                .addProperty( "vehicle.year" )
                .value( row -> getStrYear( row.getAs( "VehYear" ) ) ).ok()
                .addProperty( "vehicle.color", "Color" )
                .addProperty( "vehicle.secondarycolor", "ColorSecondary" )
                .addProperty( "vehicle.style", "Style" )
                .addProperty( "vehicle.licenseplatetype", "LIT" )
                .addProperty( "vehicle.licenseyear" )
                .value( row -> getStrYear( row.getAs( "LIY" ) ) ).ok()
                .addProperty( "publicsafety.dispatchtransfervehicle" ).value( row -> parseBool( row.getAs( "TransferVehicle" ) ) )
                .ok()
                .endEntity()
                .endEntities()

                .createAssociations()
                .addAssociation( "InvolvedInvehicleNP" )
                .ofType( "publicsafety.involvedin" )
                .to( "JohnsonCoCFSInvolvedIn" )
                .useCurrentSync()
                .fromEntity( "VehicleNP" )
                .toEntity( "CallForServicevehicleNP" )
                .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .endAssociation()
                .addAssociation( "AppearsInvehicleNP" )
                .ofType( "general.appearsin" ).to( "JohnsonAppearsIn_address" )
                .useCurrentSync()
                .fromEntity( "AddressvehicleNP" )
                .toEntity( "CallForServicevehicleNP" )
                .addProperty( "general.stringid", "ID" )
                .addProperty( "location.address" )
                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + "," + row.getAs( "OAddress_Apt" )
                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                        + " " + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) ) ) )
                .ok()
                .endAssociation()

                .endAssociations()
                .done();

        Flight noVehicleNoPeepsMapping = Flight        //entitites = CFS origin, person, originating address, contact info, vehicle.  Associations = originating at, involved in (vehicle info), appears in (personnel in CFS)
                .newFlight()
                .createEntities()

                .addEntity( "AddressNVNP" )
                .to( "JohnsonCoAddresses" )
                .useCurrentSync()
                .addProperty( "location.Address" )
                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + " " + row.getAs( "OAddress_Apt" )
                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                        + " "
                        + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) ) ) )
                .ok()
                .addProperty( "location.street" )
                .value( row -> getStreet( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.intersection" )
                .value( row -> getIntersection( row.getAs( "OAddress" ) ) ).ok()
                .addProperty( "location.apartment", "OAddress_Apt" )
                .addProperty( "location.city" )
                .value( row -> addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) ).ok()
                .addProperty( "location.state", "OState" )
                .addProperty( "location.zip", "OZip" )
                .endEntity()
                .addEntity( "ContactNVNP1" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "OPhone" )
                .endEntity()
                .addEntity( "ContactNVNP2" )
                .to( "JohnsonCoCFSContactInfo" )
                .useCurrentSync()
                .addProperty( "contact.id", "ID" )
                .addProperty( "contact.phonenumber", "CellPhone" )
                .endEntity()
                .addEntity( "CallForServiceNVNP" )
                .to( "JohnsonCoCallForService" )
                .useCurrentSync()
                .addProperty( "publicsafety.dispatchid", "Dis_ID" )
                .addProperty( "event.comments", "OSQ" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "AppearsInNVNP" )
                .ofType( "general.appearsin" ).to( "JohnsonAppearsIn_address" )
                .useCurrentSync()
                .fromEntity( "AddressNVNP" )
                .toEntity( "CallForServiceNVNP" )
                .addProperty( "general.stringid", "ID" )
                .addProperty( "location.address" )
                .value( row -> getAddressID( getStreet( row.getAs( "OAddress" ) ) + "," + row.getAs( "OAddress_Apt" )
                        + ", " + addSpaceAfterCommaUpperCase( row.getAs( "OCity" ) ) + ", " + row.getAs( "OState" )
                        + " " + row.getAs( "OZip" ) + " " + getIntersection( row.getAs( "OAddress" ) ) ) )
                .ok()
                .endAssociation()
                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>();
        flights.put( sysuserbaseMapping, sysuserbasePayload );
        flights.put( dispatchMapping, dispatchPayload );
        flights.put( nonUnitMapping, nonUnitPayload );
        flights.put( unitMapping, unitPayload );
        flights.put( noVehiclePeepsMapping, noVehiclePeepsPayload );
        flights.put( vehiclePeepsMapping, vehiclePeepsPayload );
        flights.put( vehicleNoPeepsMapping, vehicleNoPeepsPayload );
        flights.put( noVehicleNoPeepsMapping, noVehicleNoPeepsPayload );

        shuttle.launchPayloadFlight( flights );

    }

    public static Boolean isCellphone( Row row ) {
        String cellrow = row.getAs( "CellPhone" );
        if ( cellrow == null ) {
            return false;
        }
        return true;
    }

    //for strings that say "true" or "false"
    public static Boolean parseBool( Object obj ) {
        String boolStr = getAsString( obj );
        if ( boolStr != null ) {
            try {
                return Boolean.valueOf( boolStr );
            } catch ( IllegalArgumentException e ) {
                logger.error( "Unable to parse boolean from value {}", boolStr );
            }
        }
        return null;
    }

    public static String getBadgeNumber( Object obj ) {
        String badgerow = getAsString( obj );
        if ( badgerow != null && badgerow.length() > 0 ) {
            if ( Character.isDigit( badgerow.charAt( 0 ) ) ) {
                String[] strBadge = badgerow.split( " " );
                return strBadge[ 0 ].trim();
            }
        }
        return null;
    }

    public static boolean containsPerson( Map<String, String> row ) {
        String ppl = row.get( "OName" );
        if ( ppl != null ) {
            Matcher m = p.matcher( ppl );
            return false;
        }
        Matcher m = p.matcher( ppl );
        return m.find();
    }

    public static boolean containsUnit( Map<String, String> row ) {
        String unit = row.get( "Unit" );
        if ( unit != null ) {
            Matcher m = p.matcher( unit );
            return m.find();
        }
        return false;
    }

    public static String getUnitID( Row row ) {
        String unit = row.getAs( "Unit" );       //Unit name col
        String arrived = dateHelper0.parseTime( row.getAs( "TimeDisp" ) );
        String completed = dateHelper0.parseTime( row.getAs( "TimeComp" ) );

        if ( unit != null ) {
            Matcher m = p.matcher( unit );

            if ( m.find() ) {
                StringBuilder unitid = new StringBuilder( unit );
                unitid.append( " " ).append( arrived ).append( " " ).append( completed );
                return unitid.toString();
            }
            return null;
        }
        return null;
    }

    public static boolean containsVehicle( Map<String, String> row ) {
        String make = row.get( "MAKE" );
        String model = row.get( "MODEL" );
        String lic = row.get( "LIC" );
        String lis = row.get( "LIS" );
        StringBuilder car = new StringBuilder( make );
        car.append( model ).append( lic ).append( lis );

        if ( car.toString().length() > 0 ) {
            return true;
        }
        return false;
    }

    public static String getDispatchPersonID( Row row ) {
        String id = row.getAs( "OfficerID" );
        if ( id != null ) {
            return id;
        }
        return row.getAs( "ID" );
    }

    public static String getType( Row row ) {
        String ty = row.getAs( "Type" );
        if ( ty == null ) {
            return null;
        } else if ( ty.equals( "0" ) ) { return "Victim"; } else if ( ty.equals( "1" ) ) {
            return "Witness";
        } else if ( ty.equals( "2" ) ) { return "Suspect"; } else if ( ty.equals( "3" ) ) {
            return "Reported By";
        } else if ( ty.equals( "4" ) ) { return "Other"; } else if ( ty.equals( "5" ) ) {
            return "Passenger";
        } else if ( ty.equals( "6" ) ) { return "Driver"; } else if ( ty.equals( "7" ) ) {
            return "Driver Secured";
        } else if ( ty.equals( "8" ) ) { return "Passenger Secured"; } else if ( ty.equals( "9" ) ) {
            return "Secured Person";
        } else {
            return "";
        }
    }

    public static Integer getHeightInch( Object obj ) {
        String height = getAsString( obj );
        if ( height != null ) {
            if ( height.length() > 2 ) {
                String three = height.substring( 0, 3 );
                Integer feet = Parsers.parseInt( String.valueOf( three.substring( 0, 1 ) ) );
                Integer inch = Parsers.parseInt( String.valueOf( three.substring( 1 ) ) );
                if ( feet != null && inch != null ) { return ( feet * 12 ) + inch; }
            }

            return Parsers.parseInt( String.valueOf( height ) );
        }
        return null;
    }

    public static String getEmployeeId( Object obj ) {
        String employeeId = getAsString( obj );
        if ( employeeId != null ) {
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                return employeeId.substring( 2 ).trim();
            }
            return employeeId.trim();
        }
        return null;
    }

    public static String getActive( Object obj ) {
        String active = getAsString( obj );
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
        String dateStr = getAsString( obj );
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
        String s = getAsString( obj );
        if ( s != null ) {
            Double d = Parsers.parseDouble( s );
            if ( d != null ) { return d.intValue(); }
        }
        return null;
    }

    public static String getStringFromDouble( Object obj ) {
        String s = getAsString( obj );
        if ( s != null ) {
            Integer d = getIntFromDouble( s );
            if ( d != null ) { return d.toString(); }
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
        String str = getAsString( obj );
        if ( str != null ) {
            str = str.replaceAll( "[()\\- ]", "" );
            //str = str.substring( 0, 10 );
            return str;
        }
        return null;
    }

    public static String getStrYear( Object obj ) {
        String str = getAsString( obj );
        if ( str != null ) {
            String[] strDate = str.split( "/" );
            if ( strDate.length > 1 ) {
                String doubleStr = getStringFromDouble( strDate[ strDate.length - 1 ] );
                if ( doubleStr != null ) { return doubleStr.trim(); }
            }
            if ( str.contains( "DOB" ) ) {
                return "";
            }
            String doubleStr = getStringFromDouble( strDate[ 0 ] );
            if ( doubleStr != null ) { return doubleStr.trim(); }
        }
        return null;
    }

    public static String getStreet( Object obj ) {
        String address = getAsString( obj );
        if ( address != null ) {
            if ( !( address.contains( "/" ) ) ) {
                return addSpaceAfterCommaUpperCase( address );
            }
            return "";
        }
        return null;
    }

    public static String getAddressID( Object obj ) {
        String address = getAsString( obj );
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
        String address = getAsString( obj );
        if ( address != null ) {
            if ( address.contains( "/" ) ) {
                return address.replace( "/", " & " );
            }
            return "";
        }
        return null;
    }
}
