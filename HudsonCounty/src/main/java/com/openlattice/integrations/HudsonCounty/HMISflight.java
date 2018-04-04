package com.openlattice.integrations.HudsonCounty;



        import com.openlattice.client.RetrofitFactory;
        import com.openlattice.edm.EdmApi;
        import com.openlattice.shuttle.Flight;
        import com.openlattice.shuttle.MissionControl;
        import com.openlattice.shuttle.Shuttle;
        import com.openlattice.shuttle.adapter.Row;
        import com.openlattice.shuttle.dates.DateTimeHelper;
        import com.openlattice.shuttle.dates.TimeZones;
        import com.openlattice.shuttle.util.Parsers;
        import org.apache.commons.lang3.StringUtils;
        import org.slf4j.Logger;
        import org.slf4j.LoggerFactory;

        import java.util.*;
        import java.util.stream.Collectors;

        import com.openlattice.shuttle.payload.Payload;
        import com.openlattice.shuttle.payload.SimplePayload;
        import retrofit2.Retrofit;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class HMISflight {
    private static final Logger            logger      = LoggerFactory
            .getLogger( HMISflight.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;
    private static final DateTimeHelper              dtHelper    = new DateTimeHelper( TimeZones.America_NewYork, "MM/dd/YY" );

    public static void main( String[] args ) throws InterruptedException {

        final String hmisPath = args[ 0 ];
        final String jwtToken = args[ 1 ];

        SimplePayload payload = new SimplePayload( hmisPath );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        Flight hmisflight = Flight.newFlight()
                .createEntities()

                .addEntity( "person" )
                .to( "Hudson_HousingPeople" )
                    .addProperty( "nc.SubjectIdentification", "Personal ID" )
                    .addProperty( "nc.PersonSurName", "Last Name" )
                    .addProperty( "nc.PersonGivenName", "First Name" )
                    .addProperty( "nc.PersonMiddleName", "Middle Name" )
                    .addProperty( "nc.PersonBirthDate" ).value( row -> dtHelper.parseDate( row.getAs( "Date of Birth" ) ) ).ok()
                    .addProperty( "nc.SSN", "SSN" )
                    .addProperty( "nc.PersonEthnicity")
                        .value( HMISflight::standardEthnicity ).ok()
                    .addProperty( "nc.PersonRace")
                        .value( HMISflight::standardRaceList ).ok()
                    .addProperty( "nc.PersonSex" )
                        .value( HMISflight::standardSex ).ok()
                .endEntity()

                .addEntity( "housing" )
                .to( "HudsonHMISStays" )
                    .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                //.addProperty( "general.stringid" ).value( row -> UUID.randomUUID() ).ok()
//                    .addProperty( "general.stringid", "Client ID")
                    .addProperty( "housing.residentid", "Personal ID" )
                    .addProperty( "household.id", "Household ID" )
                    .addProperty( "person.gender", "Gender (HMIS)" )
                    .addProperty( "date.admission" )
                        .value( row -> dtHelper.parseDate( row.getAs( "Admission Date / Project Start Date" ) ) ).ok()
                    .addProperty( "person.ageatevent", "Age at Admission" )
                    .addProperty( "housing.admittedadults", "Number of Admitted Adults (at admission)" )
                    .addProperty( "housing.admittedchildren", "Number of Admitted Children (at admission)" )
                    .addProperty( "housing.projecttype", "HMIS Project Type" )
                    .addProperty( "housing.program", "Program" )
                    .addProperty( "housing.programtype", "Program Type" )
                    .addProperty( "housing.lengthofstay" )
                        .value( row -> Parsers.parseInt( row.getAs( "Length of Program Stay" ) ) ).ok()
                    .addProperty( "date.release")
                        .value( row -> dtHelper.parseDate( row.getAs( "Discharge Date" ))).ok()
                    .addProperty( "housing.dischargereason", "Reason for Discharge" )
                    .addProperty( "housing.timelessthan90days", "Time in Institution less than 90 days" )
                    .addProperty( "housing.timeintransitionalorpermanentlessthan7nights", "Time in Transitional or Permanent Housing less than 7 nights" )
                    .addProperty( "housing.notes", "Continuum Project" )
                    .addProperty( "housing.chronicallyhomeless",                                                                                                    "Chronically Homeless Point In Time" )
                .endEntity()

                .addEntity( "address" )
                .to("HudsonHousingAddresses")
                    .addProperty( "location.Address")
                        .value( HMISflight::getFulladdress ).ok()
                    .addProperty( "location.street", "Last Permanent Address: Street Address" )
                    .addProperty( "location.city", "Last Permanent Address: City" )
                    .addProperty( "location.state", "Last Permanent Address: State" )
                    .addProperty( "location.zip")
                        .value( HMISflight::getZip ).ok()
                .endEntity()

                .addEntity( "household" )
                .to("HudsonHousingHHs")
                    .addProperty( "household.id", "Household ID" )
                .endEntity()

                .addEntity( "admission_assmt" )
                .to("HudsonHMISAdmissionAssessments")
                    .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                    .addProperty( "housing.assessmentid", "Client ID" )
                    .addProperty( "person.gender", "Gender (HMIS)" )
                    .addProperty( "person.sexualorientation", "Sexual Orientation" )
                    .addProperty( "person.maritalstatus", "Marital Status" )
                    .addProperty( "person.veteranstatus", "Veteran Status" )
                    .addProperty( "housing.residencetype", "Type Of Residence" )
                    .addProperty( "economy.spousalsupport", "Alimony or other spousal support (Yes/No)  (Admission)" )
                    .addProperty( "economy.spousalsupportamount", "Alimony or other spousal support Amount (Admission)" )
                    .addProperty( "housing.datehomelessbegan")
                        .value( row -> dtHelper.parseDate( row.getAs( "Approximate date homelessness started" ))).ok()
                    .addProperty( "housing.chronicallyhomeless", "Chronically Homeless (Admission)" )
                    .addProperty( "housing.homelesscause", "Homeless Cause" )
                    .addProperty( "housing.monthshomelessorinshelterinpast3yrs", "Total number of months homeless on the street, in ES, or SH in the past three years" )
                    .addProperty( "economy.childsupport", "Child support (Yes/No)  (Admission)" )
                    .addProperty( "economy.childsupportamount", "Child support Amount (Admission)" )
                    .addProperty( "health.chroniccondition", "Chronic Health Condition (Admission)" )
                    //.addProperty( "health.chronicdisability", "Chronic Health Condition - Disability (Admission)" )
                    .addProperty( "health.developmentaldisability", "Developmental Disability (Admission)" )
                    .addProperty( "health.fleeingdomesticviolence", "Domestic Violence - Are you currently fleeing? (Admission)" )
                    .addProperty( "health.domesticviolenceoccurrence", "Domestic Violence Experience Occurred (Admission)" )
                    .addProperty( "economy.earnedincome", "Earned Income (i.e. employment income) (Yes/No)  (Admission)" )
                    .addProperty( "economy.earnedincomeamount", "Earned Income (i.e. employment income) Amount (Admission)" )
                    .addProperty( "economy.employmentstatus", "Employment Status (Admission)" )
                    .addProperty( "economy.publicassistance", "General Public Assistance (Yes/No)  (Admission)" )
                    .addProperty( "economy.publicassistanceamount", "General Public Assistance Amount (Admission)" )
                    .addProperty( "economy.anyincome", "Income From Any Source (Admission)" )
                    .addProperty( "economy.anyincomeamount", "Monthly Income Amount (Admission)" )
                    .addProperty( "economy.anyincometimeunit" )
                        .value( row -> "Monthly" ).ok()
                    .addProperty( "economy.anyincomesources", "Monthly Income Sources (Admission)" )
                    .addProperty( "economy.noncashbenefits", "Non-Cash Benefits from Any Source (Admission)" )
                    .addProperty( "economy.noncashbenefittypes", "Non-Cash Benefits Types (Admission)" )
                    .addProperty( "demographics.hhtype", "HMIS Household Type (at admission)" )
                    .addProperty( "demographics.hhsize", "Household Size (at admission)" )
                    .addProperty( "housing.indivorfamilytype", "Individual/Family Type (at admission)" )
                    .addProperty( "health.hivpositive", "HIV/AIDS (Admission)" )
                    //.addProperty( "health.hivdisability", "HIV/AIDS - Disability (Admission)" )
                    .addProperty( "housing.previousresidencelengthofstay", "Length of Stay in Previous Residence" )
                    .addProperty( "health.mentalissue", "Mental Health Problem (Admission)" )
                    //.addProperty( "health.mentalhealthdisability", "Mental Health Problem - Disability (Admission)" )
                    .addProperty( "housing.numberunstableepisodes", "Number of times the client has been on the streets, in ES, or SH in the past three years" )
                    .addProperty( "housing.nightbeforeinstreetorshelter", "On the night before Institution/TH/PH did you stay on the streets, ES or SH?" )
                    .addProperty( "housing.permanentmoveindate")
                        .value( row -> dtHelper.parseDate( row.getAs("Permanent Housing Move in Date" ))).ok()
                    .addProperty( "health.physicaldisability", "Physical Disability (Admission)" )
                    //.addProperty( "health.physicaldisabilitydescription", "Physical Disability - Disability (Admission)" )
                    .addProperty( "health.insurancecoverage", "Covered by Health Insurance (Admission)" )
                    .addProperty( "form.relationtoprimaryclient", "Relation to Primary Client" )
                    .addProperty( "substance.type", "Substance Abuse (Admission)" )
                   // .addProperty( "substance.abuseisdisability", "Substance Abuse - Disability (Admission)" )
                .endEntity()

                .addEntity( "discharge_assmt" )
                .to("HudsonHMISDischargeAssessments")
                    .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                    .addProperty( "housing.assessmentid", "Client ID" )
                    .addProperty( "person.gender", "Gender (HMIS)" )
                    .addProperty( "person.sexualorientation", "Sexual Orientation" )
                    .addProperty( "person.maritalstatus", "Marital Status" )
                    .addProperty( "person.veteranstatus", "Veteran Status" )
                    .addProperty( "economy.spousalsupport", "Alimony or other spousal support (Yes/No)  (Discharge)" )
                    .addProperty( "economy.spousalsupportamount", "Alimony or other spousal support Amount (Discharge)" )
                    .addProperty( "economy.childsupport", "Child support (Yes/No)  (Discharge)" )
                    .addProperty( "economy.childsupportamount", "Child support Amount (Discharge)" )
                    .addProperty( "health.chroniccondition", "Chronic Health Condition (Discharge)" )
                    //.addProperty( "health.chronicdisability", "Chronic Health Condition - Disability (Discharge)" )
                    .addProperty( "health.developmentaldisability", "Developmental Disability (Discharge)" )
                    .addProperty( "economy.earnedincome", "Earned Income (i.e. employment income) (Yes/No)  (Discharge)" )
                    .addProperty( "economy.earnedincomeamount", "Earned Income (i.e. employment income) Amount (Discharge)" )
                    .addProperty( "economy.employmentstatus", "Employment Status (Discharge)" )
                    .addProperty( "economy.publicassistance", "General Public Assistance (Yes/No)  (Discharge)" )
                    .addProperty( "economy.publicassistanceamount", "General Public Assistance Amount (Discharge)" )
                    .addProperty( "economy.anyincome", "Income From Any Source (Discharge)" )
                    .addProperty( "economy.anyincomeamount", "Monthly Income Amount (Discharge)" )
                    .addProperty( "economy.anyincometimeunit" )
                        .value( row -> "Monthly" ).ok()
                    .addProperty( "economy.anyincomesources", "Monthly Income Sources (Discharge)" )
                    .addProperty( "health.hivpositive", "HIV/AIDS (Discharge)" )
                    //.addProperty( "health.hivdisability", "HIV/AIDS - Disability (Discharge)" )
                    .addProperty( "health.mentalissue", "Mental Health Problem (Discharge)" )
                    //.addProperty( "health.mentalhealthdisability", "Mental Health Problem - Disability (Discharge)" )
                    .addProperty( "housing.permanentmoveindate")
                        .value( row -> dtHelper.parseDate( row.getAs( "Permanent Housing Move in Date" ))).ok()
                    .addProperty( "health.physicaldisability", "Physical Disability (Discharge)" )
                    //.addProperty( "health.physicaldisabilitydescription", "Physical Disability - Disability (Discharge)" )
                    .addProperty( "form.relationtoprimaryclient", "Relation to Primary Client" )
                    .addProperty( "substance.type", "Substance Abuse (Discharge)" )
                    //.addProperty( "substance.abuseisdisability", "Substance Abuse - Disability (Discharge)" )
                    .addProperty( "housing.nextsteps", "Destination" )
                .endEntity()

                .addEntity( "update_assmt" )
                .to("HudsonHMISUpdateAssessments")
                    .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                    .addProperty( "housing.assessmentid", "Client ID" )
                    .addProperty( "economy.spousalsupport", "Alimony or other spousal support (Yes/No)  (Update)" )
                    .addProperty( "economy.spousalsupportamount", "Alimony or other spousal support Amount (Update)" )
                    .addProperty( "economy.childsupport", "Child support (Yes/No)  (Update)" )
                    .addProperty( "economy.childsupportamount", "Child support Amount (Update)" )
                    .addProperty( "economy.earnedincome", "Earned Income (i.e. employment income) (Yes/No)  (Update)" )
                    .addProperty( "economy.earnedincomeamount", "Earned Income (i.e. employment income) Amount (Update)" )
                    .addProperty( "economy.publicassistance", "General Public Assistance (Yes/No)  (Update)" )
                    .addProperty( "economy.publicassistanceamount", "General Public Assistance Amount (Update)" )
                    .addProperty( "economy.anyincome", "Income From Any Source (Update)" )
                    .addProperty( "economy.anyincomeamount", "Monthly Income Amount (Update)" )
                    .addProperty( "economy.anyincometimeunit" )
                        .value( row -> "Monthly" ).ok()
                    .addProperty( "economy.anyincomesources", "Monthly Income Sources (Update)" )
                    .addProperty( "economy.noncashbenefits", "Non-Cash Benefits from Any Source (Update)" )
                    .addProperty( "economy.noncashbenefittypes", "Non-Cash Benefits Types (Update)" )
                    .addProperty( "economy.otherincome", "Other Income Type   (Please Specify) (Update)" )
                .endEntity()

                .endEntities()
                .createAssociations()

                .addAssociation( "participatedin" )
                .to( "Hudsonparticipatedin" )
                    .fromEntity( "person" )
                    .toEntity( "housing" )
                    .addProperty( "general.stringid" )
                        .value( row -> row.getAs("Client ID")+dtHelper.parseDate( row.getAs( "Admission Date / Project Start Date" ) )).ok()
                        //.value( row -> UUID.randomUUID() ).ok()
                .endAssociation()
                .addAssociation( "locatedat" )
                .to("HudsonLocatedAt")
                    .fromEntity( "person" )
                    .toEntity( "address" )
//                    .entityIdGenerator( row->row.get("Personal ID") )
                    .addProperty( "nc.SubjectIdentification", "Personal ID" )
                .endAssociation()
                .addAssociation( "kintiesto" )
                .to( "Hudsonkintiesto" )
                    .fromEntity( "person" )
                    .toEntity( "household" )
                    .addProperty( "general.stringid", "Household ID" )
                    .addProperty( "demographics.hhtype", "HMIS Household Type (at admission)" )
                    .addProperty( "demographics.hhsize", "Household Size (at admission)" )
                .endAssociation()
                .addAssociation( "assessedwith1" )
                .to( "HudsonAssessedWith" )
                    .fromEntity( "person" )
                    .toEntity( "admission_assmt" )
                        .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                        .addProperty( "general.stringid", "Personal ID" )
                .endAssociation()
                .addAssociation( "assessedwith2" )
                .to( "HudsonAssessedWith" )
                    .fromEntity( "person" )
                    .toEntity( "discharge_assmt" )
                .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                    .addProperty( "general.stringid", "Personal ID" )
                .endAssociation()
                .addAssociation( "assessedwith3" )
                .to( "HudsonAssessedWith" )
                    .fromEntity( "person" )
                    .toEntity( "update_assmt" )
                    .entityIdGenerator(row->row.get("Client ID")+dtHelper.parseDate( row.get( "Admission Date / Project Start Date" ) ))
                    .addProperty( "general.stringid", "Personal ID" )
                .endAssociation()

                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( hmisflight, payload );

        shuttle.launchPayloadFlight( flights );
    }

    public static String getZip (Row row) {
        String zipcode = row.getAs( "Zip Code of Last Permanent Address" );
        if ( StringUtils.isBlank( zipcode ) ) return null;

        if ( StringUtils.isNotBlank( zipcode ) ) {
            return StringUtils.leftPad( zipcode, 5, "0" );
        }
        return null;
    }

    public static String getFulladdress (Row row) {
        String street =  row.getAs ( "Last Permanent Address: Street Address" );
        String city = row.getAs( "Last Permanent Address: City" );
        String state = row.getAs( "Last Permanent Address: State" );
        String zipcode = HMISflight.getZip(row);

//        StringUtils.defaultString( null ) = "";
//        StringUtils.defaultString( "" ) = "";

//     if ( StringUtils.isNotBlank( zipcode )) {
        if (street != null ) {
         StringBuilder address = new StringBuilder( StringUtils.defaultString( street ) );
         address.append( ", " ).append( StringUtils.defaultString( city )). append( ", ")
                 .append( StringUtils.defaultString( state )).append( " " ).append( StringUtils.defaultString( zipcode ));

         return address.toString();
        }
//     else if ( StringUtils.isBlank( zipcode )) {

        else if ( city != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( city ));
            address.append( ", " ).append( StringUtils.defaultString( state )).append( " " )
                    .append( StringUtils.defaultString( zipcode ));
            return address.toString();
        }

        else if ( state != null ) {
            StringBuilder address = new StringBuilder( StringUtils.defaultString( state));
            address.append( " " ).append( StringUtils.defaultString( zipcode ));
            return state;
        }

        else if (zipcode != null ) {
            return StringUtils.defaultString( zipcode );
        }
    return null;
    }


    public static List standardRaceList( Row row ) {
        String sr = row.getAs("Race (HMIS)");

        if  (sr != null) {

            String[] racesArray = StringUtils.split( sr, "," );
            List<String> races = Arrays.asList( racesArray );

            if ( races != null ) {
                Collections.replaceAll( races, "Asian", "asian" );
                Collections.replaceAll( races, "White", "white" );
                Collections.replaceAll( races, "Black or African American", "black" );
                Collections.replaceAll( races, "American Indian or Alaska Native", "amindian" );
                Collections.replaceAll( races, "Native Hawaiian or Other Pacific Islander", "pacisland" );
                Collections.replaceAll( races, "Client doesn't know", "" );
                Collections.replaceAll( races, "Data not collected", "" );
                Collections.replaceAll( races, "Client refused", "" );
                Collections.replaceAll( races, "", "" );

                List<String> finalRaces = races
                        .stream()
                        .filter( StringUtils::isNotBlank )
                        //                    .filter( (race) ->
                        //                            (race != "Client refused") && (race != "Data not collected")
                        //                    )
                        .collect( Collectors.toList() );

                return finalRaces;
            }
            return null;
        }
        return null;
    }

    public static String standardEthnicity( Row row ) {
        String eth = row.getAs("Ethnicity (HMIS)");

        if ( eth != null ) {
            if ( eth.equals("Hispanic/Latino")) { return "hispanic"; };
            if ( eth.equals("Non-Hispanic/Non-Latino")) { return "nonhispanic"; };
            if ( eth.equals( "Client doesn't know" )) { return null; };
            if ( eth.equals( "Data not collected" )) { return null; };
            if ( eth.equals( "Client refused" )) { return null; };
            if ( eth.equals( "" )) { return null; };
        }

        return null;
    }

    public static String standardSex( Row row) {
        String sex = row.getAs( "Gender (HMIS)" );
        //if (StringUtils.isBlank( sex )) return null;

        if ( sex != null ) {
            if (sex.equals( "Male" )) {return "M"; };
            if (sex.equals( "Trans Male (FTM or Female to Male" )) {return "M"; };
            if (sex.equals( "Female" )) {return "F"; };
            if (sex.equals( "Trans Female (MTF or Male to Female" )) {return "F"; };
            if ( sex.equals( "Client doesn't know" )) { return null; };
            if ( sex.equals( "Data not collected" )) { return null; };
            if ( sex.equals( "Client refused" )) { return null; };
            if ( sex.equals( "" )) { return null; };

            }
            return null;

    }

    }


