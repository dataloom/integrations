
/*this is the path so change to com.openlattice.integrations....   */
package com.openlattice.integrations.portlandme;

/*public class is the same as your filename, which here is dataintegration*/

import com.dataloom.client.RetrofitFactory;
import com.dataloom.client.RetrofitFactory.Environment;
import com.dataloom.edm.EdmApi;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import com.openlattice.shuttle.util.Parsers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;


//import com.dataloom.client.RetrofitFactory.Environment;

public class Amistad {

    // Logger is used to output useful debugging messages to the console
    private static final Logger logger = LoggerFactory.getLogger(Amistad.class);

    // Set your Entity Set Name, Type, and Primary Key

    // These DateTimeHelpers go above the "public static void main" line
    private static final RetrofitFactory.Environment environment = Environment.PRODUCTION;
    private static final DateTimeHelper dateHelper0 = new DateTimeHelper(TimeZones.America_NewYork, "m/d/yy");
    private static final DateTimeHelper dateHelper1 = new DateTimeHelper(TimeZones.America_NewYork, "m/d/yy hh:mm:ss");

    public static void main(String[] args) throws InterruptedException {
        /*
         * It's worth noting that we are omitting validation such as making sure enough args were passed in, checking
         * existence of the file, and making sure authentication was successful. A failure in one of these cases
         * will cause the program to exit with an exception.
         */
        final String Amistadpath = args[0];
        final String jwtToken = args[1];
        //    final String jwtToken = MissionControl.getIdToken( username, password );
        logger.info("Using the following idToken: Bearer {}", jwtToken);

        Retrofit retrofit = RetrofitFactory.newClient(environment, () -> jwtToken);
        EdmApi edm = retrofit.create(EdmApi.class);

        //creating object type simplepayload that contains Amistadpath
        SimplePayload payload = new SimplePayload(Amistadpath);

        Flight Amistadflight = Flight.newFlight()
                .createEntities()

                .addEntity("Amistad People Entity")
                .to("amistad_people")
                .addProperty("nc.PersonGivenName", "First Name")
                .addProperty("nc.PersonMiddleName", "Middle Name")
                .addProperty("nc.PersonSurName", "Last Name")
                .addProperty("nc.PersonSuffix", "Suffix")
                .addProperty("im.PersonNickName", "Nickname")
                .addProperty("nc.SSN", "Social Security Number")
                .addProperty("nc.PersonBirthDate").value(row -> dateHelper0.parse(row.getAs("Birth Date"))).ok()
                .addProperty("nc.PersonRace", "Race")
                .addProperty("nc.PersonEthnicity", "Ethnicity")
                .addProperty("nc.PersonSex", "Sex")
                .addProperty("nc.PersonEyeColorText", "Eye Color")
                .addProperty("nc.PersonBirthPlace", "Birth Place")
                .addProperty("nc.SubjectIdentification", "Person ID")
                .addProperty("housing.notes", "Notes")
                .endEntity()

                .addEntity("Amistad Referrals In Entity")
                .to("amistad_referrals_in")
                .addProperty("housing.referralcontactname", "Referrer Name")
                .addProperty("housing.referraltype", "Referral Type")
                .addProperty("housing.referralorganization", "Referral Organization")
                .addProperty("housing.referralreason", "Referral Reason")
                .addProperty("housing.referralcontactinfo", "Referrer Contact Information")
                .addProperty("housing.referraldate").value(row -> dateHelper0.parse(row.getAs("Referral Date"))).ok()
                .addProperty("housing.notes", "Results")
                .addProperty("date.completeddatetime").value(row -> dateHelper1.parse(row.getAs("Referral DateTime"))).ok()
                .addProperty("housing.contactdetails", "Client Contact Information")
                .addProperty("housing.referralrelationship", "Relationship of Referrer to Client")
                .endEntity()

                .addEntity("Amistad Contact Information Entity")
                .to("amistad_contact_information")
                .addProperty("housing.contactdetails", "Best Way to Contact")
                .addProperty("contact.Email", "Email")
                .addProperty("contact.phonenumber", "Phone Number")
                .addProperty("contact.emergencynumber", "Emergency Contact Number")
                .addProperty("contact.emergencyname", "Emergency Contact")
                .addProperty("contact.id", "Person ID")
                .endEntity()

//                .addEntity("Amistad Personnel Entity")
//                .to("amistad_personnel_information")
//               .addProperty("personnel.id", "Person ID")
//                .addProperty("general.active")
//                .value( row -> getEmployeeId( row.getAs( "employeeid" ) ) ).ok()
//                .addProperty("person.employeeID")
//                .value( row -> getActive( row.getAs( "employeeid" ) ) ).ok()
//
//                .addProperty("housing.title", "Job Title")
//                .addProperty("personnel.agency", "Employer")
//                .endEntity()

                //   .addProperty("general.contactdate").value(row -> dateHelper0.parse(row.getAs("Contact Date"))).ok()
              //  .addProperty("personnel.employeeid").value( row -> getEmployeeId( row.getAs( "Employee ID" ) ) ).ok()
                .endEntities()

                .createAssociations()

                .addAssociation("association0")
                .to("amistad_referred_by")
                .fromEntity("Amistad People Entity")
                .toEntity("Amistad Referrals In Entity")
                .addProperty("date.completeddatetime").value(row -> dateHelper1.parse(row.getAs("Referral DateTime"))).ok()
                .endAssociation()

                .addAssociation("association1")
                .to("amistad_contacted_at")
                .fromEntity("Amistad People Entity")
                .toEntity("Amistad Contact Information Entity")
                .addProperty("date.completeddatetime").value(row -> dateHelper1.parse(row.getAs("Contacted DateTime"))).ok()
                .endAssociation()

//                .addAssociation("association2")
//                .to("amistad_works_as")
//                .fromEntity("Amistad People Entity")
//                .toEntity("Amistad ______ Entity")
//                .addProperty("general.stringid").value( row -> UUID.randomUUID().toString() ).ok()
//               .addProperty("housing.hiredate").value(row -> dateHelper1.parse(row.getAs("Hire Date"))).ok()
 //               .endAssociation()
                //this is actually a datetime but rather it be a date

                .endAssociations()
                .done();

        Shuttle shuttle = new Shuttle(environment, jwtToken);
        Map<Flight, Payload> flights = new HashMap<>(1);
        flights.put(Amistadflight, payload);
        shuttle.launchPayloadFlight(flights);
    }

    // CUSTOM FUNCTIONS DEFINED BELOW

    //will probably have to do this for the contact active and sex offender booleans
//    public static Integer getHeightInch (Object obj )  {
//        String height = Parsers.getAsString( obj );
//        if ( height != null ) {
//            if (height.length() > 2) {
//                String three = height.substring( 0, 3 );
//                Integer feet = Integer.parseInt( String.valueOf( three.substring( 0, 1 ) ) );
//                Integer inch = Integer.parseInt( String.valueOf( three.substring( 1 ) ) );
//                return ( feet * 12 ) + inch;
//            }
//
//            return Integer.parseInt( String.valueOf( height ) );
//        }
//        return null;
//    }


    public static Boolean getActive( Object obj ) {
        String active = Parsers.getAsString( obj );
        if ( active != null ) {
            if ( active.toLowerCase().startsWith( "n_" ) ) {
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }
        return null;
    }


//    public static String getEmployeeId( Object obj ) {
//        String employeeId = Parsers.getAsString( obj );
//        if ( employeeId != null ) {
//            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
//                return employeeId.substring( 2 ).trim();
//            }
//            return employeeId.trim();
//        }
//        return null;
//    }
}



