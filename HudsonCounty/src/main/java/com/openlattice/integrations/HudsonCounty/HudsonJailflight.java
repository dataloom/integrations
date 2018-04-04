package com.openlattice.integrations.HudsonCounty;

import com.openlattice.client.RetrofitFactory;
import com.openlattice.edm.EdmApi;
import com.openlattice.shuttle.Flight;
import com.openlattice.shuttle.Shuttle;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import com.openlattice.shuttle.payload.Payload;
import com.openlattice.shuttle.payload.SimplePayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Kim Engie &lt;kim@openlattice.com&gt;
 */

public class HudsonJailflight {

    private static final Logger                      logger      = LoggerFactory
            .getLogger( HMISflight.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;
    private static final DateTimeHelper              bdHelper    = new DateTimeHelper( TimeZones.America_NewYork,
            "MM/dd/YY" );
    private static final DateTimeHelper              dtHelper    = new DateTimeHelper( TimeZones.America_NewYork,
            "MM-dd-YY" );

    public static void main( String[] args ) throws InterruptedException {

        final String jailPath = args[ 0 ];
        final String jwtToken = args[ 1 ];

        SimplePayload payload = new SimplePayload( jailPath );

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

//        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
//        EdmApi edm = retrofit.create( EdmApi.class );

        Flight jailflight = Flight.newFlight()
                .createEntities()

                .addEntity( "person" )
                //.to( "HudsonPeople" )
                .to( "HudsonJailPeople" )
                    .addProperty( "nc.SubjectIdentification", "Inmate Key" )
                    .addProperty( "nc.PersonSurName", "Last Name" )
                    .addProperty( "nc.PersonGivenName", "First Name" )
                    .addProperty( "nc.PersonBirthDate" ).value( row -> bdHelper.parseDate( row.getAs( "DOB" ) ) ).ok()
                .endEntity()

                .addEntity( "cjperson" )
                .to( "HudsonCJpersoninfo")
                    .addProperty( "criminaljustice.personid", "Inmate Key" )
                    .addProperty( "person.stateidnumber", "SBI Number" )
                    .addProperty( "person.stateidstate" ).value( row -> "NJ" ).ok()
                .endEntity()

                .addEntity( "booking" )
                .to( "Hudson_JailRecords" )
                    .addProperty( "criminaljustice.jailrecordid", "Commit #" )  //unique key for each jail stay
                    .addProperty( "criminaljustice.commitnumber", "Commit #" )
                    .addProperty( "criminaljustice.jailid", "Jail Id" )
                    .addProperty( "criminaljustice.committingauthority", "Committing Authority" )
                    .addProperty( "date.commit" )
                        .value( row -> dtHelper.parseDate( row.getAs( "Commit Date" ) ) ).ok()
                    .addProperty( "date.release" )
                        .value( row -> dtHelper.parseDate( row.getAs( "Discharge Date" ) ) ).ok()
                    .addProperty( "criminaljustice.releasenotes", "Disch Reas" )
                    .addProperty( "criminaljustice.timeserveddays", "Tot Days" )
                .endEntity()
                .endEntities()

                .createAssociations()

                .addAssociation( "bookedin" )
                .to( "Hudson_BookedIn" )
                    .fromEntity( "person" )
                    .toEntity( "booking" )
                    .addProperty( "general.stringid", "Inmate Key" )
                    .addProperty( "date.booking").value( row -> dtHelper.parseDate( row.getAs( "Commit Date" ) ) ).ok()
                .endAssociation()

                .addAssociation( "becomes" )
                .to("Hudson_Becomes")
                    .fromEntity( "person" )
                    .toEntity( "cjperson" )
                    .addProperty( "nc.SubjectIdentification", "Inmate Key" )
                .endAssociation()

                .endAssociations()

                .done();

        Shuttle shuttle = new Shuttle( environment, jwtToken );
        Map<Flight, Payload> flights = new HashMap<>( 1 );
        flights.put( jailflight, payload );

        shuttle.launchPayloadFlight( flights );
    }
}
