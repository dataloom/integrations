package com.openlattice.integrations.wisconsin.danecounty;

import com.dataloom.client.RetrofitFactory;
import com.dataloom.data.serializers.FullQualifedNameJacksonDeserializer;
import com.dataloom.data.serializers.FullQualifedNameJacksonSerializer;
import com.dataloom.edm.EdmApi;
import com.dataloom.mappers.ObjectMappers;
import com.kryptnostic.rhizome.configuration.service.ConfigurationService;
import com.kryptnostic.rhizome.configuration.service.RhizomeConfigurationService;
import com.openlattice.shuttle.MissionControl;
import com.openlattice.shuttle.edm.RequiredEdmElements;
import com.openlattice.shuttle.edm.RequiredEdmElementsManager;
import org.apache.olingo.commons.api.edm.FullQualifiedName;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import retrofit2.Retrofit;

/**
 * Created by mtamayo on 6/16/17.
 */
public class EdmSetup {
    private static final Logger logger = LoggerFactory.getLogger( EdmSetup.class );
    private static final RetrofitFactory.Environment environment = RetrofitFactory.Environment.PRODUCTION;
    public static FullQualifiedName ARREST_AGENCY_FQN = new FullQualifiedName( "j.ArrestAgency" );
    public static FullQualifiedName FIRSTNAME_FQN     = new FullQualifiedName( "nc.PersonGivenName" );
    //public static FullQualifiedName MIDDLENAME_FQN               = new FullQualifiedName( "nc.PersonMiddleName" );
    public static FullQualifiedName LASTNAME_FQN      = new FullQualifiedName( "nc.PersonSurName" );
    public static FullQualifiedName SEX_FQN           = new FullQualifiedName( "nc.PersonSex" );
    public static FullQualifiedName RACE_FQN          = new FullQualifiedName( "nc.PersonRace" );
    public static FullQualifiedName ETHNICITY_FQN     = new FullQualifiedName( "nc.PersonEthnicity" );
    public static FullQualifiedName DOB_FQN           = new FullQualifiedName( "nc.PersonBirthDate" );
    public static FullQualifiedName OFFICER_ID_FQN    = new FullQualifiedName( "publicsafety.officerID" );
    public static FullQualifiedName ARREST_DATE_FQN   = new FullQualifiedName( "publicsafety.arrestdate" );
//    static {
//        FullQualifedNameJacksonDeserializer.registerWithMapper( ObjectMappers.getYamlMapper() );
//        FullQualifedNameJacksonSerializer.registerWithMapper( ObjectMappers.getYamlMapper() );
//    }
    public static void main( String[] args ) {
        final String jwtToken = args[ 0 ];

        logger.info( "Using the following idToken: Bearer {}", jwtToken );

        Retrofit retrofit = RetrofitFactory.newClient( environment, () -> jwtToken );
        EdmApi edm = retrofit.create( EdmApi.class );

        RequiredEdmElements elements = ConfigurationService.StaticLoader.loadConfiguration( RequiredEdmElements.class );

        RequiredEdmElementsManager reem = new RequiredEdmElementsManager( edm );

        reem.ensureEdmElementsExist( elements );
    }
}
