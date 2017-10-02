package com.openlattice.integrations.iowacity.dispatchcenter;

import com.google.common.collect.ImmutableList;
import com.openlattice.shuttle.dates.DateTimeHelper;
import com.openlattice.shuttle.dates.TimeZones;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Helpers {
    static         DateTimeHelper helper = new DateTimeHelper( TimeZones.America_Chicago, "yyyy-MM-dd HH:mm:ss" );
    private static Logger         logger = LoggerFactory.getLogger( Helpers.class );

    public static String getAsString( Object obj ) {
        if ( obj != null && obj.toString() != null ) {
            return obj.toString().trim();
        }
        return null;
    }

    public static Boolean getAsBoolean( Object obj ) {
        String str = getAsString( obj );
        if ( str != null ) {
            Set<String> trueSet = new HashSet<>( ImmutableList.of( "1", "true", "yes" ) );
            Set<String> falseSet = new HashSet<>( ImmutableList.of( "0", "false", "no" ) );
            if ( trueSet.contains( str.toLowerCase() ) ) {
                return true;
            } else if ( falseSet.contains( str.toLowerCase() ) ) {
                return false;
            }
        }
        return null;
    }

    public static UUID getAsUUID( Object obj ) {
        String officerId = getAsString( obj );
        if ( officerId != null ) {
            try {
                return UUID.fromString( officerId );
            } catch ( Exception e ) {
                logger.error( "Unable to parse officer id {} as UUID.", officerId );
                logger.trace( "Exception is: {}", e );
                return null;
            }
        }
        return null;
    }

    public static String getAsDateTime( Object obj ) {
        String timeStr = getAsString( obj );
        if ( timeStr != null ) {
            try {
                return helper.parse( timeStr );
                //                Date date = ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).parse( timeStr );
                //                return ( new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss" ) ).format( date );
            } catch ( Exception e ) {
                logger.error( "Unable to parse time {}.", timeStr );
                logger.trace( "Exception is: {}", e );
                return null;
            }
        }
        return null;
    }

    public static String getEmployeeId( Object obj ) {
        String employeeId = getAsString( obj );
        if ( employeeId != null ) {
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                employeeId = employeeId.substring( 2 );
            }
            return employeeId.trim();
        }
        return null;
    }

    public static boolean getActive( Object obj ) {
        String employeeId = getAsString( obj );
        return employeeId != null && !employeeId.toLowerCase().startsWith( "x_" );
    }

    public static String getAsDate( Object obj ) {
        String dateStr = getAsString( obj );
        if ( dateStr != null ) {
            try {
                Date date = helper.parseDT( dateStr ).toDate();
                return ( new SimpleDateFormat( "yyyy-MM-dd" ) ).format( date );
            } catch ( Exception e ) {
                logger.error( "Unable to parse time {}.", dateStr );
                logger.trace( "Exception is: {}", e );
                return null;
            }
        }
        return null;
    }

    public static String getAsTime( Object obj ) {
        String timeStr = getAsString( obj );
        if ( timeStr != null ) {
            try {
                Date date = helper.parseDT( timeStr ).toDate();
                return ( new SimpleDateFormat( "HH:mm:ss" ) ).format( date );
            } catch ( Exception e ) {
                logger.error( "Unable to parse time {}.", timeStr );
                logger.trace( "Exception is: {}", e );
                return null;
            }
        }
        return null;
    }
}
