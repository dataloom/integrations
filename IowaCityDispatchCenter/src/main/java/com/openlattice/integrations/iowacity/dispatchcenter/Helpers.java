package com.openlattice.integrations.iowacity.dispatchcenter;

import com.google.common.collect.ImmutableList;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class Helpers {

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
                e.printStackTrace();
                // TODO: what do we do if the officer ID is not a valid UUID?
                return null;
            }
        }
        return null;
    }

    public static String getAsDateTime( Object obj ) {
        String timeStr = getAsString( obj );
        if ( timeStr != null ) {
            try {
                Date date = ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).parse( timeStr );
                return ( new SimpleDateFormat( "yyyy-MM-dd'T'HH:mm:ss" ) ).format( date );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return timeStr;
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
                Date date = ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).parse( dateStr );
                return ( new SimpleDateFormat( "yyyy-MM-dd" ) ).format( date );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return dateStr;
        }
        return null;
    }

    public static String getAsTime( Object obj ) {
        String timeStr = getAsString( obj );
        if ( timeStr != null ) {
            try {
                Date date = ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).parse( timeStr );
                return ( new SimpleDateFormat( "HH:mm:ss" ) ).format( date );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return timeStr;
        }
        return null;
    }
}
