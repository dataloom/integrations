package com.openlattice.integrations.iowacity.dispatchcenter;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class Helpers {

    public static UUID EMPTY_UUID = new UUID( 0L, 0L );

    public static String getString( Object obj ) {
        if ( obj != null && obj.toString() != null ) {
            return obj.toString().trim();
        }
        return null;
    }

    public static Integer getInteger( Object obj ) {
        String str = getString( obj );
        if ( str != null ) {
            try {
                return Integer.parseInt( str );
            } catch ( Exception e ) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }

    public static UUID getUUID( Object obj ) {
        String officerId = getString( obj );
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

    public static String getEmployeeId( Object obj ) {
        String employeeId = getString( obj );
        if ( employeeId != null ) {
            if ( employeeId.toLowerCase().startsWith( "x_" ) ) {
                employeeId = employeeId.substring( 2 );
            }
            return employeeId.trim();
        }
        return null;
    }

    public static boolean getActive( Object obj ) {
        String employeeId = getString( obj );
        return employeeId != null && !employeeId.toLowerCase().startsWith( "x_" );
    }

    public static String getDispatchDate( Object obj ) {
        String dateStr = getString( obj );
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

    public static String getDispatchTime( Object obj ) {
        String timeStr = getString( obj );
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

    public static String getDateTimeJanet( Object obj ) {
        String timeStr = getString( obj );
        if ( timeStr != null ) {
            try {
                Date date = ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).parse( timeStr );
                return ( new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss" ) ).format( date );
            } catch ( Exception e ) {
                e.printStackTrace();
            }
            return timeStr;
        }
        return null;
    }

    public static Integer getLZip( Object obj ) {
        String str = getString( obj );
        if ( str != null ) {
            try {
                return Integer.parseInt( str.substring( 0, 5 ) );
            } catch ( Exception e ) {
                e.printStackTrace();
                return null;
            }
        }
        return null;
    }
}
