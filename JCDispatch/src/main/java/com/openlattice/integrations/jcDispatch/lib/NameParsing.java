package com.openlattice.integrations.jcDispatch.lib;

import com.openlattice.shuttle.util.Parsers;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NameParsing {

    public static String addSpaceAfterCommaUpperCase( Object obj ) {
        if ( obj != null ) {
            String name = Parsers.getAsString( obj ).toLowerCase();
            return name.replace( ",", ", " ).toUpperCase();
        }
        return null;
    }

    public static String removeDigits( Object obj ) {
        if ( obj != null ) {
            String name = Parsers.getAsString( obj );
            name = name.replaceAll( "[\\d]+", "" ).trim();
            return name;
        }
        return null;
    }

    public static final Pattern p = Pattern
            .compile( "\\b(UNIVERSITY|EMS|HONDA|LP|REHABILITATION|INC|IOWA|ADT|VERIZON|SPRINT|SANDWICHES|AT&T|BLDG|CENTER|CELLULAR|INTERNATIONAL|SCIENCES)\\b" , Pattern.CASE_INSENSITIVE );

    public static String getFirstName( Object obj ) {
        if ( obj != null ) {
            String name = addSpaceAfterCommaUpperCase( obj );
            name = removeDigits( name );
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                String fName = strNames[ 1 ].trim();
                String[] fNames = fName.split( " " );
                return fNames[ 0 ].trim();
            }

            String[] fullNames = strNames[ 0 ].split( " " );
            return fullNames[ 0 ].trim();
        }
        return null;
    }

    public static String getLastName( Object obj ) {
        if ( obj != null ) {
            String name = addSpaceAfterCommaUpperCase( obj );
            name = removeDigits( name );
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                return strNames[ 0 ].trim();
            }
            String[] fullNames = strNames[ 0 ].split( " " );
            return fullNames[ fullNames.length - 1 ].trim();
        }
        return null;
    }

    public static String getMiddleName( Object obj ) {
        if ( obj != null ) {
            String name = addSpaceAfterCommaUpperCase( obj );
            name = removeDigits( name );
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                String fName = strNames[ 1 ].trim();
                List<String> fNames = Arrays.asList( fName.split( " " ) );
                if ( fNames.size() > 2 ) {
                    List<String> mNames = fNames.subList( 1, fNames.size() );
                    return String.join( " ", mNames ).trim();
                }
                if ( fNames.size() == 2 ) {
                    return fNames.get( fNames.size() - 1 ).trim();
                }
                return "";
            }
            String[] middleNames = strNames[ 0 ].split( " " );
            if ( middleNames.length > 2 ) {
                return middleNames[ 1 ].trim();
            }
        }
        return null;
    }

    public static String getName( Object obj ) {
        if ( obj != null ) {
            String name = Parsers.getAsString( obj );
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return name;
            }
            return "";
        }
        return null;
    }
}
