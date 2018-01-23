package com.openlattice.integrations.jcDispatch.lib;

import com.openlattice.shuttle.util.Parsers;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class NameParsing {

    public static String addSpaceAfterCommaUpperCase( Object obj ) {
        String name = Parsers.getAsString( obj );
        if ( name != null ) {
            return name.toLowerCase().replace( ",", ", " ).toUpperCase();
        }
        return null;
    }

    public static String removeDigits( Object obj ) {
        String name = Parsers.getAsString( obj );
        if ( name != null ) {
            return name.replaceAll( "[\\d]+", "" ).trim();
        }
        return null;
    }

    public static final Pattern p = Pattern
            .compile( "\\b(UNIVERSITY|EMS|ENGINE|RESCUE|HONDA|LP|REHABILITATION|INC|IOWA|ADT|VERIZON|SPRINT|SANDWICHES|AT&T|BLDG|CENTER|CELLULAR|INTERNATIONAL|SCIENCES)\\b" , Pattern.CASE_INSENSITIVE );

    public static String getFirstName( Object obj ) {
        String name = removeDigits( obj );
        if ( name != null ) {
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            //for if entry is like DOE, JOHN
            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                String fName = strNames[ 1 ].trim();
                String[] fNames = fName.split( " " );  //this split is for parsing out middle names if they are there
                if ( fNames.length == 0 ) return null;
                return fNames[ 0 ].trim();
            }

            //for if entry is like JOHN DOE
            if (strNames.length == 0 ) return null;
            String[] fullNames = strNames[ 0 ].split( " " );
            if (fullNames.length == 0 ) return null;
            return fullNames[ 0 ].trim();
        }
        return null;
    }

    public static String getLastName( Object obj ) {
        String name = removeDigits( obj );
        if ( name != null ) {
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                return strNames[ 0 ].trim();
            }
            else if (strNames.length == 0) return null;
            // for if entry is like John Doe
            String[] fullNames = strNames[ 0 ].trim().split( " " );
            if (fullNames.length == 0 ) return null;
            return fullNames[ fullNames.length - 1 ].trim();
        }
        return null;
    }


    public static Object getMiddleName( Object obj ) {
        //String name = addSpaceAfterCommaUpperCase( obj );
        String name = removeDigits( obj );
        if ( name != null ) {
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return "";
            }

            String[] strNames = name.split( "," );
            if ( strNames.length > 1 ) {
                String fName = strNames[ 1 ].trim();
                List<String> fNames = Arrays.asList( fName.split( " " ) );
                if ( fNames.size() > 2 ) {
                    return fNames.subList( 1, fNames.size() );

                }
                if ( fNames.size() == 2 ) {
                    return fNames.get( fNames.size() - 1 ).trim();
                }
                return null;
            }
            else if (strNames.length == 0) return null;
            String[] middleNames = strNames[ 0 ].split( " " );   //for if entry is like John Middle Doe
            if ( middleNames.length > 2 ) {
                List<String> mNames = Arrays.asList(middleNames);
                return mNames.subList( 1, mNames.size() -1 );
            }
        }
        return null;
    }

    public static String getName( Object obj ) {
        String name = Parsers.getAsString( obj );
        if ( name != null ) {
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return name;
            }
            return "";
        }
        return null;
    }
}
