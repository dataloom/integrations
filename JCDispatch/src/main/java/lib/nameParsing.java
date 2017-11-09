package lib;

import com.auth0.jwt.internal.org.apache.commons.lang3.text.WordUtils;
import org.apache.spark.sql.Row;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class nameParsing {

    public static String formatText( Object obj ) {
        if ( obj != null ) {
            String name = obj.toString().toLowerCase().trim();
            name = name.replace( ",", ", " );
            if (name.contains( "-" )) {
                name = name.replace( "-", "- " );
                return WordUtils.capitalize( name.trim() ).replace( "-", "-" );
            }
            return WordUtils.capitalize( name.trim() );
        }
        return null;
    }

    public static String removeDigit( Object obj ) {
        String name = obj.toString().trim();
        name = name.replaceAll( "[\\d]+", "" ).trim();
        return name;
    }

    public static final Pattern p = Pattern
            .compile( "\\b(UNIVERSITY|EMS|HONDA|LP|REHABILITATION|INC|IOWA|ADT|VERIZON|SPRINT|SANDWICHES|AT&T|BLDG|CENTER|CELLULAR|INTERNATIONAL|SCIENCES)\\b" , Pattern.CASE_INSENSITIVE );

    public static String getFirstName( Object obj ) {
        if ( obj != null ) {
            String name = formatText( obj );
            name = removeDigit( name );
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
            String name = formatText( obj );
            name = removeDigit( name );
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
            String name = formatText( obj );
            name = removeDigit( name );
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
            String name = obj.toString().trim();
            Matcher m = p.matcher( name );

            if ( m.find() ) {
                return name;
            }
            return "";
        }
        return null;
    }

    public static String getAddressID( Object obj ) {
        if ( obj != null ) {
            String address = obj.toString().trim();
            if ( address.contains( "Ia" ) || address.contains( "null" ) ) {
                address = address.replace( "Ia", "IA" );
                address = address.replace( "null", "" );
                return String.join( "" , Arrays.asList( address.split( " " ) ) );
            }
            return String.join( "" , Arrays.asList( address.split( " " ) ) );
        }
        return null;
    }

    public static String getIntersection( Object obj ) {
        if ( obj != null ) {
            String address = obj.toString().trim();
            if ( address.contains( "/" ) ) {
                return address.replace( "/", " & " );
            }
            return "";
        }
        return null;
    }

    public static String getLocation( Object obj ) {
        if ( obj != null ) {
            String location = obj.toString().trim();
            if ( location.contains( "/" ) ) {
                return "";
            }
            return location;
        }
        return null;
    }
}