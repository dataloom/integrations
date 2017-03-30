package com.dataloom.integrations.utils;

import org.apache.olingo.commons.api.edm.geo.Point;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by julia on 3/30/17.
 */
public class Tester {
    private static final Logger logger = LoggerFactory.getLogger( Tester.class );

    public static void main( String[] args ) throws InterruptedException {
        //testDates();
        testGeo();
    }

    private static void testDates() {
        // Returns datetime
        FormattedDateTime dateAndTime = new FormattedDateTime( "01/02/2000", "12:34:56", "MM/dd/yy", "HH:mm:ss" );

        // Returns date
        FormattedDateTime date = new FormattedDateTime( "01/02/2000", null, "MM/dd/yy", null );

        // Returns time
        FormattedDateTime time = new FormattedDateTime( "", "23:11:01", "", "HH:mm:ss" );

        // If either date or datePattern is missing, only time will be returned
        FormattedDateTime missingDate = new FormattedDateTime( "01/02/2000", "12:10:11", "", "HH:mm:ss" );

        // If either time or timePattern is missing, only date will be returned
        FormattedDateTime missingTime = new FormattedDateTime( "01/02/2000", "12:10:11", "MM/dd/yy", null );

        // No date or time returns null
        FormattedDateTime missingBoth = new FormattedDateTime( "", null, "", null );

        assert dateAndTime.getDateTime().equals( "1989-01-02T12:34:00.000" );
        assert date.getDateTime().equals( "1989-01-02" );
        assert time.getDateTime().equals( "12:34:56.000" );
        assert missingDate.getDateTime().equals( "12:34:56.000" );
        assert missingTime.getDateTime().equals( "1989-01-02" );
        assert missingBoth.getDateTime() == null;
    }

    private static void testGeo() {
        GeocodedAddress addr = new GeocodedAddress( "1600 Pennsylvania Avenue NW, DC" );
        String formatted = addr.getFormattedAddress();
        assert addr instanceof Point;
        assert formatted.equals( "1600 Pennsylvania Ave NW, Washington, DC 20500, USA" );
    }
}
