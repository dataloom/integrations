package com.dataloom.integrations.utils;

// Formatting datetime

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.LocalDate;
import org.joda.time.LocalTime;
import org.joda.time.LocalDateTime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by julia on 3/4/17.
 */
public class FormattedDateTime {
    private static final Logger logger    = LoggerFactory.getLogger( FormattedDateTime.class );
    // Instance variables
    private              String smartDate = null;

    private LocalDateTime dt     = new LocalDateTime();
    private LocalDate     myDate = new LocalDate();
    private LocalTime     myTime = new LocalTime();

    private String datePattern;
    private String timePattern;

    // Constructor
    public FormattedDateTime( String date, String time, String datePattern, String timePattern ) {
        if ( date != null && !date.equals( "" ) ) {
            formatDate( date, datePattern ); // start by formatting date
        }
        if ( time != null && !time.equals( "" ) ) {
            formatTime( time, timePattern ); // then format time
        }

        if ( ( date.equals( "" ) || date == null ) && ( time != null && !time.equals( "" ) ) ) {
            smartDate = myTime.toString(); // only time
        } else if ( ( !date.equals( "" ) && date != null ) && ( time == null || time.equals( "" ) ) ) {
            smartDate = myDate.toString(); // only date
        } else if ( ( !date.equals( "" ) && date != null ) && ( time != null && !time.equals( "" ) ) ) {
            smartDate = dt.toString(); // date time
        }
    }

    public String getDateTime() {
        logger.info( "Smart: {}", smartDate );
        return smartDate;
    }

    private void formatDate( String date, String datePattern ) {
        DateTimeFormatter customDateFormatter = DateTimeFormat.forPattern( datePattern );
        String dateString = date;
        if ( date != null && !myDate.equals( "" ) ) {
            dateString = date;
            // dateString should already be padded with zeros before being parsed
            myDate = date == null ? null : LocalDate.parse( dateString, customDateFormatter );
            dt = dt.withDate( myDate.getYear(), myDate.getMonthOfYear(), myDate.getDayOfMonth() );
        }
    }

    private void formatTime( String time, String timePattern ) {
        DateTimeFormatter customTimeFormatter = DateTimeFormat.forPattern( timePattern );
        String timeString = time;
        if ( time != null && !time.equals( "" ) ) {
            timeString = time;
            // timeString should already be padded with zeros before being parsed
            myTime = time == null ? null : LocalTime.parse( timeString, customTimeFormatter );
            dt = dt.withTime( myTime.getHourOfDay(), myTime.getMinuteOfHour(), 0, 0 );
        }
    }
}
