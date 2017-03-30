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
    private static final Logger        logger        = LoggerFactory.getLogger( FormattedDateTime.class );
    private              String        formatted     = null;
    private              LocalDateTime myDateTime    = new LocalDateTime();
    private              LocalDate     myDate        = new LocalDate();
    private              LocalTime     myTime        = new LocalTime();
    private              boolean       dateSubmitted = false;
    private              boolean       timeSubmitted = false;

    // Constructor
    public FormattedDateTime( String date, String time, String datePattern, String timePattern ) {
        dateSubmitted = date != null && !date.equals( "" ) && datePattern != null && !datePattern.equals( "" );
        timeSubmitted = time != null && !time.equals( "" ) && timePattern != null && !timePattern.equals( "" );

        // Start by formatting the date
        if ( dateSubmitted ) {
            formatDate( date, datePattern );
        }
        // Then format the time
        if ( timeSubmitted ) {
            formatTime( time, timePattern );
        }

        // Create formatted string for date, time, or datetime
        if ( !dateSubmitted && timeSubmitted ) {
            formatted = myTime.toString();
        } else if ( dateSubmitted && !timeSubmitted ) {
            formatted = myDate.toString();
        } else if ( dateSubmitted && timeSubmitted ) {
            formatted = myDateTime.toString();
        }
    }

    // Returns formatted string
    public String getDateTime() {
        return formatted;
    }

    private void formatDate( String date, String datePattern ) {
        DateTimeFormatter customDateFormatter = DateTimeFormat.forPattern( datePattern );
        // Note that date should already match datePattern format before parsing
        myDate = date == null ? null : LocalDate.parse( date, customDateFormatter );
        myDateTime = myDateTime.withDate( myDate.getYear(), myDate.getMonthOfYear(), myDate.getDayOfMonth() );
    }

    private void formatTime( String time, String timePattern ) {
        DateTimeFormatter customTimeFormatter = DateTimeFormat.forPattern( timePattern );
        // Note that time should already match timePattern format before parsing
        myTime = time == null ? null : LocalTime.parse( time, customTimeFormatter );
        myDateTime = myDateTime.withTime( myTime.getHourOfDay(), myTime.getMinuteOfHour(), 0, 0 );
    }
}
