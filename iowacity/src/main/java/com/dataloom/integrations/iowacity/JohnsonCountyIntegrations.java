package com.dataloom.integrations.iowacity;

/**
 * Created by mtamayo on 6/29/17.
 */
public class JohnsonCountyIntegrations {

    public static void main( String[] args ) throws InterruptedException {
        switch( args[0] ) {
            case "0":
                JohnsonCounty.main( args );
                break;
            case "1":
                JohnsonCountyExtended.main( args );
                break;
            case "2":
                JohnsonCountyJailBookings.main( args );
            case "3":
                JohnsonCountyMugshots.main( args );

        }
    }
}
