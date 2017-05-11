package com.dataloom.integrations.sacramento;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class SacSheriff {
    public static void main( String[] args ) throws InterruptedException {
        switch ( args[ 0 ] ) {
            case "gangs":
                GangData.main( args );
                break;
            case "cad":
                CadCalls.main( args );
                break;
            case "jail":
                JailCustody.main( args );
                break;
            case "mugshots":
                MugShotsIntegration.main( args );
                break;
            default:
                System.err.println( "Usage is ./sacramento (gangs|cad|jail|mugshots) jwtToken" );
        }
    }
}
