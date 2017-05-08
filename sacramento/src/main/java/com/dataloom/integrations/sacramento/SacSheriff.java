package com.dataloom.integrations.sacramento;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class SacSheriff {
    public static void main(String[] args) throws InterruptedException {
        GangData.main( args );
        CadCalls.main( args );
        JailCustody.main( args );
        MugShotsIntegration.main( args );
    }
}
