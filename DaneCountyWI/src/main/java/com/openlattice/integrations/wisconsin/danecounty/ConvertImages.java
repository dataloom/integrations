package com.dataloom.integrations.iowacity;

import com.google.common.base.Splitter;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Matthew Tamayo-Rios &lt;matthew@kryptnostic.com&gt;
 */
public class ConvertImages {
    private static final Base64.Encoder encoder  = Base64.getEncoder();
    private static final Splitter       splitter = Splitter.on( '.' ).trimResults().omitEmptyStrings();
    private static final Map<String, String> imageIdToMni = new HashMap<>(130486);

    public static void main( String[] args ) throws IOException {
        if ( args.length != 2 ) {
            System.err.println( "Expected usage is ./ConvertImages inputDirectory outputCsvFile" );
        }
        File directory = new File( args[ 0 ] );
        File outputFile = new File( args[ 1 ] );
        FileWriter writer = new FileWriter( outputFile );

        writer.write("XREF,mugshot\n" );

        if ( directory.isDirectory() ) {
            for ( File file : directory.listFiles() ) {
                if ( file.isFile() && isImage( file.getCanonicalPath() ) ) {
                    writer.write( new StringBuilder( getXref( file.getName() ) ).append( "," )
                            .append( encoder.encodeToString( Files.readAllBytes( file.toPath() ) ) ).append( "\n" )
                            .toString() );

                }
            }
        }
        writer.flush();
        writer.close();
    }

    public static String getXref( String filename ) {
        return splitter.split( filename ).iterator().next();
    }

    public static boolean isImage( String path ) {
        return StringUtils.endsWith( path, ".jpg" )
                || StringUtils.endsWith( path, ".png" )
                || StringUtils.endsWith( path, ".gif" );
    }
}
