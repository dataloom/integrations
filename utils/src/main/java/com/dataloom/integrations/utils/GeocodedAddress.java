package com.dataloom.integrations.utils;

/**
 * Created by julia on 3/22/17.
 */

import com.dataloom.client.RetrofitFactory;
import org.apache.olingo.commons.api.edm.geo.Point;
import retrofit2.Retrofit;

import java.util.ArrayList;
import java.util.Map;

public class GeocodedAddress extends Point {

    private static final String APIKEY = "secret-api-key";
    private Double latitude;
    private Double longitude;
    private String formattedAddress;

    public GeocodedAddress( String address ) {

        super( Dimension.GEOGRAPHY, null );

        // Get JSON with Google Maps information
        MapsApi api = setApi( "https://maps.googleapis.com/" );
        Map<String, Object> ga = api.gmap( address, APIKEY );

        // Set formatted address
        ArrayList results = (ArrayList) ga.get( "results" );
        Map<String, Object> firstResult = (Map) results.get( 0 );
        formattedAddress = (String) firstResult.get( "formatted_address" );

        // Set lat and long
        Map<String, Object> geometry = (Map) firstResult.get( "geometry" );
        Map<String, Double> location = (Map) geometry.get( "location" );
        latitude = location.get( "lat" );
        longitude = location.get( "lng" );

        // set point
        setX( longitude );
        setY( latitude );

    }

    public Double getLatitude() {
        return latitude;
    }

    public Double getLongitude() {
        return longitude;
    }

    public String getFormattedAddress() {
        return formattedAddress;
    }

    private static MapsApi setApi( String baseurl ) {
        Retrofit retrofit = RetrofitFactory.newClient( baseurl, () -> "" );
        return retrofit.create( MapsApi.class );
    }
}