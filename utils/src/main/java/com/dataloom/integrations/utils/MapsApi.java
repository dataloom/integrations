package com.dataloom.integrations.utils;

import retrofit2.http.GET;
import retrofit2.http.Query;

import java.util.Map;

/**
 * Created by julia on 3/2/17.
 */
public interface MapsApi {

    @GET( "/maps/api/geocode/json" )
    Map<String, Object> gmap( @Query( "address" ) String address, @Query( "key" ) String key );
}
