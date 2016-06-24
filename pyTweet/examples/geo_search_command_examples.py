#!/usr/bin/env python


#
# Copyright 2016 MIT Lincoln Laboratory, Massachusetts Institute of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use these files except in compliance with
# the License.
#
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
#


"""
Authors: Kelly Geyer
Date: June 2, 2016
Installation: Python 2.7 on Windows 7

Description: This script is an example of using pyTweet's geo search capabilities
"""


from pyTweet import pyTweet


def main():
    # PARAMETERS
    # Enter proxy host and port information
    host = 'your proxy host'
    port = 'your proxy port'
    # Define coordinates
    BOS = (42.3601, 71.0589)        # (lat, lon)
    HOU = (29.7604, 95.3698)


    ##
    # AUTHENTICATE INTO THE OFFICIAL TWITTER API
    # Create proxies dictionary
    proxies = {'http': 'http://%s:%s' % (host, port), 'https': 'http://%s:%s' % (host, port)}
    # Load twitter keys
    twitter_keys = pyTweet.load_twitter_api_key_set()
    # API authorization
    OAUTH = pyTweet.get_authorization(twitter_keys)


    ##
    # GEO SEARCH API WRAPPERS
    # Reverse search geo: Given a latitude and a longitude, searches for up to 20 places that can be used as a place_id
    # when updating a status. Returns a list of place dictionaries
    #
    # PARAMETERS:
    # --------------
    #   lat: The latitude to search around, which must be inside the range -90.0 to +90.0
    #   lon: The longitude to search around, which must be inside the range -180.0 to +180.0
    #   accuracy: The hint on the "region" in which to search, in meters, with a default value of 0m. If a number, then
    #       this is a radius in meters, but it can also take a string that is suffixed with ft to specify feet.
    #   granularity: This is the minimal granularity of place types: 'poi', 'neighborhood', 'city', 'admin' or
    #       'country'. Default is 'neighborhood'
    #   limit: Number of places to return, returns up to 20
    place_ids = pyTweet.reverse_geocode(lat=HOU[0], lon=HOU[1], proxies=proxies, auth=OAUTH, accuracy='5ft', granularity='neighborhood', limit=20)
    print "\nPlace IDs: ", place_ids

    # Find place information using the place ID
    info = pyTweet.lookup_place(place_id='4797714c95971ac1', proxies=proxies, auth=OAUTH)
    print "\nPlace Information: ", info

    # Search for places that can be attached to a statuses/update. Given a latitude and a longitude pair, an IP address,
    #  or a name, this request will return a list of all the valid places that can be used as the place_id when
    # updating a status.
    #
    # PARAMETERS:
    # --------------
    #   lat: The latitude to search around, which must be inside the range -90.0 to +90.0
    #   lon: The longitude to search around, which must be inside the range -180.0 to +180.0
    #   q: Search term query, must be a string object or list of string objects
    #   exclusive: Boolean, if True, search with ORs rather than ANDs. Default is False
    #   ip: An IP address. Used when attempting to fix geolocation based off of the user's IP address.
    #   granularity: This is the minimal granularity of place types: 'poi', 'neighborhood', 'city', 'admin' or
    #       'country'. Default is 'neighborhood'
    #   accuracy: The hint on the "region" in which to search, in meters, with a default value of 0m. If a number, then
    #       this is a radius in meters, but it can also take a string that is suffixed with ft to specify feet.
    #   max_results: A hint as to the number of results to return. This does not guarantee that the number of results
    #       returned will equal max_results, but instead informs how many 'nearby' results to return
    #   place_id: This is the place_id which you would like to restrict the search results to. Setting this value means
    #       only places within the given place_id will be found.
    geo_search_results = pyTweet.geocode_search(proxies=proxies, auth=OAUTH, q='Toronto')
    print "\nGeo Search Results: ", geo_search_results


if __name__ == '__main__':
    main()
