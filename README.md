# Opening Hours Scraper:
This is an open-source project, overseen by Lanterne, started in the aim to offer an interface capable of returning the opening hours to any store (or POI) in the world, with the possible aim of integration into OpenStreetMap (OSM).
## How it all works:
The way this is achieved, is by interfacing with hundreds of individual store's APIs, and then transforming the given response into a formatted list of opening hours (as specified below) for each day of the of the given store. Therefore this project could be taken and used as an external API, or used to fill a database of stores etc.. as it has been designed to be very flexible, as a back-end tool, a sort of building block.

This project therefore offers a low cost/free alternative to retrieving opening hours using a paid API such as Google Places.
## How to help out with the project:
If you are a new contributer interested in helping expand the reach of this project, an excellent place to start is by using the network inspector of your browser on a brands store locator page, as they almost always tend to call an in-house API to retrieve data regarding their stores. You can then make a function to perform a get request with the desired latitude / longitude of the store as queries to this API and then return the formatted hours. This may then be submitted along with some short documentation of the store's API to be integrated into this project :). For guidance of the standardised recommended approach to use to safely sort through the api response data (dealing with missing days / out of order days / repeated days in the response etc..), see the section at the beginning of the POI Documentation notebook, called Design Schema / Heap Structure.
### How the opening hours should be formatted when returned:
<pre>
[
        {
            day: weekday (e.g. Monday),
            open: True | False,
            hours: [ //This should be an array because there could be multiple opening hours for one day. E.g. 08:00-12:00 and 15:00-19:00.
                {
                    open: zero-padded hour (24-hour clock):zero-padded minute (e.g.'06:00')
                    close: zero-padded hour (24-hour clock):zero-padded minute (e.g.'22:00')
                }
            ]
        }
]
 </pre>
 
## Non-exhaustive list of stores currently implemented:
### United Kingdom:
* Sainsburys
* Asda
* Tesco
* Morrisons
* Waitrose
* Aldi
* Co-op
* Marks and Spencers
* Iceland
### Germany:
* REWE
* Netto
* Netto Marken Discount
* EDEKA
* Kaufland
