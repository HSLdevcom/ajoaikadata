# Utilities

This module contains utility code for typings and configs, for example. Balise registry is also defined here.

## Balise registry

Balise registry contains id-direction pairs of balises, that can be used to interpret the station event for trains. The registry contains many, **but not all** stations and tracks from Helsinki local train area.

Each balise id-direction pair gives an information what is the station, track and the direction where the train is moving. That is used to fire an station event, e.g., 'ARRIVAL, Huopalahti, track 4'.

Balise registry have sometimes multiple balise ids for the same event to enhance reliability of the events. (If one balise event is missing, it could be possible to get an event from the second balise group). Duplicate events can be filtered away if needed.

Note that balise configuration are *usually* symmetrical. If balise id 61730, direction 2 means that the train arrives to the station Aviapolis on track 1 and it moves to direction 1 (incresing railway distance location), balise id 61730 with direction 1 means the opposite: the train departures from the station Aviapolis on track 1, and it moves to direction 2.
