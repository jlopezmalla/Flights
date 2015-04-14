# Flights

This project will include some Spark simple exercises based in the [airline datashet](http://www.stat.purdue.edu/~sguha/rhipe/doc/html/airline.html).

## File format.

The input format used in the exercicies will be a extract for the Flights Dataset.

The file will be separated by commas and each line will have the following structure:

| Field Name        | Description                                                                                                         | Field Type          |
|-------------------|---------------------------------------------------------------------------------------------------------------------|---------------------|
| Year              | year                                                                                                                | Int                 |
| Month             | month                                                                                                               | Int                 |
| DayofMonth        | day of month                                                                                                        | Int                 |
| DayOfWeek         | day of week                                                                                                         | Int                 |
| DepTime           | flight's departure time using the format: [h]hmm being: h-> hour, m-> minute and [] optional int position           | Int                 |
| CRSDepTime        | flight's estimated departure time using the format: [h]hmm being: h-> hour, m-> minute and [] optional int position | Int                 |
| ArrTime           | flight's arrive time using the format: [h]hmm being: h-> hour, m-> minute and [] optional int position              | Int                 |
| CRSArrTime        | flight's estimated arrive time using the format: [h]hmm being: h-> hour, m-> minute and [] optional int position    | Int                 |
| UniqueCarrier     | unique id by carrier                                                                                                | String              |
| FlightNum         | Flight Number                                                                                                       | Int                 |
| TailNum           | N/A                                                                                                                 | N/A                 |
| ActualElapsedTime | flight's real duration in minutes                                                                                   | Int                 |
| CRSElapsedTime    | flight's calculated duration in minutes                                                                             | Int                 |
| AirTime           | N/A                                                                                                                 | N/A                 |
| ArrDelay          | flight's arrival delay                                                                                              | Int                 |
| DepDelay          | flight's departure delay                                                                                            | Int                 |
| Origin            | origin airport                                                                                                      | String              |
| Dest              | destination airport                                                                                                 | String              |
| Distance          | flight's disntance in meters                                                                                        | Int                 |
| TaxiIn            | N/A                                                                                                                 | N/A                 |
| TaxiOut           | N/A                                                                                                                 | N/A                 |
| Cancelled         | Determinate if the flight has been cancelled or not. 0-> OnTime, 1 -> Cancel                                        | Structuted Field    |
| CancellationCode  | Cancelation code                                                                                                    | Int                 |
| Diverted          | N/A                                                                                                                 |                     |
| CarrierDelay      | Determinate if the flight has been cancelled beacause carrier issues or not. 0-> OnTime, 1 -> Cancel                | Structuted Field    |
| WeatherDelay      | Determinate if the flight has been cancelled beacause weathers issues or not. 0-> OnTime, 1 -> Cancel               | Structuted Field    |
| NASDelay          | Determinate if the flight has been cancelled beacause NAS issues or not. 0-> OnTime, 1 -> Cancel                    | Structuted Field    |
| SecurityDelay     | Determinate if the flight has been cancelled beacause Security issues or not. 0-> OnTime, 1 -> Cancel               | Structuted Field    |
| LateAircraftDelay | 1 causa de retraso avion / 0 No causa de retraso avion                                                              | IStructuted Fieldnt |
