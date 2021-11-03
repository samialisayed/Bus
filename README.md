# Bus-Network-Analysis
Bus is the only public transport option in HCMC. Timetables were not available for bus services. We had the historical timestamped GPS data of each of the buses. Our GPS data does not have any information about next stops. It captures only distance for last stop and time at which data is reported.
We used Hadoop computing environment as a big data framework to run our spark program on NYU CUSP cluster.
We could construct timetables for each stop giving with them a certainty interval of how many minutes the bus can
come late or early. We also provided a graph representation for the trips. For each day, we can timetables for it
